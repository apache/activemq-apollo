/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.stomp

import _root_.org.fusesource.hawtbuf._
import org.fusesource.hawtdispatch._

import Buffer._
import org.apache.activemq.apollo.broker._
import java.lang.String
import protocol.{HeartBeatMonitor, ProtocolHandler}
import security.SecurityContext
import Stomp._
import java.io.IOException
import org.apache.activemq.apollo.selector.SelectorParser
import org.apache.activemq.apollo.filter.{BooleanExpression, FilterException}
import org.apache.activemq.apollo.store._
import org.apache.activemq.apollo.util._
import java.util.concurrent.TimeUnit
import java.util.Map.Entry
import scala.util.continuations._
import org.apache.activemq.apollo.dto._
import org.apache.activemq.apollo.transport.tcp.SslTransport
import java.security.cert.X509Certificate
import collection.mutable.{ArrayBuffer, ListBuffer, HashMap}

object StompProtocolHandler extends Log {

  // How long we hold a failed connection open so that the remote end
  // can get the resulting error message.
  val DEFAULT_DIE_DELAY = 5*1000L
  var die_delay = DEFAULT_DIE_DELAY

    // How often we can send heartbeats of the connection is idle.
  val DEFAULT_OUTBOUND_HEARTBEAT = 100L
  var outbound_heartbeat = DEFAULT_OUTBOUND_HEARTBEAT

  // How often we want to get heartbeats from the peer if the connection is idle.
  val DEFAULT_INBOUND_HEARTBEAT = 10*1000L
  var inbound_heartbeat = DEFAULT_INBOUND_HEARTBEAT

}

import StompProtocolHandler._


/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class StompProtocolHandler extends ProtocolHandler with DispatchLogging {

  def protocol = "stomp"

  def decode_header(value:Buffer):String = {
    var rc = new ByteArrayOutputStream(value.length)
    val pos = new Buffer(value)
    val max = value.offset + value.length
    while( pos.offset < max ) {
      if( pos.startsWith(ESCAPE_ESCAPE_SEQ) ) {
        rc.write(ESCAPE)
        pos.offset += 2
      } else if( pos.startsWith(COLON_ESCAPE_SEQ) ) {
        rc.write(COLON)
        pos.offset += 2
      } else if( pos.startsWith(NEWLINE_ESCAPE_SEQ) ) {
        rc.write(NEWLINE)
        pos.offset += 2
      } else {
        rc.write(pos.data(pos.offset))
        pos.offset += 1
      }
    }
    new String(rc.toByteArray, "UTF-8")
  }

  def encode_header(value:String) = {
    val data = value.getBytes("UTF-8")
    var rc = new ByteArrayOutputStream(data.length)
    data.foreach {
      case ESCAPE  => rc.write(ESCAPE_ESCAPE_SEQ)
      case COLON   => rc.write(COLON_ESCAPE_SEQ)
      case NEWLINE => rc.write(COLON_ESCAPE_SEQ)
      case c       => rc.write(c)

    }
    rc.toBuffer.ascii
  }

  override protected def log = StompProtocolHandler

  protected def dispatchQueue:DispatchQueue = connection.dispatchQueue

  trait AckHandler {
    def track(delivery:Delivery):Unit
    def perform_ack(consumed:Boolean, msgid: AsciiBuffer, uow:StoreUOW=null):Unit
  }

  class AutoAckHandler extends AckHandler {
    def track(delivery:Delivery) = {
      if( delivery.ack!=null ) {
        delivery.ack(true, null)
      }
    }

    def perform_ack(consumed:Boolean, msgid: AsciiBuffer, uow:StoreUOW=null) = {
      async_die("The subscription ack mode does not expect ACK or NACK frames")
    }

  }

  class SessionAckHandler extends AckHandler{
    var consumer_acks = ListBuffer[(AsciiBuffer, (Boolean, StoreUOW)=>Unit)]()

    def track(delivery:Delivery) = {
      queue.apply {
        if( protocol_version eq V1_0 ) {
          // register on the connection since 1.0 acks may not include the subscription id
          connection_ack_handlers += ( delivery.message.id-> this )
        }
        consumer_acks += (( delivery.message.id, delivery.ack ))
      }

    }


    def perform_ack(consumed:Boolean, msgid: AsciiBuffer, uow:StoreUOW=null) = {

      // session acks ack all previously recieved messages..
      var found = false
      val (acked, not_acked) = consumer_acks.partition{ case (id, ack)=>
        if( found ) {
          false
        } else {
          if( id == msgid ) {
            found = true
          }
          true
        }
      }

      if( acked.isEmpty ) {
        async_die("ACK failed, invalid message id: %s".format(msgid))
      } else {
        consumer_acks = not_acked
        acked.foreach{case (id, ack)=>
          if( ack!=null ) {
            ack(consumed, uow)
          }
        }
      }

      if( protocol_version eq V1_0 ) {
        connection_ack_handlers.remove(msgid)
      }
    }

  }
  class MessageAckHandler extends AckHandler {
    var consumer_acks = HashMap[AsciiBuffer, (Boolean, StoreUOW)=>Unit]()

    def track(delivery:Delivery) = {
      queue.apply {
        if( protocol_version eq V1_0 ) {
          // register on the connection since 1.0 acks may not include the subscription id
          connection_ack_handlers += ( delivery.message.id-> this )
        }
        consumer_acks += ( delivery.message.id -> delivery.ack )
      }
    }

    def perform_ack(consumed:Boolean, msgid: AsciiBuffer, uow:StoreUOW=null) = {
      consumer_acks.remove(msgid) match {
        case Some(ack) =>
          if( ack!=null ) {
            ack(consumed, uow)
          }
        case None => async_die("ACK failed, invalid message id: %s".format(msgid))
      }

      if( protocol_version eq V1_0 ) {
        connection_ack_handlers.remove(msgid)
      }
    }
  }

  class StompConsumer(

    val subscription_id:Option[AsciiBuffer],
    val destination:Destination,
    val ack_handler:AckHandler,
    val selector:(String, BooleanExpression),
    val binding:BindingDTO,
    override val browser:Boolean

  ) extends BaseRetained with DeliveryConsumer {

    val dispatchQueue = StompProtocolHandler.this.dispatchQueue

    override def connection = Some(StompProtocolHandler.this.connection)

    def is_persistent = false

    def matches(delivery:Delivery) = {
      if( delivery.message.protocol eq StompProtocol ) {
        if( selector!=null ) {
          selector._2.matches(delivery.message)
        } else {
          true
        }
      } else {
        false
      }
    }

    def connect(p:DeliveryProducer) = new DeliverySession {

      // This session object should only be used from the dispatch queue context
      // of the producer.

      retain

      def producer = p
      def consumer = StompConsumer.this
      var closed = false

      val session = session_manager.open(producer.dispatchQueue)

      def close = {
        assert(getCurrentQueue == producer.dispatchQueue)
        if( !closed ) {
          closed = true
          if( browser ) {
            // Then send the end of browse message.
            var frame = StompFrame(MESSAGE, (BROWSER, END)::Nil, BufferContent(EMPTY_BUFFER))
            if( subscription_id != None ) {
              frame = frame.append_headers((SUBSCRIPTION, subscription_id.get)::Nil)
            }

            if( session.full ) {
              // session is full so use an overflow sink so to hold the message,
              // and then trigger closing the session once it empties out.
              val sink = new OverflowSink(session)
              sink.refiller = ^{
                session_manager.close(session)
                release
              }
              sink.offer(frame)
            } else {
              session.offer(frame)
              session_manager.close(session)
              release
            }
          } else {
            session_manager.close(session)
            release
          }
        }
      }

      // Delegate all the flow control stuff to the session
      def full = session.full
      def offer(delivery:Delivery) = {
        if( session.full ) {
          false
        } else {
          ack_handler.track(delivery)
          var frame = delivery.message.asInstanceOf[StompFrameMessage].frame
          if( subscription_id != None ) {
            frame = frame.append_headers((SUBSCRIPTION, subscription_id.get)::Nil)
          }
          frame.retain
          val rc = session.offer(frame)
          assert(rc, "offer should be accepted since it was not full")
          true
        }
      }

      def refiller = session.refiller
      def refiller_=(value:Runnable) = { session.refiller=value }

    }
  }

  var session_manager:SinkMux[StompFrame] = null
  var connection_sink:Sink[StompFrame] = null

  var dead = false
  var closed = false
  var consumers = Map[AsciiBuffer, StompConsumer]()

  var producerRoutes = new LRUCache[Destination, DeliveryProducerRoute](10) {
    override def onCacheEviction(eldest: Entry[Destination, DeliveryProducerRoute]) = {
      host.router.disconnect(eldest.getValue)
    }
  }

  var host:VirtualHost = null

  private def queue = connection.dispatchQueue

  // uses by STOMP 1.0 clients
  var connection_ack_handlers = HashMap[AsciiBuffer, AckHandler]()

  var session_id:AsciiBuffer = _
  var protocol_version:AsciiBuffer = _

  var heart_beat_monitor:HeartBeatMonitor = new HeartBeatMonitor
  val security_context = new SecurityContext
  var waiting_on:String = "client request"
  var config:StompDTO = _

  override def setConnection(connection: BrokerConnection) = {
    super.setConnection(connection)
    import collection.JavaConversions._
    config = connection.connector.config.protocols.find( _.isInstanceOf[StompDTO]).map(_.asInstanceOf[StompDTO]).getOrElse(new StompDTO)
  }

  override def create_connection_status = {
    var rc = new StompConnectionStatusDTO
    rc.protocol_version = if( protocol_version == null ) null else protocol_version.toString
    rc.user = security_context.user
    rc.subscription_count = consumers.size
    rc.waiting_on = waiting_on
    rc
  }

  class ProtocolException(msg:String) extends RuntimeException(msg)
  class Break extends RuntimeException

  private def async_die(msg:String, e:Throwable=null) = try {
    die(msg)
  } catch {
    case x:Break=>
  }

  private def die[T](msg:String, e:Throwable=null):T = {
    if( e!=null) {
      debug(e, "Shutting connection down due to: "+msg)
    } else {
      debug("Shutting connection down due to: "+msg)
    }
    die((MESSAGE_HEADER, encode_header(msg))::Nil, "")
  }

  private def die[T](headers:HeaderMap, body:String):T = {
    if( !dead ) {
      dead = true
      waiting_on = "shutdown"
      connection.transport.resumeRead

      if( body.isEmpty ) {
        connection_sink.offer(StompFrame(ERROR, headers, BufferContent(EMPTY_BUFFER)) )
      } else {
        connection_sink.offer(StompFrame(ERROR, (CONTENT_TYPE, TEXT_PLAIN)::headers, BufferContent(utf8(body))) )
      }

      // TODO: if there are too many open connections we should just close the connection
      // without waiting for the error to get sent to the client.
      queue.after(die_delay, TimeUnit.MILLISECONDS) {
        connection.stop()
      }
    }
    throw new Break()
  }

  override def onTransportConnected() = {

    session_manager = new SinkMux[StompFrame]( MapSink(connection.transportSink){x=>
      trace("sending frame: %s", x)
      x
    }, dispatchQueue, StompFrame)
    connection_sink = new OverflowSink(session_manager.open(dispatchQueue));
    connection_sink.refiller = NOOP
    resumeRead
  }

  override def onTransportDisconnected() = {
    if( !closed ) {
      heart_beat_monitor.stop
      closed=true;
      dead = true;

      import collection.JavaConversions._
      producerRoutes.foreach{
        case(_,route)=> host.router.disconnect(route)
      }
      producerRoutes.clear
      consumers.foreach {
        case (_,consumer)=>
          if( consumer.binding==null ) {
            host.router.unbind(consumer.destination, consumer)
          } else {
            reset {
              val queue = host.router.get_queue(consumer.binding)
              queue.foreach( _.unbind(consumer::Nil) )
            }
          }
      }
      consumers = Map()
      trace("stomp protocol resources released")
    }
  }


  override def onTransportCommand(command:Any):Unit = {
    if( dead ) {
      // We stop processing client commands once we are dead
      return;
    }
    try {
      command match {
        case s:StompCodec =>
          // this is passed on to us by the protocol discriminator
          // so we know which wire format is being used.
        case frame:StompFrame=>

          trace("received frame: %s", frame)

          if( protocol_version == null ) {

            frame.action match {
              case STOMP =>
                on_stomp_connect(frame.headers)
              case CONNECT =>
                on_stomp_connect(frame.headers)
              case DISCONNECT =>
                connection.stop
              case _ =>
                die("Client must first send a connect frame");
            }

          } else {
            frame.action match {
              case SEND =>
                on_stomp_send(frame)
              case ACK =>
                on_stomp_ack(frame)
              case BEGIN =>
                on_stomp_begin(frame.headers)
              case COMMIT =>
                on_stomp_commit(frame.headers)
              case ABORT =>
                on_stomp_abort(frame.headers)
              case SUBSCRIBE =>
                on_stomp_subscribe(frame.headers)
              case UNSUBSCRIBE =>
                on_stomp_unsubscribe(frame.headers)
              case NACK =>
                on_stomp_nack(frame)

              case DISCONNECT =>
                connection.stop

              case _ =>
                die("Invalid frame: "+frame.action);
            }
          }

        case _=>
          warn("Internal Server Error: unexpected command type")
          die("Internal Server Error");
      }
    }  catch {
      case e: Break =>
      case e:Exception =>
        e.printStackTrace
        async_die("Internal Server Error", e);
    }
  }


  def suspendRead(reason:String) = {
    waiting_on = reason
    connection.transport.suspendRead
  }
  def resumeRead() = {
    waiting_on = "client request"
    connection.transport.resumeRead
  }

  def weird(headers:HeaderMap) = {
    println("weird: "+headers)
  }

  def on_stomp_connect(headers:HeaderMap):Unit = {

    connection.transport match {
      case t:SslTransport=>
        security_context.certificates = Option(t.getPeerX509Certificates).getOrElse(Array[X509Certificate]())
      case _ => None
    }
    security_context.user = get(headers, LOGIN).map(decode_header _).getOrElse(null)
    security_context.password = get(headers, PASSCODE).map(decode_header _).getOrElse(null)

    val accept_versions = get(headers, ACCEPT_VERSION).getOrElse(V1_0).split(COMMA).map(_.ascii)
    protocol_version = SUPPORTED_PROTOCOL_VERSIONS.find( v=> accept_versions.contains(v) ) match {
      case Some(x) => x
      case None=>
        val supported_versions = SUPPORTED_PROTOCOL_VERSIONS.mkString(",")
        die((MESSAGE_HEADER, ascii("version not supported"))::
            (VERSION, ascii(supported_versions))::Nil,
            "Supported protocol versions are %s".format(supported_versions))
    }

    val heart_beat = get(headers, HEART_BEAT).getOrElse(DEFAULT_HEAT_BEAT)
    heart_beat.split(COMMA).map(_.ascii) match {
      case Array(cx,cy) =>
        try {
          val can_send = cx.toString.toLong
          val please_send = cy.toString.toLong

          if( inbound_heartbeat>=0 && can_send > 0 ) {
            heart_beat_monitor.read_interval = inbound_heartbeat.max(can_send)

            // lets be a little forgiving to account to packet transmission latency.
            heart_beat_monitor.read_interval += heart_beat_monitor.read_interval.min(5000)

            heart_beat_monitor.on_dead = () => {
              async_die("Stale connection.  Missed heartbeat.")
            }
          }
          if( outbound_heartbeat>=0 && please_send > 0 ) {
            heart_beat_monitor.write_interval = outbound_heartbeat.max(please_send)
            heart_beat_monitor.on_keep_alive = () => {
              connection.transport.offer(NEWLINE_BUFFER)
            }
          }

          heart_beat_monitor.transport = connection.transport
          heart_beat_monitor.start

        } catch {
          case x:NumberFormatException=>
            die("Invalid heart-beat header: "+heart_beat)
        }
      case _ =>
        die("Invalid heart-beat header: "+heart_beat)
    }

    def noop = shift {  k: (Unit=>Unit) => k() }

    def send_connected = {
      val outbound_heart_beat_header = ascii("%d,%d".format(outbound_heartbeat,inbound_heartbeat))
      session_id = encode_header(this.host.config.id + ":"+this.host.session_counter.incrementAndGet)
      if( connection_sink==null ) {
        weird(headers)
      }
      connection_sink.offer(
        StompFrame(CONNECTED, List(
          (VERSION, protocol_version),
          (SESSION, session_id),
          (HEART_BEAT, outbound_heart_beat_header)
        )))

      if( this.host.direct_buffer_pool!=null ) {
        val wf = connection.transport.getProtocolCodec.asInstanceOf[StompCodec]
        wf.memory_pool = this.host.direct_buffer_pool
      }
    }

    reset {
      suspendRead("virtual host lookup")
      val host_header = get(headers, HOST)
      val host = host_header match {
        case None=>
          connection.connector.broker.getDefaultVirtualHost
        case Some(host)=>
          connection.connector.broker.getVirtualHost(host)
      }
      resumeRead

      if(host==null) {
        async_die("Invalid virtual host: "+host_header.get)
        noop
      } else {
        this.host=host
        if( host.authenticator!=null &&  host.authorizer!=null ) {
          suspendRead("authenticating and authorizing connect")
          if( !host.authenticator.authenticate(security_context) ) {
            async_die("Authentication failed.")
            noop // to make the cps compiler plugin happy.
          } else if( !host.authorizer.can_connect_to(security_context, host) ) {
            async_die("Connect not authorized.")
            noop // to make the cps compiler plugin happy.
          } else {
            resumeRead
            send_connected
            noop // to make the cps compiler plugin happy.
          }
        } else {
          send_connected
          noop // to make the cps compiler plugin happy.
        }
      }
    }

  }

  def get(headers:HeaderMap, names:List[AsciiBuffer]):List[Option[AsciiBuffer]] = {
    names.map(x=>get(headers, x))
  }

  def get(headers:HeaderMap, name:AsciiBuffer):Option[AsciiBuffer] = {
    val i = headers.iterator
    while( i.hasNext ) {
      val entry = i.next
      if( entry._1 == name ) {
        return Some(entry._2)
      }
    }
    None
  }

  def on_stomp_send(frame:StompFrame) = {

    get(frame.headers, DESTINATION) match {
      case None=>
        frame.release
        die("destination not set.")

      case Some(dest)=>

        get(frame.headers, TRANSACTION) match {
          case None=>
            perform_send(frame)
          case Some(txid)=>
            get_or_create_tx_queue(txid).add { uow=>
              perform_send(frame, uow)
            }
        }

    }
  }

  def perform_send(frame:StompFrame, uow:StoreUOW=null): Unit = {

    val destiantion: Destination = get(frame.headers, DESTINATION).get
    producerRoutes.get(destiantion) match {
      case null =>
        // create the producer route...

        val producer = new DeliveryProducer() {
          override def connection = Some(StompProtocolHandler.this.connection)

          override def dispatchQueue = queue
        }

        // don't process frames until producer is connected...
        connection.transport.suspendRead
        host.router.connect(destiantion, producer, security_context) {
          case Failure(reason) =>
            async_die(reason)

          case Success(route) =>
            if (!connection.stopped) {
              resumeRead
              route.refiller = ^ {
                resumeRead
              }
              producerRoutes.put(destiantion, route)
              send_via_route(route, frame, uow)
            }
        }

      case route =>
        // we can re-use the existing producer route
        send_via_route(route, frame, uow)

    }
  }


  var message_id_counter = 0;

  def updated_headers(headers:HeaderMap) = {
    var rc:HeaderMap=Nil

    // Do we need to add the message id?
    if( get( headers, MESSAGE_ID) == None ) {
      // TODO: properly generate mesage ids
      message_id_counter += 1
      rc ::= (MESSAGE_ID, ascii("msg:"+message_id_counter))
    }

    // Do we need to add the user id?
    if( host.authenticator!=null && config.add_user_header!=null ) {
      host.authenticator.user_name(security_context).foreach{ name=>
        rc ::= (encode_header(config.add_user_header), encode_header(name))
      }
    }

    rc
  }

  def send_via_route(route:DeliveryProducerRoute, frame:StompFrame, uow:StoreUOW) = {
    var storeBatch:StoreUOW=null
    // User might be asking for ack that we have processed the message..
    val receipt = frame.header(RECEIPT_REQUESTED)

    if( !route.targets.isEmpty ) {

      // We may need to add some headers..
      var message = updated_headers(frame.headers) match {
        case Nil=>
          StompFrameMessage(StompFrame(MESSAGE, frame.headers, frame.content))
        case updated_headers =>
          StompFrameMessage(StompFrame(MESSAGE, frame.headers, frame.content, updated_headers))
      }

      val delivery = new Delivery
      delivery.message = message
      delivery.size = message.frame.size
      delivery.uow = uow

      if( receipt!=null ) {
        delivery.ack = { (consumed, uow) =>
          dispatchQueue <<| ^{
            connection_sink.offer(StompFrame(RECEIPT, List((RECEIPT_ID, receipt))))
          }
        }
      }

      // routes can always accept at least 1 delivery...
      assert( !route.full )
      route.offer(delivery)
      if( route.full ) {
        // but once it gets full.. suspend, so that we get more stomp messages
        // until it's not full anymore.
        suspendRead("blocked destination: "+route.destination)
      }

    } else {
      // info("Dropping message.  No consumers interested in message.")
      if( receipt!=null ) {
        connection_sink.offer(StompFrame(RECEIPT, List((RECEIPT_ID, receipt))))
      }
    }
    frame.release
  }

  def on_stomp_subscribe(headers:HeaderMap):Unit = {
    val dest = get(headers, DESTINATION).getOrElse(die("destination not set."))
    val destination:Destination = dest

    val subscription_id = get(headers, ID)
    var id:AsciiBuffer = subscription_id.getOrElse {
      if( protocol_version eq V1_0 ) {
          // in 1.0 it's ok if the client does not send us the
          // the id header
          dest
        } else {
          die("The id header is missing from the SUBSCRIBE frame");
        }

    }

    val topic = destination.domain == Router.TOPIC_DOMAIN
    var persistent = get(headers, PERSISTENT).map( _ == TRUE ).getOrElse(false)
    var browser = get(headers, BROWSER).map( _ == TRUE ).getOrElse(false)

    val ack = get(headers, ACK_MODE) match {
      case None=> new AutoAckHandler
      case Some(x)=> x match {
        case ACK_MODE_AUTO=>new AutoAckHandler
        case ACK_MODE_NONE=>new AutoAckHandler
        case ACK_MODE_CLIENT=> new SessionAckHandler
        case ACK_MODE_SESSION=> new SessionAckHandler
        case ACK_MODE_MESSAGE=> new MessageAckHandler
        case ack:AsciiBuffer =>
          die("Unsuported ack mode: "+ack);
      }
    }

    val selector = get(headers, SELECTOR) match {
      case None=> null
      case Some(x)=> x
        try {
          val s = decode_header(x)
          (s, SelectorParser.parse(s))
        } catch {
          case e:FilterException =>
            die("Invalid selector expression: "+e.getMessage)
        }
    }

    if ( consumers.contains(id) ) {
      die("A subscription with identified with '"+id+"' allready exists")
    }

    val binding: BindingDTO = if( topic && !persistent ) {
      null
    } else {
      // Controls how the created queue gets bound
      // to the destination name space (this is used to
      // recover the queue on restart and rebind it the
      // way again)
      if (topic) {
        val rc = new SubscriptionBindingDTO
        rc.name = DestinationParser.encode_path(destination.name)
        // TODO:
        // rc.client_id =
        rc.subscription_id = if( persistent ) decode_header(id) else null
        rc.filter = if (selector == null) null else selector._1
        rc
      } else {
        val rc = new QueueBindingDTO
        rc.name = DestinationParser.encode_path(destination.name)
        rc
      }
    }

    val consumer = new StompConsumer(subscription_id, destination, ack, selector, binding, browser);
    consumers += (id -> consumer)

    if( binding==null ) {

      // consumer is bind bound as a topic
      reset {
        val rc = host.router.bind(destination, consumer, security_context)
        consumer.release
        rc match {
          case Failure(reason)=>
            async_die(reason)
          case _=>
            send_receipt(headers)
        }
      }

    } else {
      reset {
        // create a queue and bind the consumer to it.
        val x= host.router.get_or_create_queue(binding, security_context)
        x match {
          case Success(queue) =>
            val rc = queue.bind(consumer, security_context)
            consumer.release
            rc match {
              case Failure(reason)=>
                consumers -= id
                async_die(reason)
              case _ =>
                send_receipt(headers)
            }
          case Failure(reason) =>
            consumers -= id
            async_die(reason)
        }
      }
    }
  }

  def on_stomp_unsubscribe(headers:HeaderMap):Unit = {

    var persistent = get(headers, PERSISTENT).map( _ == TRUE ).getOrElse(false)

    val id = get(headers, ID).getOrElse {
      if( protocol_version eq V1_0 ) {
        // in 1.0 it's ok if the client does not send us the
        // the id header, the destination header must be set
        get(headers, DESTINATION) match {
          case Some(dest)=> dest
          case None=>
            die("destination not set.")
        }
      } else {
        die("The id header is missing from the UNSUBSCRIBE frame");
      }
    }

    consumers.get(id) match {
      case None=>
        die("The subscription '%s' not found.".format(id))
      case Some(consumer)=>
        // consumer.close
        if( consumer.binding==null ) {
          host.router.unbind(consumer.destination, consumer)
          send_receipt(headers)
        } else {

          reset {
            val queue = host.router.get_queue(consumer.binding)
            queue.foreach( _.unbind(consumer::Nil) )
          }

          if( persistent && consumer.binding!=null ) {
            reset {
              val rc = host.router.destroy_queue(consumer.binding, security_context)
              rc match {
                case Failure(reason) =>
                  async_die(reason)
                case Success(_) =>
                  send_receipt(headers)
              }
            }
          } else {
            send_receipt(headers)
          }

        }

    }
  }

  def on_stomp_ack(frame:StompFrame):Unit = {
    on_stomp_ack(frame.headers, true)
  }

  def on_stomp_nack(frame:StompFrame):Unit = {
    on_stomp_ack(frame.headers, false)
  }

  def on_stomp_ack(headers:HeaderMap, consumed:Boolean):Unit = {
    val messageId = get(headers, MESSAGE_ID).getOrElse(die("message id header not set"))

    val subscription_id = get(headers, SUBSCRIPTION);
    val handler = subscription_id match {
      case None=>
        if( !(protocol_version eq V1_0) ) {
          die("The subscription header is required")
        }
        connection_ack_handlers.get(messageId).orElse(die("Not expecting an ACK/NACK for message id '%s'".format(messageId)))
      case Some(id) =>
        consumers.get(id).map(_.ack_handler).orElse(die("The subscription '%s' does not exist".format(id)))
    }

    handler.foreach{ handler=>
      get(headers, TRANSACTION) match {
        case None=>
          handler.perform_ack(consumed, messageId, null)
        case Some(txid)=>
          get_or_create_tx_queue(txid).add{ uow=>
            handler.perform_ack(consumed, messageId, uow)
          }
      }
      send_receipt(headers)
    }
  }


  override def onTransportFailure(error: IOException) = {
    if( !connection.stopped ) {
      suspendRead("shutdown")
      debug(error, "Shutting connection down due to: %s", error)
      super.onTransportFailure(error);
    }
  }


  def require_transaction_header[T](headers:HeaderMap):AsciiBuffer = {
    get(headers, TRANSACTION).getOrElse(die("transaction header not set"))
  }

  def on_stomp_begin(headers:HeaderMap) = {
    create_tx_queue(require_transaction_header(headers))
    send_receipt(headers)
  }

  def on_stomp_commit(headers:HeaderMap) = {
    remove_tx_queue(require_transaction_header(headers)).commit {
      send_receipt(headers)
    }
  }

  def on_stomp_abort(headers:HeaderMap) = {
    remove_tx_queue(require_transaction_header(headers)).rollback
    send_receipt(headers)
  }


  def send_receipt(headers:HeaderMap):Unit = {
    get(headers, RECEIPT_REQUESTED) match {
      case Some(receipt)=>
        dispatchQueue <<| ^{
          connection_sink.offer(StompFrame(RECEIPT, List((RECEIPT_ID, receipt))))
        }
      case None=>
    }
  }

  class TransactionQueue {
    // TODO: eventually we want to back this /w a broker Queue which
    // can provides persistence and memory swapping.

    val queue = ListBuffer[(StoreUOW)=>Unit]()

    def add(proc:(StoreUOW)=>Unit):Unit = {
      queue += proc
    }

    def commit(onComplete: => Unit) = {

      val uow = if( host.store!=null ) {
        host.store.createStoreUOW
      } else {
        null
      }

      queue.foreach{ _(uow) }
      if( uow!=null ) {
        uow.onComplete(^{
          onComplete
        })
        uow.release
      } else {
        onComplete
      }

    }

    def rollback = {
      queue.clear
    }

  }

  val transactions = HashMap[AsciiBuffer, TransactionQueue]()

  def create_tx_queue(txid:AsciiBuffer):TransactionQueue = {
    if ( transactions.contains(txid) ) {
      die("transaction allready started")
    } else {
      val queue = new TransactionQueue
      transactions.put(txid, queue)
      queue
    }
  }

  def get_or_create_tx_queue(txid:AsciiBuffer):TransactionQueue = {
    transactions.getOrElseUpdate(txid, new TransactionQueue)
  }

  def remove_tx_queue(txid:AsciiBuffer):TransactionQueue = {
    transactions.remove(txid).getOrElse(die("transaction not active: %d".format(txid)))
  }

}

