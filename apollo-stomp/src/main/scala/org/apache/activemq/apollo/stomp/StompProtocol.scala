/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
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

import _root_.org.fusesource.hawtdispatch.{DispatchQueue, BaseRetained}
import _root_.org.fusesource.hawtbuf._
import collection.mutable.{ListBuffer, HashMap}
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._

import AsciiBuffer._
import org.apache.activemq.apollo.broker._
import protocol.{ProtocolFactory, Protocol, ProtocolHandler}
import java.lang.String
import Stomp._
import BufferConversions._
import java.io.IOException
import org.apache.activemq.apollo.selector.SelectorParser
import org.apache.activemq.apollo.filter.{BooleanExpression, FilterException}
import org.apache.activemq.apollo.transport._
import org.apache.activemq.apollo.store._
import org.apache.activemq.apollo.util._
import java.util.concurrent.TimeUnit
import java.util.Map.Entry
import org.apache.activemq.apollo.dto.{StompConnectionStatusDTO, BindingDTO, DurableSubscriptionBindingDTO, PointToPointBindingDTO}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
/**
 * Creates StompCodec objects that encode/decode the
 * <a href="http://activemq.apache.org/stomp/">Stomp</a> protocol.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class StompProtocolCodecFactory extends ProtocolCodecFactory.Provider {

  def protocol = PROTOCOL

  def createProtocolCodec() = new StompCodec();

  def isIdentifiable() = true

  def maxIdentificaionLength() = CONNECT.length;

  def matchesIdentification(header: Buffer):Boolean = {
    if (header.length < CONNECT.length) {
      false
    } else {
      header.startsWith(CONNECT) || header.startsWith(STOMP)
    }
  }
}

class StompProtocolFactory extends ProtocolFactory.Provider {

  def create() = StompProtocol

  def create(config: String) = if(config == "stomp") {
    StompProtocol
  } else {
    null
  }

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object StompProtocol extends StompProtocolCodecFactory with Protocol {

  def createProtocolHandler = new StompProtocolHandler

  def encode(message: Message):MessageRecord = {
    StompCodec.encode(message.asInstanceOf[StompFrameMessage])
  }

  def decode(message: MessageRecord) = {
    StompCodec.decode(message)
  }

}


class HeartBeatMonitor() {

  var transport:Transport = _
  var write_interval = 0L
  var read_interval = 0L

  var on_keep_alive = ()=>{}
  var on_dead = ()=>{}

  var session = 0

  def schedual_check_writes(session:Int):Unit = {
    val last_write_counter = transport.getProtocolCodec.getWriteCounter()
    transport.getDispatchQueue.after(write_interval, TimeUnit.MILLISECONDS) {
      if( this.session == session ) {
        if( last_write_counter==transport.getProtocolCodec.getWriteCounter ) {
          on_keep_alive()
        }
        schedual_check_writes(session)
      }
    }
  }

  def schedual_check_reads(session:Int):Unit = {
    val last_read_counter = transport.getProtocolCodec.getReadCounter()
    transport.getDispatchQueue.after(read_interval, TimeUnit.MILLISECONDS) {
      if( this.session == session ) {
        if( last_read_counter==transport.getProtocolCodec.getReadCounter ) {
          on_dead()
        }
        schedual_check_reads(session)
      }
    }
  }

  def start = {
    session += 1
    if( write_interval!=0 ) {
      schedual_check_writes(session)
    }
    if( read_interval!=0 ) {
      schedual_check_reads(session)
    }
  }

  def stop = {
    session += 1
  }
}

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

  override protected def log = StompProtocolHandler

  protected def dispatchQueue:DispatchQueue = connection.dispatchQueue

  trait AckHandler {
    def track(delivery:Delivery):Unit
    def perform_ack(msgid: AsciiBuffer, uow:StoreUOW=null):Unit
  }

  class AutoAckHandler extends AckHandler {
    def track(delivery:Delivery) = {
      if( delivery.ack!=null ) {
        delivery.ack(null)
      }
    }
    
    def perform_ack(msgid: AsciiBuffer, uow:StoreUOW=null) = {
      die("The subscription ack mode does not expect ACK frames")
    }
  }

  class SessionAckHandler extends AckHandler{
    var consumer_acks = ListBuffer[(AsciiBuffer, (StoreUOW)=>Unit)]()

    def track(delivery:Delivery) = {
      queue {
        if( protocol_version eq V1_0 ) {
          // register on the connection since 1.0 acks may not include the subscription id
          connection_ack_handlers += ( delivery.message.id-> this )
        }
        consumer_acks += (( delivery.message.id, delivery.ack ))
      }

    }


    def perform_ack(msgid: AsciiBuffer, uow:StoreUOW=null) = {

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
        die("ACK failed, invalid message id: %s".format(msgid))
      } else {
        consumer_acks = not_acked
        acked.foreach{case (id, ack)=>
          if( ack!=null ) {
            ack(uow)
          }
        }
      }

      if( protocol_version eq V1_0 ) {
        connection_ack_handlers.remove(msgid)
      }
    }



  }
  class MessageAckHandler extends AckHandler {
    var consumer_acks = HashMap[AsciiBuffer, (StoreUOW)=>Unit]()

    def track(delivery:Delivery) = {
      queue {
        if( protocol_version eq V1_0 ) {
          // register on the connection since 1.0 acks may not include the subscription id
          connection_ack_handlers += ( delivery.message.id-> this )
        }
        consumer_acks += ( delivery.message.id -> delivery.ack )
      }
    }

    def perform_ack(msgid: AsciiBuffer, uow:StoreUOW=null) = {
      consumer_acks.remove(msgid) match {
        case Some(ack) =>
          if( ack!=null ) {
            ack(uow)
          }
        case None => die("ACK failed, invalid message id: %s".format(msgid))
      }

      if( protocol_version eq V1_0 ) {
        connection_ack_handlers.remove(msgid)
      }
    }
  }

  class StompConsumer(val subscription_id:Option[AsciiBuffer], val destination:Destination, val ack_handler:AckHandler, val selector:(AsciiBuffer, BooleanExpression), val binding:BindingDTO) extends BaseRetained with DeliveryConsumer {
    val dispatchQueue = StompProtocolHandler.this.dispatchQueue


    dispatchQueue.retain
    setDisposer(^{
      session_manager.release
      dispatchQueue.release
    })

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
      retain

      def producer = p
      def consumer = StompConsumer.this

      val session = session_manager.open(producer.dispatchQueue)

      def close = {
        session_manager.close(session)
        release
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
  var login:Option[AsciiBuffer] = None
  var passcode:Option[AsciiBuffer] = None

  var heart_beat_monitor:HeartBeatMonitor = new HeartBeatMonitor

  var waiting_on:String = "client request"


  override def create_connection_status = {
    var rc = new StompConnectionStatusDTO
    rc.protocol_version = if( protocol_version == null ) null else protocol_version.toString
    rc.user = login.map(_.toString).getOrElse(null)
    rc.subscription_count = consumers.size
    rc.waiting_on = waiting_on
    rc
  }

  override def onTransportConnected() = {

    session_manager = new SinkMux[StompFrame]( MapSink(connection.transportSink){x=>x}, dispatchQueue, StompFrame)
    connection_sink = new OverflowSink(session_manager.open(dispatchQueue));
    connection_sink.refiller = ^{}
    resumeRead

  }

  override def onTransportDisconnected() = {
    if( !closed ) {
      heart_beat_monitor.stop
      closed=true;

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
            host.router.get_queue(consumer.binding) { queue=>
              queue.foreach( _.unbind(consumer::Nil) )
            }
          }
      }
      consumers = Map()
      trace("stomp protocol resources released")
    }
  }


  override def onTransportCommand(command:Any) = {
    try {
      command match {
        case s:StompCodec =>
          // this is passed on to us by the protocol discriminator
          // so we know which wire format is being used.
        case frame:StompFrame=>

          if( protocol_version == null ) {

            info("got command: %s", frame)
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
      case e:Exception =>
        warn(e, "Internal Server Error")
        die("Internal Server Error");
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

  def on_stomp_connect(headers:HeaderMap):Unit = {

    login = get(headers, LOGIN)
    passcode = get(headers, PASSCODE)

    val accept_versions = get(headers, ACCEPT_VERSION).getOrElse(V1_0).split(COMMA).map(_.ascii)
    protocol_version = SUPPORTED_PROTOCOL_VERSIONS.find( v=> accept_versions.contains(v) ) match {
      case Some(x) => x
      case None=>
        val supported_versions = SUPPORTED_PROTOCOL_VERSIONS.mkString(",")
        _die((MESSAGE_HEADER, ascii("version not supported"))::
            (VERSION, ascii(supported_versions))::Nil,
            "Supported protocol versions are %s".format(supported_versions))
        return
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
              die("Stale connection.  Missed heartbeat.")
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
            return
        }
      case _ =>
        die("Invalid heart-beat header: "+heart_beat)
        return
    }

    suspendRead("virtual host lookup")
    val host_header = get(headers, HOST)
    val cb: (VirtualHost)=>Unit = (host)=>
      queue {
        if(host!=null) {
          this.host=host

          val outbound_heart_beat_header = ascii("%d,%d".format(outbound_heartbeat,inbound_heartbeat))
          session_id = ascii(this.host.config.id + ":"+this.host.session_counter.incrementAndGet)

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
          resumeRead

        } else {
          die("Invalid virtual host: "+host_header.get)
        }
      }

    host_header match {
      case None=>
        connection.connector.broker.getDefaultVirtualHost(cb)
      case Some(host)=>
        connection.connector.broker.getVirtualHost(host, cb)
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
            get_or_create_tx_queue(txid){ txqueue=>
              txqueue.add(frame, (uow)=>{perform_send(frame, uow)} )
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
        host.router.connect(destiantion, producer) {
          route =>
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
  def next_message_id = {
    message_id_counter += 1
    // TODO: properly generate mesage ids
    new AsciiBuffer("msg:"+message_id_counter);
  }

  def send_via_route(route:DeliveryProducerRoute, frame:StompFrame, uow:StoreUOW) = {
    var storeBatch:StoreUOW=null
    // User might be asking for ack that we have processed the message..
    val receipt = frame.header(RECEIPT_REQUESTED)

    if( !route.targets.isEmpty ) {

      // We may need to add some headers..
      var message = get( frame.headers, MESSAGE_ID) match {
        case None=>
          var updated_headers:HeaderMap=Nil;
          updated_headers ::= (MESSAGE_ID, next_message_id)
          StompFrameMessage(StompFrame(MESSAGE, frame.headers, frame.content, updated_headers))
        case Some(id)=>
          StompFrameMessage(StompFrame(MESSAGE, frame.headers, frame.content))
      }

      val delivery = new Delivery
      delivery.message = message
      delivery.size = message.frame.size
      delivery.uow = uow

      if( receipt!=null ) {
        delivery.ack = { storeTx =>
          connection_sink.offer(StompFrame(RECEIPT, List((RECEIPT_ID, receipt))))
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
    val receipt = get(headers, RECEIPT_REQUESTED)

    val dest = get(headers, DESTINATION) match {
      case Some(dest)=> dest
      case None=>
        die("destination not set.")
        return
    }
    val destination:Destination = dest

    val subscription_id = get(headers, ID)
    var id:AsciiBuffer = subscription_id match {
      case None =>
        if( protocol_version eq V1_0 ) {
          // in 1.0 it's ok if the client does not send us the
          // the id header
          dest
        } else {
          die("The id header is missing from the SUBSCRIBE frame");
          return
        }
      case Some(x:AsciiBuffer)=> x
    }

    val topic = destination.getDomain == Router.TOPIC_DOMAIN
    var persistent = get(headers, PERSISTENT).map( _ == TRUE ).getOrElse(false)

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
          return;
      }
    }

    val selector = get(headers, SELECTOR) match {
      case None=> null
      case Some(x)=> x
        try {
          (x, SelectorParser.parse(x.utf8.toString))
        } catch {
          case e:FilterException =>
            die("Invalid selector expression: "+e.getMessage)
            return;
        }
    }

    if ( consumers.contains(id) ) {
      die("A subscription with identified with '"+id+"' allready exists")
      return;
    }

    info("subscribing to: %s", destination)
    val binding: BindingDTO = if( topic && !persistent ) {
      null
    } else {
      // Controls how the created queue gets bound
      // to the destination name space (this is used to
      // recover the queue on restart and rebind it the
      // way again)
      if (topic) {
        val rc = new DurableSubscriptionBindingDTO
        rc.destination = destination.getName.toString
        // TODO:
        // rc.client_id =
        rc.subscription_id = if( persistent ) id else null
        rc.filter = if (selector == null) null else selector._1
        rc
      } else {
        val rc = new PointToPointBindingDTO
        rc.destination = destination.getName.toString
        rc
      }
    }

    val consumer = new StompConsumer(subscription_id, destination, ack, selector, binding);
    consumers += (id -> consumer)

    if( binding==null ) {

      // consumer is bind bound as a topic
      host.router.bind(destination, consumer, ^{
        receipt.foreach{ receipt =>
          connection_sink.offer(StompFrame(RECEIPT, List((RECEIPT_ID, receipt))))
        }
      })
      consumer.release

    } else {

      // create a queue and bind the consumer to it.
      host.router.create_queue(binding) { x=>
        x match {
          case Some(queue:Queue) =>
            queue.bind(consumer::Nil)
            receipt.foreach{ receipt =>
              connection_sink.offer(StompFrame(RECEIPT, List((RECEIPT_ID, receipt))))
            }
            consumer.release
          case None => throw new RuntimeException("case not yet implemented.")
        }
      }
    }
  }

  def on_stomp_unsubscribe(headers:HeaderMap):Unit = {

    val receipt = get(headers, RECEIPT_REQUESTED)
    var persistent = get(headers, PERSISTENT).map( _ == TRUE ).getOrElse(false)

    val id = get(headers, ID).getOrElse {
      if( protocol_version eq V1_0 ) {
        // in 1.0 it's ok if the client does not send us the
        // the id header, the destination header must be set
        get(headers, DESTINATION) match {
          case Some(dest)=> dest
          case None=>
            die("destination not set.")
            return
        }
      } else {
        die("The id header is missing from the UNSUBSCRIBE frame");
        return
      }
    }

    consumers.get(id) match {
      case None=>
        die("The subscription '%s' not found.".format(id))
        return;
      case Some(consumer)=>
        // consumer.close
        if( consumer.binding==null ) {
          host.router.unbind(consumer.destination, consumer)
          receipt.foreach{ receipt =>
            connection_sink.offer(StompFrame(RECEIPT, List((RECEIPT_ID, receipt))))
          }
        } else {
          host.router.get_queue(consumer.binding) { queue=>
            queue.foreach( _.unbind(consumer::Nil) )
          }

          if( persistent && consumer.binding!=null ) {
            host.router.destroy_queue(consumer.binding){sucess=>
              receipt.foreach{ receipt =>
                connection_sink.offer(StompFrame(RECEIPT, List((RECEIPT_ID, receipt))))
              }
            }
          } else {
            receipt.foreach{ receipt =>
              connection_sink.offer(StompFrame(RECEIPT, List((RECEIPT_ID, receipt))))
            }
          }

        }

    }
  }

  def on_stomp_ack(frame:StompFrame):Unit = {
    val headers = frame.headers
    get(headers, MESSAGE_ID) match {
      case Some(messageId)=>

        val subscription_id = get(headers, SUBSCRIPTION);
        if( subscription_id == None && !(protocol_version eq V1_0) ) {
          die("The subscription header is required")
          return
        }

        val handler = subscription_id match {
          case None=>

            connection_ack_handlers.get(messageId) match {
              case None =>
                die("Not expecting ack for message id '%s'".format(messageId))
                None
              case Some(handler) =>
                Some(handler)
            }

          case Some(id) =>
            consumers.get(id) match {
              case None=>
                die("The subscription '%s' does not exist".format(id))
                None
              case Some(consumer)=>
                Some(consumer.ack_handler)
            }
        }

        handler.foreach{ handler=>

          get(headers, TRANSACTION) match {
            case None=>
              handler.perform_ack(messageId, null)
            case Some(txid)=>
              get_or_create_tx_queue(txid){ _.add(frame, (uow)=>{ handler.perform_ack(messageId, uow)} ) }
          }

          get(headers, RECEIPT_REQUESTED).foreach { receipt =>
            connection_sink.offer(StompFrame(RECEIPT, List((RECEIPT_ID, receipt))))
          }

        }


      case None=> die("message id header not set")
    }
  }

  private def die(msg:String, explained:String="") = {
    info("Shutting connection down due to: "+msg)
    _die((MESSAGE_HEADER, ascii(msg))::Nil, explained)
  }

  private def _die(headers:HeaderMap, explained:String="") = {
    if( !connection.stopped ) {
      suspendRead("shutdown")
      connection.transport.offer(StompFrame(ERROR, headers, BufferContent(ascii(explained))) )
      // TODO: if there are too many open connections we should just close the connection
      // without waiting for the error to get sent to the client.
      queue.after(die_delay, TimeUnit.MILLISECONDS) {
        connection.stop()
      }
    }
  }

  override def onTransportFailure(error: IOException) = {
    if( !connection.stopped ) {
      suspendRead("shutdown")
      info(error, "Shutting connection down due to: %s", error)
      super.onTransportFailure(error);
    }
  }


  def require_transaction_header[T](headers:HeaderMap)(proc:(AsciiBuffer)=>T):Option[T] = {
    get(headers, TRANSACTION) match {
      case None=> die("transaction header not set")
      None
      case Some(txid)=> Some(proc(txid))
    }
  }

  def on_stomp_begin(headers:HeaderMap) = {
    require_transaction_header(headers){ txid=>create_tx_queue(txid){ _ => send_receipt(headers) }  }
  }

  def on_stomp_commit(headers:HeaderMap) = {
    require_transaction_header(headers){ txid=>remove_tx_queue(txid){ _.commit { send_receipt(headers) } } }
  }

  def on_stomp_abort(headers:HeaderMap) = {
    require_transaction_header(headers){ txid=>remove_tx_queue(txid){ _.rollback { send_receipt(headers) } } }
  }


  def send_receipt(headers:HeaderMap) = {
    get(headers, RECEIPT_REQUESTED) match {
      case Some(receipt)=>
        connection_sink.offer(StompFrame(RECEIPT, List((RECEIPT_ID, receipt))))
      case None=>
    }
  }

  class TransactionQueue {
    // TODO: eventually we want to back this /w a broker Queue which
    // can provides persistence and memory swapping.

    val queue = ListBuffer[(StompFrame, (StoreUOW)=>Unit)]()

    def add(frame:StompFrame, proc:(StoreUOW)=>Unit) = {
      queue += ( frame->proc )
    }

    def commit(onComplete: => Unit) = {

      val uow = if( host.store!=null ) {
        host.store.createStoreUOW
      } else {
        null
      }

      queue.foreach { case (frame, proc) =>
        proc(uow)
//        frame.action match {
//          case SEND =>
//            perform_send(frame, uow)
//          case ACK =>
//            perform_ack(frame, uow)
//          case _ => throw new java.lang.AssertionError("assertion failed: only send or ack frames are transactional")
//        }
      }
      if( uow!=null ) {
        uow.onComplete(^{
          onComplete
        })
        uow.release
      } else {
        onComplete
      }

    }

    def rollback(onComplete: => Unit) = {
      queue.clear
      onComplete
    }

  }

  val transactions = HashMap[AsciiBuffer, TransactionQueue]()

  def create_tx_queue(txid:AsciiBuffer)(proc:(TransactionQueue)=>Unit) = {
    if ( transactions.contains(txid) ) {
      die("transaction allready started")
    } else {
      val queue = new TransactionQueue
      transactions.put(txid, queue)
      proc( queue )
    }
  }

  def get_or_create_tx_queue(txid:AsciiBuffer)(proc:(TransactionQueue)=>Unit) = {
    proc(transactions.getOrElseUpdate(txid, new TransactionQueue))
  }

  def remove_tx_queue(txid:AsciiBuffer)(proc:(TransactionQueue)=>Unit) = {
    transactions.remove(txid) match {
      case None=> die("transaction not active: %d".format(txid))
      case Some(txqueue)=> proc(txqueue)
    }
  }

}

