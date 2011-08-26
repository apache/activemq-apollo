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
import dto.{StompConnectionStatusDTO, StompDTO}
import org.fusesource.hawtdispatch._

import org.apache.activemq.apollo.broker._
import Buffer._
import java.lang.String
import protocol.{ProtocolFilter, HeartBeatMonitor, ProtocolHandler}
import security.SecurityContext
import Stomp._
import org.apache.activemq.apollo.selector.SelectorParser
import org.apache.activemq.apollo.filter.{BooleanExpression, FilterException}
import org.apache.activemq.apollo.broker.store._
import org.apache.activemq.apollo.util._
import java.util.concurrent.TimeUnit
import java.util.Map.Entry
import path.PathParser
import scala.util.continuations._
import org.apache.activemq.apollo.dto._
import org.apache.activemq.apollo.transport.tcp.SslTransport
import java.security.cert.X509Certificate
import collection.mutable.{ListBuffer, HashMap}
import java.io.IOException


case class RichBuffer(self:Buffer) extends Proxy {
  def + (rhs: Buffer) = {
    val rc = new Buffer(self.length + rhs.length)
    System.arraycopy(self.data, self.offset, rc.data, rc.offset, self.length)
    System.arraycopy(rhs.data, rhs.offset, rc.data, rc.offset+self.length, rhs.length)
    rc
  }
}

object BufferSupport {
  implicit def to_rich_buffer(value:Buffer):RichBuffer = RichBuffer(value)
}

import BufferSupport._

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


  def noop = shift {  k: (Unit=>Unit) => k() }
  def unit:Unit = {}
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class StompProtocolHandler extends ProtocolHandler {
  import StompProtocolHandler._

  var connection_log:Log = StompProtocolHandler
  def protocol = "stomp"
  def broker = connection.connector.broker

  def decode_header(value:Buffer):String = {
    var rc = new ByteArrayOutputStream(value.length)
    val pos = new Buffer(value)
    val max = value.offset + value.length
    while( pos.offset < max ) {
      if( pos.startsWith(ESCAPE_ESCAPE_SEQ) ) {
        rc.write(ESCAPE)
        pos.moveHead(2)
      } else if( pos.startsWith(COLON_ESCAPE_SEQ) ) {
        rc.write(COLON)
        pos.moveHead(2)
      } else if( pos.startsWith(NEWLINE_ESCAPE_SEQ) ) {
        rc.write(NEWLINE)
        pos.moveHead(2)
      } else {
        rc.write(pos.data(pos.offset))
        pos.moveHead(1)
      }
    }
    new String(rc.toByteArray, "UTF-8")
  }

  def encode_header(value:String) = {
    protocol_version match {
      case null => utf8(value).ascii
      case V1_0 => utf8(value).ascii
      case _ =>

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
  }

  protected def dispatchQueue:DispatchQueue = connection.dispatch_queue

  class StompConsumer(

    val subscription_id:Option[AsciiBuffer],
    val destination:Array[DestinationDTO],
    ack_mode:AsciiBuffer,
    val selector:(String, BooleanExpression),
    override val browser:Boolean,
    override val exclusive:Boolean,
    val auto_delete:Boolean,
    val initial_credit_window:(Int,Int, Boolean)
  ) extends BaseRetained with DeliveryConsumer {

////  The following comes in handy if we need to debug the
////  reference counts of the consumers.
//
//    val r = new BaseRetained
//
//    def setDisposer(p1: Runnable): Unit = r.setDisposer(p1)
//    def retained: Int =r.retained
//
//    def printST(name:String) = {
//      val e = new Exception
//      println(name+": ")
//      println("  "+e.getStackTrace.drop(1).take(4).mkString("\n  "))
//    }
//
//    def retain: Unit = {
//      printST("retain")
//      r.retain
//    }
//    def release: Unit = {
//      printST("release")
//      r.release
//    }

    val ack_source = createSource(new EventAggregator[(Int, Int), (Int, Int)] {
      def mergeEvent(previous:(Int, Int), event:(Int, Int)) = {
        if( previous == null ) {
          event
        } else {
          (previous._1+event._1, previous._2+event._2)
        }
      }
      def mergeEvents(previous:(Int, Int), events:(Int, Int)) = mergeEvent(previous, events)
    }, dispatch_queue)

    ack_source.setEventHandler(^ {
      val data = ack_source.getData
      credit_window_filter.credit(data._1, data._2)
    });
    ack_source.resume

    trait AckHandler {
      def track(delivery:Delivery):Unit
      def credit(msgid: AsciiBuffer, credit_value: (Int, Int)):Unit
      def perform_ack(consumed:DeliveryResult, msgid: AsciiBuffer, uow:StoreUOW=null):Unit
    }

    class AutoAckHandler extends AckHandler {

      def track(delivery:Delivery) = {
        if( delivery.ack!=null ) {
          delivery.ack(Consumed, null)
        }
        ack_source.merge((delivery.size, 1))
      }

      def credit(msgid: AsciiBuffer, credit_value: (Int, Int)):Unit = {
      }

      def perform_ack(consumed:DeliveryResult, msgid: AsciiBuffer, uow:StoreUOW=null) = {
        async_die("The subscription ack mode does not expect ACK or NACK frames")
      }

    }

    class TrackedAck(var credit:Option[Int], val ack:(DeliveryResult, StoreUOW)=>Unit)

    class SessionAckHandler extends AckHandler{
      var consumer_acks = ListBuffer[(AsciiBuffer, TrackedAck)]()

      def track(delivery:Delivery) = {
        queue.assertExecuting()
        if( protocol_version eq V1_0 ) {
          // register on the connection since 1.0 acks may not include the subscription id
          connection_ack_handlers += ( delivery.message.id-> this )
        }
        consumer_acks += delivery.message.id -> new TrackedAck(Some(delivery.size), delivery.ack )
      }

      def credit(msgid: AsciiBuffer, credit_value: (Int, Int)):Unit = {
        queue.assertExecuting()
        if( initial_credit_window._3 ) {
          var found = false
          val (acked, not_acked) = consumer_acks.partition{ case (id, ack)=>
            if( id == msgid ) {
              found = true
              true
            } else {
              !found
            }
          }

          for( (id, delivery) <- acked ) {
            for( credit <- delivery.credit ) {
              ack_source.merge((credit, 1))
              delivery.credit = None
            }
          }
        } else {
          if( credit_value!=null ) {
            ack_source.merge((credit_value._1, credit_value._2))
          }
        }
      }

      def perform_ack(consumed:DeliveryResult, msgid: AsciiBuffer, uow:StoreUOW=null) = {
        queue.assertExecuting()

        // session acks ack all previously received messages..
        var found = false
        val (acked, not_acked) = consumer_acks.partition{ case (id, ack)=>
          if( id == msgid ) {
            found = true
            true
          } else {
            !found
          }
        }

        if( !found ) {
          trace("%s: ACK failed, invalid message id: %s, dest: %s".format(security_context.remote_address, msgid, destination.mkString(",")))
        } else {
          consumer_acks = not_acked
          acked.foreach{case (id, delivery)=>
            if( delivery.ack!=null ) {
              delivery.ack(consumed, uow)
            }
          }
        }

        if( protocol_version eq V1_0 ) {
          connection_ack_handlers.remove(msgid)
        }
      }

    }

    class MessageAckHandler extends AckHandler {
      var consumer_acks = HashMap[AsciiBuffer, TrackedAck]()

      def track(delivery:Delivery) = {
        queue.assertExecuting();
        if( protocol_version eq V1_0 ) {
          // register on the connection since 1.0 acks may not include the subscription id
          connection_ack_handlers += ( delivery.message.id-> this )
        }
        consumer_acks += delivery.message.id -> new TrackedAck(Some(delivery.size), delivery.ack)
      }

      def credit(msgid: AsciiBuffer, credit_value: (Int, Int)):Unit = {
        queue.assertExecuting()
        if( initial_credit_window._3 ) {
          for( delivery <- consumer_acks.get(msgid)) {
            for( credit <- delivery.credit ) {
              ack_source.merge((credit,1))
              delivery.credit = None
            }
          }
        } else {
          if( credit_value!=null ) {
            ack_source.merge((credit_value._1, credit_value._2))
          }
        }
      }

      def perform_ack(consumed:DeliveryResult, msgid: AsciiBuffer, uow:StoreUOW=null) = {
        queue.assertExecuting()
        consumer_acks.remove(msgid) match {
          case Some(delivery) =>
            if( delivery.ack!=null ) {
              delivery.ack(consumed, uow)
            }
          case None => async_die("ACK failed, invalid message id: %s".format(msgid))
        }

        if( protocol_version eq V1_0 ) {
          connection_ack_handlers.remove(msgid)
        }
      }
    }

    val ack_handler = ack_mode match {
      case ACK_MODE_AUTO=>new AutoAckHandler
      case ACK_MODE_NONE=>new AutoAckHandler
      case ACK_MODE_CLIENT=> new SessionAckHandler
      case ACK_MODE_CLIENT_INDIVIDUAL=> new MessageAckHandler
      case ack:AsciiBuffer =>
        die("Unsupported ack mode: "+ack);
    }

    val consumer_sink = sink_manager.open()
    val credit_window_filter = new CreditWindowFilter[Delivery](consumer_sink.map { delivery =>
      ack_handler.track(delivery)
      var frame = delivery.message.asInstanceOf[StompFrameMessage].frame
      if( subscription_id != None ) {
        frame = frame.append_headers((SUBSCRIPTION, subscription_id.get)::Nil)
      }
      if( config.add_redeliveries_header!=null && delivery.redeliveries > 0) {
        val header = encode_header(config.add_redeliveries_header)
        val value = ascii(delivery.redeliveries.toString())
        frame = frame.append_headers((header, value)::Nil)
      }
      frame
    }, Delivery)

    credit_window_filter.credit(initial_credit_window._1, initial_credit_window._2)

    val session_manager = new SessionSinkMux[Delivery](credit_window_filter, dispatchQueue, Delivery) {
      override def time_stamp = broker.now
    }

    override def dispose() = dispatchQueue {
      super.dispose()
      sink_manager.close(consumer_sink)
    }

    def dispatch_queue = StompProtocolHandler.this.dispatchQueue

    override def connection = Some(StompProtocolHandler.this.connection)
    override def receive_buffer_size = codec.write_buffer_size

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

    class StompConsumerSession(val producer:DeliveryProducer) extends DeliverySession with SessionSinkFilter[Delivery] {
      producer.dispatch_queue.assertExecuting()
      retain

      val downstream = session_manager.open(producer.dispatch_queue, receive_buffer_size)

      override def toString = "connection to "+StompProtocolHandler.this.connection.transport.getRemoteAddress

      def consumer = StompConsumer.this
      var closed = false

      def close = {
        assert(producer.dispatch_queue.isExecuting)
        if( !closed ) {
          closed = true
          if( browser ) {
            // Then send the end of browse message.
            val headers:HeaderMap = List(DESTINATION->EMPTY, MESSAGE_ID->EMPTY, BROWSER->END)
            var frame = StompFrame(MESSAGE, headers, BufferContent(EMPTY_BUFFER))
            if( subscription_id != None ) {
              frame = frame.append_headers((SUBSCRIPTION, subscription_id.get)::Nil)
            }

            val delivery = new Delivery()
            delivery.message = StompFrameMessage(frame)
            delivery.size = frame.size

            if( downstream.full ) {
              // session is full so use an overflow sink so to hold the message,
              // and then trigger closing the session once it empties out.
              val sink = new OverflowSink(downstream)
              sink.refiller = ^{
                dispose
              }
              sink.offer(delivery)
            } else {
              downstream.offer(delivery)
              dispose
            }
          } else {
            dispose
          }
        }
      }

      def dispose = {
        session_manager.close(downstream)
        if( auto_delete ) {
          reset {
            val rc = host.router.delete(destination, security_context)
            rc match {
              case Some(error) =>
                async_die(error)
              case None =>
                unit
            }
          }
        }
        release
      }

      // Delegate all the flow control stuff to the session
      def offer(delivery:Delivery) = {
        if( full ) {
          false
        } else {
          delivery.message.retain()
          val rc = downstream.offer(delivery)
          assert(rc, "offer should be accepted since it was not full")
          true
        }
      }

    }
    def connect(p:DeliveryProducer) = new StompConsumerSession(p)
  }

//  var session_manager:SessionSinkMux[StompFrame] = null
  var sink_manager:SinkMux[StompFrame] = null
  var connection_sink:Sink[StompFrame] = null

  var dead = false
  var closed = false
  var consumers = Map[AsciiBuffer, StompConsumer]()

  var producerRoutes = new LRUCache[List[DestinationDTO], DeliveryProducerRoute](10) {
    override def onCacheEviction(eldest: Entry[List[DestinationDTO], DeliveryProducerRoute]) = {
      host.router.disconnect(eldest.getKey.toArray, eldest.getValue)
    }
  }

  var host:VirtualHost = null

  private def queue = connection.dispatch_queue

  // uses by STOMP 1.0 clients
  var connection_ack_handlers = HashMap[AsciiBuffer, StompConsumer#AckHandler]()

  var protocol_version:AsciiBuffer = _

  var heart_beat_monitor:HeartBeatMonitor = new HeartBeatMonitor
  val security_context = new SecurityContext
  var waiting_on:String = "client request"
  var config:StompDTO = _
  var session_id:AsciiBuffer = _

  var protocol_filters = List[ProtocolFilter]()

  var destination_parser = Stomp.destination_parser

  var codec:StompCodec = _

  implicit def toDestinationDTO(value:AsciiBuffer):Array[DestinationDTO] = {
    val rc = destination_parser.decode_destination(value.toString)
    if( rc==null ) {
      throw new ProtocolException("Invalid stomp destiantion name: "+value);
    }
    rc
  }

  override def set_connection(connection: BrokerConnection) = {
    super.set_connection(connection)
    import collection.JavaConversions._

    codec = connection.transport.getProtocolCodec.asInstanceOf[StompCodec]
    val connector_config = connection.connector.config.asInstanceOf[AcceptingConnectorDTO]
    config = connector_config.protocols.find( _.isInstanceOf[StompDTO]).map(_.asInstanceOf[StompDTO]).getOrElse(new StompDTO)

    protocol_filters = ProtocolFilter.create_filters(config.protocol_filters.toList, this)

    import OptionSupport._
    config.max_data_length.foreach( codec.max_data_length = _ )
    config.max_header_length.foreach( codec.max_header_length = _ )
    config.max_headers.foreach( codec.max_headers = _ )

    if( config.queue_prefix!=null ||
        config.topic_prefix!=null ||
        config.destination_separator!=null ||
        config.path_separator!= null ||
        config.any_child_wildcard != null ||
        config.any_descendant_wildcard!= null ||
        config.regex_wildcard_start!= null ||
        config.regex_wildcard_end!= null
    ) {

      destination_parser = new DestinationParser().copy(Stomp.destination_parser)
      if( config.queue_prefix!=null ) { destination_parser.queue_prefix = config.queue_prefix }
      if( config.topic_prefix!=null ) { destination_parser.topic_prefix = config.topic_prefix }
      if( config.destination_separator!=null ) { destination_parser.destination_separator = config.destination_separator }
      if( config.path_separator!=null ) { destination_parser.path_separator = config.path_separator }
      if( config.any_child_wildcard!=null ) { destination_parser.any_child_wildcard = config.any_child_wildcard }
      if( config.any_descendant_wildcard!=null ) { destination_parser.any_descendant_wildcard = config.any_descendant_wildcard }
      if( config.regex_wildcard_start!=null ) { destination_parser.regex_wildcard_start = config.regex_wildcard_start }
      if( config.regex_wildcard_end!=null ) { destination_parser.regex_wildcard_end = config.regex_wildcard_end }

    }

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

  private def async_die(headers:HeaderMap, body:String) = try {
    die(headers, body)
  } catch {
    case x:Break=>
  }

  private def die[T](msg:String, e:Throwable=null):T = {
    if( e!=null) {
      connection_log.info(e, "STOMP connection '%s' error: %s", security_context.remote_address, msg)
    } else {
      connection_log.info("STOMP connection '%s' error: %s", security_context.remote_address, msg)
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

  override def on_transport_connected() = {
    connection_log = connection.connector.broker.connection_log
    sink_manager = new SinkMux[StompFrame]( connection.transport_sink.map {x=>
      trace("sending frame: %s", x)
      x
    })
    connection_sink = new OverflowSink(sink_manager.open());
    resumeRead
  }

  override def on_transport_disconnected() = {
    if( !closed ) {
      heart_beat_monitor.stop
      closed=true;
      dead = true;

      import collection.JavaConversions._
      producerRoutes.foreach{
        case(dests,route)=> host.router.disconnect(dests.toArray, route)
      }
      producerRoutes.clear
      consumers.foreach {
        case (_,consumer)=>
          host.router.unbind(consumer.destination, consumer, false , security_context)
      }
      consumers = Map()
      trace("stomp protocol resources released")
    }
  }


  override def on_transport_command(command:AnyRef):Unit = {
    if( dead ) {
      // We stop processing client commands once we are dead
      return;
    }
    try {
      command match {
        case s:StompCodec =>
          // this is passed on to us by the protocol discriminator
          // so we know which wire format is being used.
        case f:StompFrame=>

          trace("received frame: %s", f)

          var frame = f
          protocol_filters.foreach { filter =>
            frame = filter.filter(frame)
          }

          if( protocol_version == null ) {

            frame.action match {
              case STOMP =>
                on_stomp_connect(frame.headers)
              case CONNECT =>
                on_stomp_connect(frame.headers)
              case _ =>
                die("Expecting a STOMP or CONNECT frame, but got: "+frame.action.ascii);
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

                val delay = send_receipt(frame.headers)!=null
                on_transport_disconnected
                if( delay ) {
                  queue.after(die_delay, TimeUnit.MILLISECONDS) {
                    connection.stop()
                  }
                } else {
                  // no point in delaying the connection shutdown
                  // if the client does not want a receipt..
                  connection.stop()
                }

              case _ =>
                die("Invalid frame: "+frame.action);
            }
          }

        case _=>
          die("Internal Server Error: unexpected command type");
      }
    }  catch {
      case e: Break =>
      case e:Exception =>
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

  def on_stomp_connect(headers:HeaderMap):Unit = {

    connection.transport match {
      case t:SslTransport=>
        security_context.certificates = Option(t.getPeerX509Certificates).getOrElse(Array[X509Certificate]())
      case _ => None
    }

    security_context.connection_id = Some(connection.id)
    security_context.local_address = connection.transport.getLocalAddress
    security_context.remote_address = connection.transport.getRemoteAddress
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

    if( protocol_version != V1_0 ) {
      // disable trimming...
      connection.transport.getProtocolCodec.asInstanceOf[StompCodec].trim = false
    }

    val heart_beat = get(headers, HEART_BEAT).getOrElse(DEFAULT_HEART_BEAT)
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

    def send_connected = {

      var connected_headers = ListBuffer((VERSION, protocol_version))

      connected_headers += SERVER->encode_header("apache-apollo/"+Broker.version)

      session_id = encode_header("%s-%x-".format(this.host.config.id, this.host.session_counter.incrementAndGet))
      connected_headers += SESSION->session_id

      val outbound_heart_beat_header = ascii("%d,%d".format(outbound_heartbeat,inbound_heartbeat))
      connected_headers += HEART_BEAT->outbound_heart_beat_header

      if( host.authenticator!=null ) {
        host.authenticator.user_name(security_context).foreach{ name=>
          connected_headers += USER_ID->encode_header(name)
        }
      }

      connection_sink.offer(StompFrame(CONNECTED,connected_headers.toList))

      if( this.host.store!=null && this.host.store.zero_copy_buffer_allocator!=null ) {
        val wf = connection.transport.getProtocolCodec.asInstanceOf[StompCodec]
        wf.zero_copy_buffer_allocator = this.host.store.zero_copy_buffer_allocator
      }
    }

    reset {
      suspendRead("virtual host lookup")
      val host_header = get(headers, HOST)
      val host = host_header match {
        case None=>
          connection.connector.broker.get_default_virtual_host
        case Some(host)=>
          connection.connector.broker.get_virtual_host(host)
      }
      resumeRead

      if(host==null) {
        async_die("Invalid virtual host: "+host_header.get)
        noop
      } else if(!host.service_state.is_started) {
        var headers = (MESSAGE_HEADER, encode_header("Virtual host stopped")) :: Nil
        host.client_redirect.foreach(x=> headers ::= REDIRECT_HEADER->encode_header(x) )
        async_die(headers, "")
        noop
      } else {
        this.host=host
        connection_log = host.connection_log
        if( host.authenticator!=null &&  host.authorizer!=null ) {
          suspendRead("authenticating and authorizing connect")
          if( !host.authenticator.authenticate(security_context) ) {
            var msg = if( security_context.user==null ) {
              "Authentication failed."
            } else {
              "Authentication failed. Username="+security_context.user
            }
            async_die(msg)
            noop // to make the cps compiler plugin happy.
          } else if( !host.authorizer.can_connect_to(security_context, host, connection.connector) ) {

            var msg = if( security_context.user==null ) {
              "Connect not authorized."
            } else {
              "Connect not authorized. Username="+security_context.user
            }
            async_die(msg)
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

    val destiantion: Array[DestinationDTO] = get(frame.headers, DESTINATION).get
    val key = destiantion.toList
    producerRoutes.get(key) match {
      case null =>
        // create the producer route...

        val route = new DeliveryProducerRoute(host.router) {
          override def send_buffer_size = codec.read_buffer_size
          override def connection = Some(StompProtocolHandler.this.connection)
          override def dispatch_queue = queue

          refiller = ^{
            resumeRead
          }
        }

        // don't process frames until producer is connected...
        connection.transport.suspendRead
        reset {
          val rc = host.router.connect(destiantion, route, security_context)
          rc match {
            case Some(failure) =>
              async_die(failure)
            case None =>
              if (!connection.stopped) {
                resumeRead
                producerRoutes.put(key, route)
                send_via_route(route, frame, uow)
              }
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
      message_id_counter += 1
      val msgid: Buffer = session_id + encode_header(message_id_counter.toString())
      rc ::= (MESSAGE_ID -> msgid.ascii)
    }

    if( config.add_timestamp_header!=null ) {
      rc ::= (encode_header(config.add_timestamp_header), ascii(broker.now.toString()))
    }

    // Do we need to add the user id?
    if( host.authenticator!=null ) {
      if( config.add_user_header!=null ) {
        host.authenticator.user_name(security_context).foreach{ name=>
          rc ::= (encode_header(config.add_user_header), encode_header(name))
        }
      }
      if( !config.add_user_headers.isEmpty ){
        import collection.JavaConversions._
        config.add_user_headers.foreach { h =>
          val matches = security_context.principles(Option(h.kind).getOrElse("*"))
          if( !matches.isEmpty ) {
            h.separator match {
              case null=>
                rc ::= (encode_header(h.name.trim), encode_header(matches.head.allow))
              case separator =>
                rc ::= (encode_header(h.name.trim), encode_header(matches.map(_.allow).mkString(separator)))
            }
          }
        }
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
        suspendRead("blocked sending to: "+route.overflowSessions.mkString(", "))
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
    var destination:Array[DestinationDTO] = dest

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

//    val topic = destination.isInstanceOf[TopicDestinationDTO]
    var persistent = get(headers, PERSISTENT).map( _ == TRUE ).getOrElse(false)
    var browser = get(headers, BROWSER).map( _ == TRUE ).getOrElse(false)
    var exclusive = get(headers, EXCLUSIVE).map( _ == TRUE ).getOrElse(false)
    var auto_delete = get(headers, AUTO_DELETE).map( _ == TRUE ).getOrElse(false)
    val ack_mode = get(headers, ACK_MODE).getOrElse(ACK_MODE_AUTO)
    val credit_window = get(headers, CREDIT) match {
      case Some(value) =>
        value.toString.split(",").toList match {
          case x :: Nil =>
            (codec.write_buffer_size, x.toInt, true)
          case x :: y :: Nil =>
            (y.toInt, x.toInt, true)
          case x :: y :: z :: _ =>
            (y.toInt, x.toInt, z.toBoolean)
          case _ => (codec.write_buffer_size, 1, true)
        }
      case None =>
        (codec.write_buffer_size, 1, true)
    }

    if(auto_delete) {
      if( destination.length != 1 ) {
        die("The auto-delete subscription header cannot be used in conjunction with composite destinations");
      }
      val path = destination_parser.decode_path(destination.head.path)
      if( PathParser.containsWildCards(path) ) {
        die("The auto-delete subscription header cannot be used in conjunction with wildcard destinations");
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

    if( persistent ) {
      destination = destination.map { _ match {
        case x:DurableSubscriptionDestinationDTO=>
          x
        case x:TopicDestinationDTO=>
          val rc = new DurableSubscriptionDestinationDTO()
          rc.path = x.path
          rc.subscription_id = decode_header(id)
          rc.selector = if (selector == null) null else selector._1
          rc
        case _ => die("A persistent subscription can only be used on a topic destination")
        }
      }
    }

    val consumer = new StompConsumer(subscription_id, destination, ack_mode, selector, browser, exclusive, auto_delete, credit_window);
    consumers += (id -> consumer)

    reset {
      val rc = host.router.bind(destination, consumer, security_context)
      consumer.release
      rc match {
        case Some(reason)=>
          consumers -= id
          async_die(reason)
        case None =>
          send_receipt(headers)
          unit
      }
    }

//      reset {
//        // create a queue and bind the consumer to it.
//        val x= host.router.get_or_create_queue(binding, security_context)
//        x match {
//          case Success(queue) =>
//            val rc = queue.bind(consumer, security_context)
//            consumer.release
//            rc match {
//              case Failure(reason)=>
//                consumers -= id
//                async_die(reason)
//              case _ =>
//                send_receipt(headers)
//            }
//          case Failure(reason) =>
//            consumers -= id
//            async_die(reason)
//        }
//      }
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

        // consumer gets disposed after all producer stop sending to it...
        consumer.setDisposer(^{ send_receipt(headers) })
        consumers -= id
        host.router.unbind(consumer.destination, consumer, persistent, security_context)
    }
  }

  def on_stomp_ack(frame:StompFrame):Unit = {
    on_stomp_ack(frame.headers, Consumed)
  }

  def on_stomp_nack(frame:StompFrame):Unit = {
    on_stomp_ack(frame.headers, Delivered)
  }

  def on_stomp_ack(headers:HeaderMap, consumed:DeliveryResult):Unit = {
    val credit = get(headers, CREDIT) match {
      case None => null
      case Some(value) =>
        value.toString.split(",").toList match {
          case x :: Nil =>
            (0, x.toInt)
          case x :: y :: _ =>
            (y.toInt, x.toInt)
          case _ => (0,0)
        }

    }
    val messageId = get(headers, MESSAGE_ID).getOrElse(null)

    if( credit==null && messageId==null) {
      die("message id header not set")
    }

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
      handler.credit(messageId, credit)
      if( messageId!=null ) {
        get(headers, TRANSACTION) match {
          case None=>
            handler.perform_ack(consumed, messageId, null)
          case Some(txid)=>
            get_or_create_tx_queue(txid).add{ uow=>
              handler.perform_ack(consumed, messageId, uow)
            }
        }
      }
      send_receipt(headers)
    }
  }


  override def on_transport_failure(error: IOException) = {
    if( !connection.stopped ) {
      suspendRead("shutdown")
      connection_log.debug(error, "Shutting connection '%s'  down due to: %s", security_context.remote_address, error)
      super.on_transport_failure(error);
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


  def send_receipt(headers:HeaderMap) = {
    get(headers, RECEIPT_REQUESTED) match {
      case Some(receipt)=>
        val frame = StompFrame(RECEIPT, List((RECEIPT_ID, receipt)))
        dispatchQueue <<| ^{
          connection_sink.offer(frame)
        }
        frame
      case None=>
        null
    }
  }

  class TransactionQueue {
    // TODO: eventually we want to back this /w a broker Queue which
    // can provides persistence and memory swapping.

    val queue = ListBuffer[(StoreUOW)=>Unit]()

    def add(proc:(StoreUOW)=>Unit):Unit = {
      queue += proc
    }

    def commit(on_complete: => Unit) = {
      if( host.store!=null ) {
        val uow = host.store.create_uow
//        println("UOW starting: "+uow.asInstanceOf[DelayingStoreSupport#DelayableUOW].uow_id)
        uow.on_complete {
//          println("UOW completed: "+uow.asInstanceOf[DelayingStoreSupport#DelayableUOW].uow_id)
          on_complete
        }
        queue.foreach{ _(uow) }
        uow.release
      } else {
        queue.foreach{ _(null) }
        on_complete
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

