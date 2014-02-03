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

import org.fusesource.hawtbuf._
import collection.mutable.{ListBuffer, HashMap}
import dto.{StompConnectionStatusDTO, StompDTO}
import java.io.IOException
import java.lang.String
import java.util
import java.util.concurrent.TimeUnit
import language.implicitConversions
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.broker.store._
import org.apache.activemq.apollo.dto._
import org.apache.activemq.apollo.filter.{BooleanExpression, FilterException}
import org.apache.activemq.apollo.selector.SelectorParser
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.util.path.LiteralPart
import org.fusesource.hawtdispatch._
import org.fusesource.hawtdispatch.transport.HeartBeatMonitor
import org.apache.activemq.apollo.util.path.{Path, PathParser}
import org.apache.activemq.apollo.broker.protocol.{ProtocolFilter3, ProtocolHandler}
import org.apache.activemq.apollo.broker.security.SecurityContext
import org.fusesource.hawtbuf.Buffer._
import Stomp._


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


object StompProtocolHandler extends Log {

  // How long we hold a failed connection open so that the remote end
  // can get the resulting error message.
  val DEFAULT_DIE_DELAY = 5*1000L

    // How often we can send heartbeats of the connection is idle.
  val DEFAULT_OUTBOUND_HEARTBEAT = 100L
  var outbound_heartbeat = DEFAULT_OUTBOUND_HEARTBEAT

  // How often we want to get heartbeats from the peer if the connection is idle.
  val DEFAULT_INBOUND_HEARTBEAT = 10*1000L
  var inbound_heartbeat = DEFAULT_INBOUND_HEARTBEAT

  val WAITING_ON_CLIENT_REQUEST = ()=> "client request"

  object SessionDeliverySizer extends Sizer[(Session[Delivery], Delivery)] {
    def size(value: (Session[Delivery], Delivery)) = Delivery.size(value._2)
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

  def get(headers:HeaderMap, names:List[AsciiBuffer]):List[Option[AsciiBuffer]] = {
    names.map(x=>get(headers, x))
  }

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
      } else if( pos.startsWith(CR_ESCAPE_SEQ) ) {
        rc.write(CR)
        pos.moveHead(2)
      } else {
        rc.write(pos.data(pos.offset))
        pos.moveHead(1)
      }
    }
    new String(rc.toByteArray, "UTF-8")
  }

  def encode_header(value:String, protocol_version:AsciiBuffer=V1_1):AsciiBuffer = {
    protocol_version match {
      case null => utf8(value).ascii
      case V1_0 => utf8(value).ascii
      case V1_1 =>
        val data = value.getBytes("UTF-8")
        var rc = new ByteArrayOutputStream(data.length)
        data.foreach {
          case ESCAPE  => rc.write(ESCAPE_ESCAPE_SEQ)
          case COLON   => rc.write(COLON_ESCAPE_SEQ)
          case NEWLINE => rc.write(COLON_ESCAPE_SEQ)
          case c       => rc.write(c)
        }
        rc.toBuffer.ascii

      case _ =>

        val data = value.getBytes("UTF-8")
        var rc = new ByteArrayOutputStream(data.length)
        data.foreach {
          case ESCAPE  => rc.write(ESCAPE_ESCAPE_SEQ)
          case COLON   => rc.write(COLON_ESCAPE_SEQ)
          case NEWLINE => rc.write(COLON_ESCAPE_SEQ)
          case CR      => rc.write(CR_ESCAPE_SEQ)
          case c       => rc.write(c)
        }
        rc.toBuffer.ascii
    }
  }

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class StompProtocolHandler extends ProtocolHandler {

  import StompProtocolHandler._

  var connection_log:Log = StompProtocolHandler

  def encode_header(value:String):AsciiBuffer = StompProtocolHandler.encode_header(value, protocol_version)

  def protocol = "stomp"
  def broker = connection.connector.broker

  protected def dispatchQueue:DispatchQueue = connection.dispatch_queue

  case class InitialCreditWindow(count:Int,size:Int,auto_credit:Boolean)


  //////////////////////////////////////////////////////////////////
  //
  // Since ack id's are re-useable once they are acked by the client,
  // try to re-use them since the first ones generated will be the
  // shortest ack-ids available.
  //
  //////////////////////////////////////////////////////////////////
  var ack_id_counter = 1L
  var ack_id_pool = new util.HashSet[AsciiBuffer]()
  def checkout_ack_id:AsciiBuffer = {
    if( ack_id_pool!=null && ack_id_pool.size()>0 ) {
      val i = ack_id_pool.iterator();
      val rc = i.next()
      i.remove()
      rc
    } else {
      ack_id_counter += 1
      ascii(java.lang.Long.toHexString(ack_id_counter))
    }
  }

  def checkin_ack_id(id:AsciiBuffer) = {
    if( ack_id_pool!=null ) {
      if( ack_id_pool.size() < 0xFFF ) {
        ack_id_pool.add(id);
      } else {
        // if we are sending too many messages at once
        // then it might not makes sense to pool..
        ack_id_pool = null
      }
    }
  }

  class StompConsumer (

    val subscription_id:Option[AsciiBuffer],
    val addresses:Array[_ <: BindAddress],
    ack_mode:AsciiBuffer,
    val selector:(String, BooleanExpression),
    override val browser:Boolean,
    override val exclusive:Boolean,
    val initial_credit_window:InitialCreditWindow,
    val include_seq:Option[AsciiBuffer],
    val from_seq:Long,
    override val close_on_drain:Boolean
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


    override def toString ={
      // Lets setup some helpers
      def b( x:Boolean, y: =>String):String  = if(x) y else ""
      def h( x:AnyRef, y: =>String):String  = if(x!=null) y else ""
      def o[T]( x:Option[T], y: =>String):String = if(x.isDefined) y else ""

      "StompConsumer("+
        o(subscription_id, "subscription_id: "+subscription_id.get+", ")+
        "addresses: "+addresses.mkString(",")+", "+
        "ack_mode: "+ack_mode+", "+
        initial_credit_window+
        h(selector, ", selector: "+selector._1)+
        b(browser, ", browser")+
        b(close_on_drain, ", close_on_drain")+
        b(exclusive, ", exclusive")+
        b(from_seq!=0, ", from_seq: "+from_seq)+
        o(include_seq, ", include_seq: "+include_seq)+
        ", "+session_manager+
      ")"
    }

    override def start_from_tail = from_seq == -1

    override def jms_selector = if(selector!=null){ selector._1 } else { null }

    override def user = security_context.user

    var starting_seq:Long = 0L
    override def set_starting_seq(seq: Long):Unit = {
      starting_seq=seq
    }

    val credit_window_source = createSource(new EventAggregator[(Int, Int), (Int, Int)] {
      def mergeEvent(previous:(Int, Int), event:(Int, Int)) = {
        if( previous == null ) {
          event
        } else {
          (previous._1+event._1, previous._2+event._2)
        }
      }
      def mergeEvents(previous:(Int, Int), events:(Int, Int)) = mergeEvent(previous, events)
    }, dispatch_queue)

    credit_window_source.setEventHandler(^ {
      val data = credit_window_source.getData
      credit_window_filter.credit(data._1, data._2)
    });
    credit_window_source.resume

    trait AckHandler {
      def track(session:Session[Delivery], msgid: AsciiBuffer, size:Int, ack:(DeliveryResult, StoreUOW)=>Unit):Unit
      def credit(msgid: AsciiBuffer, credit_value: (Int, Int)):Unit
      def perform_ack(consumed:DeliveryResult, msgid: AsciiBuffer, uow:StoreUOW=null):Unit
      def close:Unit
      def consumer = StompConsumer.this
    }

    class AutoAckHandler extends AckHandler {
      var closed = false

      def close = { closed  = true}

      def track(session:Session[Delivery], msgid: AsciiBuffer, size:Int, ack:(DeliveryResult, StoreUOW)=>Unit) = {
        if( closed ) {
          if( ack!=null ) {
            ack(Undelivered, null)
          }
        } else {
          if( ack!=null ) {
            ack(Consumed, null)
          }
          if( !dead ) {
            credit_window_source.merge((1, size))
          }
        }
      }

      def credit(msgid: AsciiBuffer, credit_value: (Int, Int)):Unit = {
      }

      def perform_ack(consumed:DeliveryResult, msgid: AsciiBuffer, uow:StoreUOW=null) = {
        async_die("The subscription ack mode does not expect ACK or NACK frames")
      }

    }

    class TrackedAck(var credit:Option[(Session[Delivery], Int)], val ack:(DeliveryResult, StoreUOW)=>Unit)

    class SessionAckHandler extends AckHandler{
      var consumer_acks = ListBuffer[(AsciiBuffer, TrackedAck)]()

      def close = {
        queue.assertExecuting()
        consumer_acks.foreach { case(_, tack) =>
          if( tack.ack !=null ) {
            tack.ack(Delivered, null)
          }
        }
        consumer_acks = null
      }

      def track(session:Session[Delivery], msgid: AsciiBuffer, size:Int, ack:(DeliveryResult, StoreUOW)=>Unit) = {
        queue.assertExecuting()
        if( consumer_acks == null ) {
          // It can happen if we get closed.. but destination is still sending data..
          if( ack!=null ) {
            ack(Undelivered, null)
          }
        } else {
          if( (protocol_version eq V1_0) || (protocol_version eq V1_2) ) {
            // register on the connection since 1.0 acks may not include the subscription id
            connection_ack_handlers += ( msgid -> this )
          }
          if( initial_credit_window.auto_credit) {
            consumer_acks += msgid -> new TrackedAck(Some((session, size)), ack )
          } else {
//            session_manager.delivered(session, size)
          }
        }
      }

      def credit(msgid: AsciiBuffer, credit_value: (Int, Int)):Unit = {
        queue.assertExecuting()
        if( initial_credit_window.auto_credit ) {

          val acked = if( !consumer_acks.isEmpty && consumer_acks.headOption.get._1 == msgid ) {
            Seq(consumer_acks.headOption.get)
          } else {
            var found = false
            val (acked, _) = consumer_acks.partition{ case (id, ack)=>
              if( id == msgid ) {
                found = true
                true
              } else {
                !found
              }
            }
            acked
          }

          for( (id, delivery) <- acked ) {
            for( credit <- delivery.credit ) {
//              session_manager.delivered(credit._1, credit._2)
              credit_window_source.merge((1, credit._2))
              delivery.credit = None
            }
          }
        } else {
          if( credit_value!=null ) {
            credit_window_source.merge((credit_value._1, credit_value._2))
          }
        }
      }

      def perform_ack(consumed:DeliveryResult, msgid: AsciiBuffer, uow:StoreUOW=null) = {
        queue.assertExecuting()
        assert(consumer_acks !=null)

        val acked = if( !consumer_acks.isEmpty && consumer_acks.headOption.get._1 == msgid ) {
          Seq(consumer_acks.remove(0))
        } else {
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
            trace("%s: ACK failed, invalid message id: %s, dest: %s".format(security_context.remote_address, msgid, addresses.mkString(",")))
          }
          consumer_acks = not_acked
          acked
        }

        acked.foreach{case (id, delivery)=>
          if( delivery.ack!=null ) {
            delivery.ack(consumed, uow)
          }
        }

        if( protocol_version eq V1_0 ) {
          connection_ack_handlers.remove(msgid)
        } else if( protocol_version eq V1_2 ) {
          connection_ack_handlers.remove(msgid)
          checkin_ack_id(msgid)
        }
      }

    }

    class MessageAckHandler extends AckHandler {
      var consumer_acks = HashMap[AsciiBuffer, TrackedAck]()

      def close = {
        queue.assertExecuting()
        consumer_acks.foreach { case(_, tack) =>
          if( tack.ack !=null ) {
            tack.ack(Delivered, null)
          }
        }
        consumer_acks = null
      }

      def track(session:Session[Delivery], msgid: AsciiBuffer, size:Int, ack:(DeliveryResult, StoreUOW)=>Unit) = {
        queue.assertExecuting();
        if( consumer_acks == null ) {
          // It can happen if we get closed.. but destination is still sending data..
          if( ack!=null ) {
            ack(Undelivered, null)
          }
        } else {
          if( (protocol_version eq V1_0) || (protocol_version eq V1_2) ) {
            // register on the connection since 1.0 acks may not include the subscription id
            connection_ack_handlers += ( msgid -> this )
          }
          if( initial_credit_window.auto_credit ) {
            consumer_acks += msgid -> new TrackedAck(Some((session, size)), ack)
          } else {
//            session_manager.delivered(session, size)
          }
        }
      }

      def credit(msgid: AsciiBuffer, credit_value: (Int, Int)):Unit = {
        queue.assertExecuting()
        if( initial_credit_window.auto_credit ) {
          for( delivery <- consumer_acks.get(msgid)) {
            for( credit <- delivery.credit ) {
//              session_manager.delivered(credit._1, credit._2)
              credit_window_source.merge((1, credit._2))
              delivery.credit = None
            }
          }
        } else {
          if( credit_value!=null ) {
            credit_window_source.merge((credit_value._1, credit_value._2))
          }
        }
      }

      def perform_ack(consumed:DeliveryResult, msgid: AsciiBuffer, uow:StoreUOW=null) = {
        queue.assertExecuting()
        assert(consumer_acks !=null)
        consumer_acks.remove(msgid) match {
          case Some(delivery) =>
            if( delivery.ack!=null ) {
              delivery.ack(consumed, uow)
            }
          case None => async_die("ACK failed, invalid message id: %s".format(msgid))
        }

        if( protocol_version eq V1_0 ) {
          connection_ack_handlers.remove(msgid)
        } else if( protocol_version eq V1_2 ) {
          connection_ack_handlers.remove(msgid)
          checkin_ack_id(msgid)
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
    val credit_window_filter = new CreditWindowFilter[(Session[Delivery], Delivery)](consumer_sink.flatMap { event =>
      val (session, delivery) = event

      // perhaps it has expired.. no need to deliver.
      if( delivery.expiration != 0 && delivery.expiration <= Broker.now ) {
        session_manager.delivered(session, delivery.size)
        if( delivery.ack != null ) {
          delivery.ack(Expired, null)
        }
        None
      } else {
        val message = delivery.message
        var frame = if( message.codec eq StompMessageCodec ) {
          message.asInstanceOf[StompFrameMessage].frame
        } else {
          val (body, content_type) =  protocol_convert match{
            case "body" => (message.getBodyAs(classOf[Buffer]), "protocol/"+message.codec.id+";conv=body")
            case _ => (message.encoded, "protocol/"+message.codec.id())
          }
          message_id_counter += 1
          var headers =  (MESSAGE_ID -> ascii(session_id+message_id_counter)) :: Nil
          headers ::= (CONTENT_TYPE -> ascii(content_type))
          headers ::= (CONTENT_LENGTH -> ascii(body.length().toString))
          headers ::= (DESTINATION -> encode_header(destination_parser.encode_destination(delivery.sender.tail)))
          StompFrame(MESSAGE, headers, BufferContent(body))
        }

        val ack_id = if( (protocol_version eq V1_0) || (protocol_version eq V1_1) ) {
          frame.header(MESSAGE_ID)
        } else {
          val ack_id = checkout_ack_id
          // we need to add the ACK id.
          frame = frame.append_headers((ACK_HEADER->ack_id)::Nil)
          ack_id
        }

        session_manager.delivered(session, delivery.size)
        ack_handler.track(session, ack_id, delivery.size, delivery.ack)

        if( subscription_id != None ) {
          frame = frame.append_headers((SUBSCRIPTION, subscription_id.get)::Nil)
        }
        if( config.add_redeliveries_header!=null && delivery.redeliveries > 0) {
          val header = encode_header(config.add_redeliveries_header)
          val value = ascii(delivery.redeliveries.toString())
          frame = frame.append_headers((header, value)::Nil)
        }
        if( include_seq.isDefined ) {
          frame = frame.append_headers((include_seq.get, ascii(delivery.seq.toString))::Nil)
        }
        messages_sent += 1
        Some(frame)
      }

    }, SessionDeliverySizer)

    def supply_initial_credit = {
      credit_window_filter.credit(initial_credit_window.count, initial_credit_window.size)
    }

    val session_manager:SessionSinkMux[Delivery] = new SessionSinkMux[Delivery](credit_window_filter, dispatchQueue, Delivery, initial_credit_window.count.max(1), buffer_size) {
      override def time_stamp = broker.now
    }

    override def dispose() = defer {
      ack_handler.close
      credit_window_filter.disable
      sink_manager.close(consumer_sink, (frame)=>{
        // No point in sending the frame down to the socket..
      })
      super.dispose()
    }

    def dispatch_queue = StompProtocolHandler.this.dispatchQueue

    override def connection = Some(StompProtocolHandler.this.connection)
    override val receive_buffer_size = buffer_size

    def is_persistent = false

    def match_selector(delivery:Delivery)= selector._2.matches(delivery.message)
    def match_from_seq(delivery:Delivery)= delivery.seq >= from_seq
    def match_from_tail(delivery:Delivery)= delivery.seq >= starting_seq

    val matchers = {
      var l = ListBuffer[(Delivery)=>Boolean]()
      if( from_seq > 0 ) {
        l += match_from_seq
      }
      if( start_from_tail ) {
        l += match_from_tail
      }
      if( selector!=null ) {
        l += match_selector
      }
      l.toArray
    }

    def matches(delivery:Delivery):Boolean = {
      var i=0;
      while( i < matchers.length ) {
        if(!matchers(i)(delivery))
          return false
        i+=1
      }
      true
    }

    class StompConsumerSession(val producer:DeliveryProducer) extends DeliverySession with SessionSinkFilter[Delivery] {
      producer.dispatch_queue.assertExecuting()
      retain

      val downstream = session_manager.open(producer.dispatch_queue)

      override def toString = {
        "stomp consumer session("+
          "connection: "+StompProtocolHandler.this.connection.id+", "+
          "closed: "+closed+", "+
          downstream+
        ")"
      }

      def consumer = StompConsumer.this
      var closed = false

      def close = {
        assert(producer.dispatch_queue.isExecuting)
        if( !closed ) {
          closed = true
          if( browser && close_on_drain ) {
            // Then send the end of browse message.
            val headers:HeaderMap = List(DESTINATION->EMPTY, MESSAGE_ID->EMPTY, BROWSER->END)
            var frame = StompFrame(MESSAGE, headers, BufferContent(EMPTY_BUFFER))

            val delivery = new Delivery()
            var message = StompFrameMessage(frame)
            delivery.message = message
            delivery.size = frame.size
            delivery.expiration = message.expiration
            delivery.persistent = message.persistent

            if( downstream.full ) {
              // session is full so use an overflow sink so to hold the message,
              // and then trigger closing the session once it empties out.
              val sink = new OverflowSink(downstream)
              var disposed = false
              sink.refiller = ^{
                // refiller could get triggered multiple times. only care about the first one.
                if( !disposed ) {
                  disposed = true
                  dispose
                }
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
        session_manager.close(downstream, (delivery)=>{
          // We have been closed so we have to nak any deliveries.
          if( delivery.ack!=null ) {
            delivery.ack(Undelivered, delivery.uow)
          }
        })
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
  var connection_sink:OverflowSink[StompFrame] = null
  var connection_sink_read_suspended = false

  var dead = false
  var closed = false
  var consumers = Map[AsciiBuffer, StompConsumer]()

  var host:VirtualHost = null

  private def queue = connection.dispatch_queue

  // uses by STOMP 1.0 and 1.2 clients
  var connection_ack_handlers = HashMap[AsciiBuffer, StompConsumer#AckHandler]()

  var protocol_version:AsciiBuffer = _

  var heart_beat_monitor = new HeartBeatMonitor
  val security_context = new SecurityContext
  var waiting_on = WAITING_ON_CLIENT_REQUEST
  var config:StompDTO = _

  var protocol_filters = List[ProtocolFilter3]()

  var destination_parser = Stomp.destination_parser
  var protocol_convert = "full"
  var temp_destination_map = HashMap[SimpleAddress, SimpleAddress]()

  var codec:StompCodec = _

  def session_id = security_context.session_id

  def decode_addresses(value:AsciiBuffer):Array[SimpleAddress] = {
    val rc = destination_parser.decode_multi_destination(value.toString)
    if( rc==null ) {
      throw new ProtocolException("Invalid stomp destination name: "+value);
    }
    rc.map { dest =>
      if( dest.domain.startsWith("temp-") ) {
        temp_destination_map.getOrElseUpdate(dest, {
          val parts = LiteralPart("temp") :: LiteralPart(broker.id) :: LiteralPart(session_id) :: dest.path.parts
          SimpleAddress(dest.domain.stripPrefix("temp-"), Path(parts))
        })
      } else {
        dest
      }
    }
  }

  def die_delay = {
    import OptionSupport._
    config.die_delay.getOrElse(DEFAULT_DIE_DELAY)
  }

  lazy val buffer_size = Option(config.buffer_size).map(MemoryPropertyEditor.parse(_).toInt).getOrElse(broker.auto_tuned_send_receiver_buffer_size*10)

  override def set_connection(connection: BrokerConnection) = {
    super.set_connection(connection)
    import OptionSupport._
    import collection.JavaConversions._

    codec = connection.protocol_codec(classOf[StompCodec])
    val connector_config = connection.connector.config.asInstanceOf[AcceptingConnectorDTO]
    config = connector_config.protocols.find( _.isInstanceOf[StompDTO]).map(_.asInstanceOf[StompDTO]).getOrElse(new StompDTO)

    protocol_filters = ProtocolFilter3.create_filters(config.protocol_filters.toList, this)

    Option(config.max_data_length).map(MemoryPropertyEditor.parse(_).toInt).foreach( codec.max_data_length = _ )
    Option(config.max_header_length).map(MemoryPropertyEditor.parse(_).toInt).foreach( codec.max_header_length = _ )
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
      if( config.temp_queue_prefix!=null ) { destination_parser.temp_queue_prefix = config.temp_queue_prefix }
      if( config.temp_topic_prefix!=null ) { destination_parser.temp_topic_prefix = config.temp_topic_prefix }
      if( config.destination_separator!=null ) { destination_parser.destination_separator = config.destination_separator }
      if( config.path_separator!=null ) { destination_parser.path_separator = config.path_separator }
      if( config.any_child_wildcard!=null ) { destination_parser.any_child_wildcard = config.any_child_wildcard }
      if( config.any_descendant_wildcard!=null ) { destination_parser.any_descendant_wildcard = config.any_descendant_wildcard }
      if( config.regex_wildcard_start!=null ) { destination_parser.regex_wildcard_start = config.regex_wildcard_start }
      if( config.regex_wildcard_end!=null ) { destination_parser.regex_wildcard_end = config.regex_wildcard_end }

    }

  }

  var messages_sent = 0L
  var messages_received = 0L

  override def create_connection_status(debug:Boolean) = {
    var rc = new StompConnectionStatusDTO
    rc.protocol_version = if( protocol_version == null ) null else protocol_version.toString
    rc.user = security_context.user
    rc.subscription_count = consumers.size
    rc.waiting_on = waiting_on()
    rc.messages_sent = messages_sent
    rc.messages_received = messages_received
    if( debug ) {
      import collection.JavaConversions._
      val out = new StringBuilder
      out.append("\n--- connection ---\n")
      out.append("  { routing_size:"+routing_size+" }\n")
      out.append("--- producers ---\n")
      for( p <- producer_routes.values() ) {
        out.append("  { "+p+" }\n")
      }
      out.append("--- consumers ---\n")
      for( c <- consumers.values ) {
        out.append("  { "+c+" }\n")
      }
      out.append("--- transactions ---\n")
      for( t <- transactions.values ) {
        out.append("  { "+t+" }\n")
      }
      rc.debug = out.toString()
    }
    rc
  }

  class ProtocolException(msg:String) extends RuntimeException(msg)
  class Break extends RuntimeException

  def async_die(msg:String) = async_die(msg, null)

  private def async_die(msg:String, e:Throwable) = try {
    die(msg, e)
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
      connection_log.info(e, "STOMP connection '%s' error: %s", security_context.remote_address, msg, e)
    } else {
      connection_log.info("STOMP connection '%s' error: %s", security_context.remote_address, msg)
    }
    die((MESSAGE_HEADER, encode_header(msg))::Nil, "")
  }

  private def die[T](headers:HeaderMap, body:String):T = {
    if( !dead ) {
      dead = true
      connection.transport.resumeRead

      if( body.isEmpty ) {
        connection_sink.offer(StompFrame(ERROR, headers, BufferContent(EMPTY_BUFFER)) )
      } else {
        connection_sink.offer(StompFrame(ERROR, (CONTENT_TYPE, TEXT_PLAIN)::headers, BufferContent(utf8(body))) )
      }

      // TODO: if there are too many open connections we should just close the connection
      // without waiting for the error to get sent to the client.
      disconnect(true)
    }
    throw new Break()
  }

  override def on_transport_connected() = {
    connection_log = connection.connector.broker.connection_log

    var filtering_sink:Sink[StompFrame] = connection.transport_sink.map { x=>
      trace("sending frame: %s", x)
      x
    }

    if(!protocol_filters.isEmpty) {
      filtering_sink = new AbstractSinkFilter[StompFrame, StompFrame]() {
        val downstream = filtering_sink
        def filter(value: StompFrame): StompFrame = {
          var cur = value
          for(filter <- protocol_filters) {
            if( cur != null ) {
              cur = filter.filter_outbound(cur)
            }
          }
          cur
        }
      }
    }

    sink_manager = new SinkMux[StompFrame](filtering_sink)
    connection_sink = new OverflowSink(sink_manager.open());
    connection_sink.refiller =  ^ {
      if( connection_sink_read_suspended ) {
        connection_sink_read_suspended = false
        resume_read()
      }
    }
    resume_read
  }

  override def on_transport_failure(error: IOException) = {
    if( !closed ) {
      suspend_read(waiting_on())
      error match {
        case e:StompProtocolException =>
          async_die(e.getMessage)
        case _ =>
          connection_log.info("Shutting connection '%s'  down due to: %s", security_context.remote_address, error)
          disconnect(false)
      }
    }
  }

  override def on_transport_disconnected {
    disconnect(false)
  }

  def disconnect(delay: =>Boolean) = {
    if( !closed ) {
      heart_beat_monitor.stop
      closed=true;
      dead = true;

      import collection.JavaConversions._

      // Rollback any in-progress transactions..
      for( (id, tx) <- transactions ) {
        tx.rollback
      }
      transactions.clear()

      consumers.foreach { case (_,consumer)=>
        val addresses = consumer.addresses
        host.dispatch_queue {
          host.router.unbind(addresses, consumer, false , security_context)
          consumer.release()
        }
      }
      consumers = Map()
      security_context.logout( e => {
        if(e!=null) {
          connection_log.info(e, "STOMP connection '%s' log out error: %s", security_context.remote_address, e)
        }
      })
      trace("stomp protocol resources released")

      waiting_on = ()=> { "Producer delivery competition" }
      on_routing_empty {
        producer_routes.values().foreach{ route=>
          host.dispatch_queue {
            host.router.disconnect(route.addresses, route)
          }
        }
        producer_routes.clear

        if( delay ) {
          waiting_on = ()=>"die delay"
          queue.after(die_delay, TimeUnit.MILLISECONDS) {
            connection.stop(NOOP)
          }
        } else {
          connection.stop(NOOP)
        }
      }
    }
  }


  override def on_transport_command(command:AnyRef):Unit = {
    if( dead ) {
      // We stop processing client commands once we are dead
      return;
    }
    try {
      command match {
        case f:StompFrame=>

          trace("received frame: %s", f)

          val frame = if(!protocol_filters.isEmpty) {
            var cur = f
            for( filter <- protocol_filters) {
              if( cur !=null ) {
                cur = filter.filter_inbound(cur)
              }
            }
            if( cur == null ) {
              return // dropping the frame.
            }
            cur
          } else {
            f
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
                on_stomp_subscribe(deep_copy(frame.headers))
              case UNSUBSCRIBE =>
                on_stomp_unsubscribe(frame.headers)
              case NACK =>
                on_stomp_nack(frame)
              case DISCONNECT =>
                disconnect(send_receipt(frame.headers)!=null)
              case _ =>
                die("Invalid STOMP frame command: "+frame.action);
            }
          }

        case _=>
          die("Internal Server Error: unexpected stomp type");
      }
    }  catch {
      case e: Break =>
      case e:Exception =>
        // To avoid double logging to the same log category..
        var msg: String = "Internal Server Error: " + e
        if( connection_log!=StompProtocolHandler ) {
          // but we also want the error on the apollo.log file.
          warn(e, msg)
        }
        async_die(msg, e);
    }
  }

  def suspend_read(reason: =>String) = {
    waiting_on = reason _
    connection.transport.suspendRead
    heart_beat_monitor.suspendRead
  }
  def resume_read() = {
    waiting_on = WAITING_ON_CLIENT_REQUEST
    connection.transport.resumeRead
    heart_beat_monitor.resumeRead
  }

  def on_stomp_connect(headers:HeaderMap):Unit = {

    security_context.certificates = connection.certificates
    security_context.local_address = connection.transport.getLocalAddress
    security_context.remote_address = connection.transport.getRemoteAddress
    security_context.user = get(headers, LOGIN).map(decode_header _).getOrElse(null)
    security_context.password = get(headers, PASSCODE).map(decode_header _).getOrElse(null)
    security_context.connector_id = connection.connector.id

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
      codec.trim = false
      if( protocol_version != V1_1 ) {
        // enable \r triming
        codec.trim_cr = true
      }
    }

    val heart_beat = get(headers, HEART_BEAT).getOrElse(DEFAULT_HEART_BEAT)
    heart_beat.split(COMMA).map(_.ascii) match {
      case Array(cx,cy) =>
        try {
          val can_send = cx.toString.toLong
          val please_send = cy.toString.toLong

          if( inbound_heartbeat>=0 && can_send > 0 ) {
            heart_beat_monitor.setReadInterval((inbound_heartbeat.max(can_send)*1.5).toLong)

            heart_beat_monitor.setOnDead(^{
              async_die("Stale connection.  Missed heartbeat.")
            });
          }
          if( outbound_heartbeat>=0 && please_send > 0 ) {
            heart_beat_monitor.setWriteInterval(outbound_heartbeat.max(please_send)/2)
            heart_beat_monitor.setOnKeepAlive(^{
              connection.transport.offer(NEWLINE_BUFFER)
            })
          }

          heart_beat_monitor.suspendRead()
          heart_beat_monitor.setTransport(connection.transport)
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
      connected_headers += HOST_ID->encode_header(host.id)
      connected_headers += SESSION->encode_header(session_id)

      val outbound_heart_beat_header = ascii("%d,%d".format(outbound_heartbeat,inbound_heartbeat))
      connected_headers += HEART_BEAT->outbound_heart_beat_header

      if( host.authenticator!=null ) {
        host.authenticator.user_name(security_context).foreach{ name=>
          connected_headers += USER_ID->encode_header(name)
        }
      }

      connection_sink.offer(StompFrame(CONNECTED,connected_headers.toList))
//      codec.direct_buffer_allocator = this.host.direct_buffer_allocator
    }

    suspend_read("virtual host lookup")
    val host_header = get(headers, HOST)

    broker.dispatch_queue {
      val host = host_header match {
        case None=> broker.default_virtual_host
        case Some(host)=> broker.get_virtual_host(host)
      }
      defer {
        resume_read
        if(host==null) {
          async_die("Invalid virtual host: "+host_header.get)
        } else if(!host.service_state.is_started) {
          var headers = (MESSAGE_HEADER, encode_header("Virtual host stopped")) :: Nil
          host.client_redirect.foreach(x=> headers ::= REDIRECT_HEADER->encode_header(x) )
          async_die(headers, "")
        } else {
          this.host=host
          security_context.session_id = "%s-%x".format(this.host.config.id, this.host.session_counter.incrementAndGet)
          connection_log = host.connection_log
          if( host.authenticator!=null &&  host.authorizer!=null ) {
            suspend_read("authenticating and authorizing connect")
            host.authenticator.authenticate(security_context) { auth_failure=>
              defer {
                if( auth_failure!=null ) {
                  async_die("%s. Credentials=%s".format(auth_failure, security_context.credential_dump))
                } else if( !host.authorizer.can(security_context, "connect", connection.connector) ) {
                  async_die("Not authorized to connect to connector '%s'. Principals=%s".format(connection.connector.id, security_context.principal_dump))
                } else if( !host.authorizer.can(security_context, "connect", this.host) ) {
                  async_die("Not authorized to connect to virtual host '%s'. Principals=%s".format(this.host.id, security_context.principal_dump))
                } else {
                  resume_read
                  send_connected
                }
              }
            }
          } else {
            send_connected
          }
        }
      }
    }
  }

  def on_stomp_send(frame:StompFrame) = {
    messages_received += 1

    get(frame.headers, DESTINATION) match {
      case None=>
        frame.release
        die("destination not set.")

      case Some(dest)=>

        get(frame.headers, TRANSACTION) match {
          case None=>
            perform_send(frame)
          case Some(txid)=>
            get_or_create_tx_queue(txid).add (new TransactionAction(){
              override def on_commit(uow: StoreUOW) {
                perform_send(frame, uow)
              }
              override def toString: String = {
                "send to: "+dest
              }
            })
        }

    }
  }

  var routing_size = 0L
  var pending_routing_empty_callbacks = ListBuffer[()=>Unit]()
  def on_routing_empty(func: => Unit) = {
    if( routing_size== 0 ) {
      func
    } else {
      pending_routing_empty_callbacks.append( func _ )
    }
  }

  class StompProducerRoute(val dest: AsciiBuffer) extends DeliveryProducerRoute(host.router) {
    val addresses = decode_addresses(dest)
    val key = addresses.toList

    override def send_buffer_size = buffer_size

    override def connection = Some(StompProtocolHandler.this.connection)

    override def dispatch_queue = queue
    var suspended = false

    refiller = ^ {
      if( suspended ) {
        resume_read
        suspended = false
      }
    }

    var routing_items = 0

    override def offer(delivery: Delivery): Boolean = {
      if( full )
        return false
      routing_size += delivery.size
      routing_items += 1
      val original_ack = delivery.ack
      delivery.ack = (result, uow) => {
        dispatch_queue.assertExecuting()
        if ( original_ack!=null ) {
          original_ack(result, uow)
        }
        routing_items -= 1
        routing_size -= delivery.size
        if( routing_size==0 && !pending_routing_empty_callbacks.isEmpty) {
          val t = pending_routing_empty_callbacks
          pending_routing_empty_callbacks = ListBuffer()
          for ( func <- t ) {
            func()
          }
        }
      }
      super.offer(delivery)
    }

    override def toString = {
      "addresses:"+key+", routing_items:"+routing_items+", "+super.toString
    }
  }


  var maintenance_scheduled = false
  def schedule_maintenance:Unit = {
    if(!maintenance_scheduled && !producer_routes.isEmpty) {
      maintenance_scheduled = true
      dispatchQueue.after(2, TimeUnit.SECONDS) {
        maintenance_scheduled = false
        if( !producer_routes.isEmpty ) {
          try {
            producer_maintenance
          } finally {
            schedule_maintenance
          }
        }
      }
    }
  }

  def producer_maintenance = defer {
    val now = Broker.now
    import collection.JavaConversions._
    val expired = ListBuffer[StompProducerRoute]()
    for( route <- producer_routes.values() ) {
      if( (now - route.last_send) > 2000 && route.routing_items==0 ) {
        expired += route
      }
    }
    for( route <- expired ) {
      producer_routes.remove(route.dest)
      host.dispatch_queue {
        host.router.disconnect(route.addresses, route)
      }
    }
  }

  var producer_routes = new java.util.HashMap[AsciiBuffer, StompProducerRoute]()

  def perform_send(frame:StompFrame, uow:StoreUOW=null): Unit = {
    val dest = get(frame.headers, DESTINATION).get
    producer_routes.get(dest) match {
      case null =>
        // Deep copy to avoid holding onto a 64k buffer
        val trimmed_dest = dest.deepCopy().ascii()
        // create the producer route...
        val route = new StompProducerRoute(trimmed_dest)   // don't process frames until producer is connected...
        suspend_read("Connecting to destination")
        if( uow !=null ) {
          uow.retain
        }
        host.dispatch_queue {
          val rc = host.router.connect(route.addresses, route, security_context)
          defer {
            rc match {
              case Some(failure) =>
                async_die(failure)
              case None =>
                if (!connection.stopped) {
                  resume_read
                  producer_routes.put(trimmed_dest, route)
                  schedule_maintenance
                  send_via_route(route.addresses, route, frame, uow)
                }
            }
            if( uow !=null ) {
              uow.release
            }
          }
        }

      case route =>
        // we can re-use the existing producer route
        send_via_route(route.addresses, route, frame, uow)

    }
  }

  var message_id_counter = 0L

  def encode_address(value: Array[_ <: DestinationAddress]): String = {
    destination_parser.encode_destination(value)
//    if (value == null) {
//      null
//    } else {
//      val rc = new StringBuilder
//      value.foreach { dest =>
//        if (rc.length != 0 ) {
//          assert( destination_parser.destination_separator!=null )
//          rc.append(destination_parser.destination_separator)
//        }
//        import collection.JavaConversions._
//        dest match {
//          case d:QueueDestinationDTO =>
//            rc.append(destination_parser.queue_prefix)
//            rc.append(destination_parser.encode_path_iter(dest.path.toIterable, false))
//          case d:DurableSubscriptionDestinationDTO =>
//            rc.append(destination_parser.dsub_prefix)
//            rc.append(destination_parser.unsanitize_destination_part(d.subscription_id))
//          case d:TopicDestinationDTO =>
//            rc.append(destination_parser.topic_prefix)
//            rc.append(destination_parser.encode_path_iter(dest.path.toIterable, false))
//          case _ =>
//            throw new Exception("Uknown destination type: "+dest.getClass);
//        }
//      }
//      rc.toString
//    }
  }

  def updated_headers(addresses: Array[SimpleAddress], headers:HeaderMap) = {
    var rc:HeaderMap=Nil

    // Do we need to re-write the destination names?
    if( addresses.find(_.id.startsWith("temp.")).isDefined ) {
      rc ::= (DESTINATION -> encode_header(encode_address(addresses)))
    }
    get(headers, REPLY_TO).foreach { value=>
      // we may need to translate local temp destination names to broker destination names
      if( value.indexOf(TEMP_QUEUE)>=0 || value.indexOf(TEMP_TOPIC)>=0 ) {
        try {
          val dests = decode_addresses(value)
          if (dests.find(_.id.startsWith("temp.")).isDefined) {
            rc ::= (REPLY_TO -> encode_header(encode_address(dests)))
          }
        } catch {
          case _:Throwable=> // the translation is a best effort thing.
        }
      }
    }

    // Do we need to add an expires header?
    for( ttl <- get( headers, TTL) ) {
      if( get( headers, EXPIRES)==None ) {
        val expiration = Broker.now + java.lang.Long.parseLong(ttl.toString)
        rc ::= (EXPIRES -> ascii(expiration.toString))
      }
    }

    // Do we need to add the message id?
    if( get( headers, MESSAGE_ID) == None ) {
      message_id_counter += 1
      rc ::= (MESSAGE_ID -> ascii(session_id+message_id_counter))
    }

    if( config.add_timestamp_header!=null ) {
      rc ::= (encode_header(config.add_timestamp_header), ascii(broker.now.toString()))
    }

    // Do we need to add the user id?
    if( host.authenticator!=null ) {
      if( config.add_user_header!=null ) {
        host.authenticator.user_name(security_context).foreach{ name=>
          val value = host.authenticator.user_name(security_context).getOrElse("")
          rc ::= (encode_header(config.add_user_header), encode_header(value))
        }
      }
      if( !config.add_user_headers.isEmpty ){
        import collection.JavaConversions._
        config.add_user_headers.foreach { h =>
          val matches = security_context.principals(Option(h.kind).getOrElse("*"))
          val value = if( !matches.isEmpty ) {
            h.separator match {
              case null=>
                matches.head.getName
              case separator =>
                matches.map(_.getName).mkString(separator)
            }
          } else {
            ""
          }
          rc ::= (encode_header(h.name.trim), encode_header(value))
        }
      }
    }

    rc
  }

  def send_via_route(addresses: Array[SimpleAddress], route:StompProducerRoute, frame:StompFrame, uow:StoreUOW) = {
    var storeBatch:StoreUOW=null

    // User might be asking for ack that we have processed the message..
    val receipt = frame.header(RECEIPT_REQUESTED)

    // We may need to add some headers..
    var message = updated_headers(addresses, frame.headers) match {
      case Nil=>
        StompFrameMessage(StompFrame(MESSAGE, frame.headers, frame.content, frame.contiguous))
      case updated_headers =>
        StompFrameMessage(StompFrame(MESSAGE, frame.headers, frame.content, frame.contiguous, updated_headers))
    }

    val delivery = new Delivery
    delivery.message = message
    delivery.expiration = message.expiration
    delivery.persistent = message.persistent
    delivery.size = message.frame.size
    delivery.uow = uow
    get(frame.headers, RETAIN).foreach { retain =>
      delivery.retain = retain match {
        case SET => RetainSet
        case REMOVE => RetainRemove
        case _ => RetainIgnore
      }
    }

    if( receipt!=null ) {
      val trimmed_receipt = receipt.deepCopy().ascii()
      delivery.ack = { (consumed, uow) =>
        defer {
          send_receipt(trimmed_receipt)
        }
      }
    }

    route.offer(delivery)
    if( route.full && !route.suspended ) {
      // but once it gets full.. suspend, so that we get more stomp messages
      // until it's not full anymore.
      route.suspended = true
      suspend_read("blocked sending to: "+route.addresses.mkString(", "))
    }
    frame.release
  }

  def deep_copy(headers:HeaderMap) = {
    headers.map { header=>
      (header._1.deepCopy().ascii(), header._2.deepCopy().ascii())
    }
  }

  def on_stomp_subscribe(headers:HeaderMap):Unit = {

    val dest = get(headers, DESTINATION).getOrElse(die("destination not set."))
    var addresses:Array[_ <: BindAddress] = decode_addresses(dest)

    val subscription_id = get(headers, ID)
    var id:AsciiBuffer = subscription_id.getOrElse {
      if( protocol_version eq V1_0 ) {
          // in 1.0 it's ok if the client does not send us the
          // the id header
          dest.deepCopy().ascii()
        } else {
          die("The id header is missing from the SUBSCRIBE frame");
        }

    }

//    val topic = destination.isInstanceOf[TopicDestinationDTO]
    var persistent = get(headers, PERSISTENT).map( _ == TRUE ).getOrElse(false)
    var browser = get(headers, BROWSER).map( _ == TRUE ).getOrElse(false)
    var browser_end = browser && get(headers, BROWSER_END).map( _ == TRUE ).getOrElse(true)
    var exclusive = !browser && get(headers, EXCLUSIVE).map( _ == TRUE ).getOrElse(false)
    var include_seq = get(headers, INCLUDE_SEQ)
    val from_seq_opt = get(headers, FROM_SEQ)


    def is_multi_destination = if( addresses.length > 1 ) {
      true
    } else {
      PathParser.containsWildCards(addresses(0).path)
    }
    if( from_seq_opt.isDefined && is_multi_destination ) {
      die("The from-seq header is only supported when you subscribe to one destination");
    }

    val ack_mode = get(headers, ACK_HEADER).getOrElse(ACK_MODE_AUTO)
    val credit_window = get(headers, CREDIT) match {
      case Some(value) =>
        value.toString.split(",").toList match {
          case x :: Nil =>
            InitialCreditWindow(x.toInt, buffer_size, true)
          case x :: y :: Nil =>
            InitialCreditWindow(x.toInt, y.toInt, true)
          case x :: y :: z :: _ =>
            InitialCreditWindow(x.toInt, y.toInt, z.toBoolean)
          case _ =>
            InitialCreditWindow(buffer_size, buffer_size, true)
        }
      case None =>
        InitialCreditWindow(buffer_size, buffer_size, true)
    }

    val selector = get(headers, SELECTOR) match {
      case None=> null
      case Some(x)=>
        try {
          val s = "convert_string_expressions:hyphenated_props:"+decode_header(x)
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
      val dsubs = ListBuffer[BindAddress]()
      val topics = ListBuffer[BindAddress]()
      addresses.foreach { address =>
        address.domain match {
          case "dsub" => dsubs += address
          case "topic" => topics += address
          case _ => die("A persistent subscription can only be used on a topic destination")
        }
      }
      val s = if (selector == null) null else selector._1
      dsubs += SubscriptionAddress(destination_parser.decode_path(decode_header(id)), s, topics.toArray)
      addresses = dsubs.toArray
    }

    val from_seq = from_seq_opt.map(_.toString.toLong).getOrElse(0L)
    val consumer = new StompConsumer(subscription_id, addresses, ack_mode, selector, browser, exclusive, credit_window, include_seq, from_seq, browser_end);
    consumers += (id -> consumer)

    host.dispatch_queue {
      host.router.bind(addresses, consumer, security_context) { rc =>
        defer {
          rc match {
            case Some(reason)=>
              consumers -= id
              consumer.release
              async_die(reason)
            case None =>
              send_receipt(headers)
              consumer.supply_initial_credit
          }
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
        if( persistent ) {
          // We just want to delete a durable sub but client has not connected
          // to it yet in this session
          host.dispatch_queue {
            var addresses = Array[DestinationAddress](SubscriptionAddress(destination_parser.decode_path(decode_header(id)), null, Array[BindAddress]()))
            host.router.delete(addresses, security_context) match {
              case Some(error)=>
                defer {
                  async_die(error)
                }
              case None =>
                defer {
                  send_receipt(headers)
                }
            }
          }
        } else {
          die("The subscription '%s' not found.".format(id))
        }

      case Some(consumer)=>
        // consumer gets disposed after all producer stop sending to it...
        consumers -= id
        host.dispatch_queue {
          host.router.unbind(consumer.addresses, consumer, persistent, security_context)
          consumer.release()
          defer {
            send_receipt(headers)
          }
        }
    }
  }

  def on_stomp_ack(frame:StompFrame):Unit = {
    on_stomp_ack(frame.headers, Consumed)
  }

  def on_stomp_nack(frame:StompFrame):Unit = {
    on_stomp_ack(frame.headers, Poisoned)
  }

  def on_stomp_ack(headers:HeaderMap, consumed:DeliveryResult):Unit = {
    val credit = get(headers, CREDIT) match {
      case None => null
      case Some(value) =>
        value.toString.split(",").toList match {
          case x :: Nil =>
            (x.toInt, 0)
          case x :: y :: _ =>
            (x.toInt, y.toInt)
          case _ => (0,0)
        }

    }

    val (messageId,handler) = if( (protocol_version eq V1_0) || (protocol_version eq V1_1) ) {
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

      (messageId,handler)
    } else {
      val id = get(headers, ID).getOrElse(null)
      if( credit==null && id==null) {
        die("id header not set")
      }
      val handler = connection_ack_handlers.get(id).orElse(die("Not expecting an ACK/NACK for id '%s'".format(id)))
      (id,handler)
    }

    handler.foreach{ handler=>
      handler.credit(messageId, credit)
      if( messageId!=null ) {
        get(headers, TRANSACTION) match {
          case None=>
            handler.perform_ack(consumed, messageId, null)
          case Some(txid)=>
            handler.consumer.retain()
            get_or_create_tx_queue(txid).add (new TransactionAction(){
              override def on_commit(uow: StoreUOW) {
                handler.perform_ack(consumed, messageId, uow)
                handler.consumer.release()
              }
              override def on_rollback() {
                handler.consumer.release()
              }
              override def toString: String = {
                "ack: "+messageId
              }
            })
        }
      }
      send_receipt(headers)
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
    val txid = require_transaction_header(headers)
    val tx = transactions.get(txid).getOrElse(die("transaction not active: %d".format(txid)))
    tx.commit {
      queue {
        remove_tx_queue(txid)
        send_receipt(headers)
      }
    }
  }

  def on_stomp_abort(headers:HeaderMap) = {
    remove_tx_queue(require_transaction_header(headers)).rollback
    send_receipt(headers)
  }

  def send_receipt(headers:HeaderMap):StompFrame = {
    get(headers, RECEIPT_REQUESTED) match {
      case Some(receipt)=>
        send_receipt(receipt)
      case None=>
        null
    }
  }

  def send_receipt(receipt:AsciiBuffer):StompFrame = {
    dispatchQueue.assertExecuting()
    val frame = StompFrame(RECEIPT, List((RECEIPT_ID, receipt)))
    connection_sink.offer(frame)
    if( connection_sink.overflow.size() > 1000 && !connection_sink_read_suspended) {
      connection_sink_read_suspended = true
      suspend_read("client to drain receipts")
    }
    frame
  }

  class TransactionAction {
    def on_commit(uow:StoreUOW):Unit = {}
    def on_rollback() = {}
  }

  class TransactionQueue {
    // TODO: eventually we want to back this /w a broker Queue which
    // can provides persistence and memory swapping.

    val queue = ListBuffer[TransactionAction]()
    var uow:StoreUOW = _

    override def toString: String = {
      "{ uow: "+uow+", actions: "+queue+" }"
    }

    def add(action:TransactionAction):Unit = {
      queue += action
    }

    def commit(on_complete: => Unit) = {
      if( host.store!=null ) {
        uow = host.store.create_uow
//        println("UOW starting: "+uow.asInstanceOf[DelayingStoreSupport#DelayableUOW].uow_id)
        uow.on_complete {
//          println("UOW completed: "+uow.asInstanceOf[DelayingStoreSupport#DelayableUOW].uow_id)
          defer {
            on_complete
          }
        }
        queue.foreach{ _.on_commit(uow) }
        uow.release
      } else {
        queue.foreach{ _.on_commit(null) }
        on_complete
      }
    }

    def rollback = {
      queue.foreach{ _.on_rollback() }
    }

  }

  val transactions = HashMap[AsciiBuffer, TransactionQueue]()

  def create_tx_queue(txid:AsciiBuffer):TransactionQueue = {
    if ( transactions.contains(txid) ) {
      die("transaction already started")
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
    transactions.remove(txid).getOrElse(die("transaction not active: %s".format(txid)))
  }

}

