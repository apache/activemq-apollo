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
import StompFrameConstants._
import java.io.IOException
import org.apache.activemq.apollo.selector.SelectorParser
import org.apache.activemq.apollo.filter.{BooleanExpression, FilterException}
import org.apache.activemq.apollo.transport._
import org.apache.activemq.apollo.store._
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.dto.{BindingDTO, DurableSubscriptionBindingDTO, PointToPointBindingDTO}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object StompConstants {

  val PROTOCOL = "stomp"
  val DURABLE_PREFIX = ascii("durable:")
  val DURABLE_QUEUE_KIND = ascii("stomp:sub")

  val options = new ParserOptions
  options.queuePrefix = ascii("/queue/")
  options.topicPrefix = ascii("/topic/")

  options.defaultDomain = Router.QUEUE_DOMAIN

  implicit def toDestination(value:AsciiBuffer):Destination = {
    val d = DestinationParser.parse(value, options)
    if( d==null ) {
      throw new ProtocolException("Invalid stomp destiantion name: "+value);
    }
    d
  }

}
/**
 * Creates StompCodec objects that encode/decode the
 * <a href="http://activemq.apache.org/stomp/">Stomp</a> protocol.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class StompProtocolCodecFactory extends ProtocolCodecFactory.Provider {
  import Stomp.Commands.CONNECT
  import Stomp.Commands.STOMP

  def protocol = StompConstants.PROTOCOL

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

import StompConstants._

object StompProtocolHandler extends Log

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class StompProtocolHandler extends ProtocolHandler with DispatchLogging {
  
  def protocol = "stomp"

  override protected def log = StompProtocolHandler
  
  protected def dispatchQueue:DispatchQueue = connection.dispatchQueue

  class StompConsumer(val destination:Destination, val ackMode:AsciiBuffer, val selector:(AsciiBuffer, BooleanExpression), val binding:BindingDTO) extends BaseRetained with DeliveryConsumer {
    val dispatchQueue = StompProtocolHandler.this.dispatchQueue

    dispatchQueue.retain
    setDisposer(^{
      session_manager.release
      dispatchQueue.release
    })

    override def connection = Some(StompProtocolHandler.this.connection) 

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
          if( delivery.ack!=null) {
            if( ackMode eq Headers.Subscribe.AckModeValues.AUTO ) {
              delivery.ack(null)
            } else {
              // switch the the queue context.. this method is in the producer's context.
              queue {
                // we need to correlate acks from the client.. to invoke the
                // delivery ack.
                pendingAcks += ( delivery.message.id->delivery.ack )
              }
            }
          }
          val frame = delivery.message.asInstanceOf[StompFrameMessage].frame
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

  var producerRoutes = Map[Destination, DeliveryProducerRoute]()
  var host:VirtualHost = null

  private def queue = connection.dispatchQueue
  var pendingAcks = HashMap[AsciiBuffer, (StoreUOW)=>Unit]()

  override def onTransportConnected() = {

    session_manager = new SinkMux[StompFrame]( MapSink(connection.transportSink){x=>x}, dispatchQueue, StompFrame)
    connection_sink = new OverflowSink(session_manager.open(dispatchQueue));
    connection_sink.refiller = ^{}
    
    connection.connector.broker.getDefaultVirtualHost(
      queue.wrap { (host)=>
        this.host=host
        if( this.host.direct_buffer_pool!=null ) {
          val wf = connection.transport.getProtocolCodec.asInstanceOf[StompCodec]
          wf.memory_pool = this.host.direct_buffer_pool
        }
        connection.transport.resumeRead
      }
    )
  }

  override def onTransportDisconnected() = {
    if( !closed ) {
      closed=true;
      producerRoutes.foreach{
        case(_,route)=> host.router.disconnect(route)
      }
      producerRoutes = Map()
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
        case StompFrame(Commands.SEND, headers, content, _) =>
          on_stomp_send(command.asInstanceOf[StompFrame])
        case StompFrame(Commands.ACK, headers, content, _) =>
          on_stomp_ack(headers)
        case StompFrame(Commands.SUBSCRIBE, headers, content, _) =>
          info("got command: %s", command)
          on_stomp_subscribe(headers)
        case StompFrame(Commands.CONNECT, headers, _, _) =>
          info("got command: %s", command)
          on_stomp_connect(headers)
        case StompFrame(Commands.DISCONNECT, headers, content, _t) =>
          info("got command: %s", command)
          connection.stop
        case s:StompCodec =>
          // this is passed on to us by the protocol discriminator
          // so we know which wire format is being used.
        case StompFrame(unknown, _, _, _) =>
          die("Unsupported STOMP command: "+unknown);
        case _ =>
          die("Unsupported command: "+command);
      }
    }  catch {
      case e:Exception =>
        die("Unexpected error: "+e);  
    }
  }


  def on_stomp_connect(headers:HeaderMap) = {
    connection_sink.offer(StompFrame(Responses.CONNECTED))
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
    get(frame.headers, Headers.Send.DESTINATION) match {
      case Some(dest)=>
        val destiantion:Destination = dest

        producerRoutes.get(destiantion) match {
          case None=>
            // create the producer route...

            val producer = new DeliveryProducer() {

              override def connection = Some( StompProtocolHandler.this.connection )

              override def dispatchQueue = queue
            }

            // don't process frames until producer is connected...
            connection.transport.suspendRead
            host.router.connect(destiantion, producer) { route =>
                if( !connection.stopped ) {
                  connection.transport.resumeRead
                  route.refiller = ^{
                    connection.transport.resumeRead
                  }
                  producerRoutes += destiantion->route
                  send_via_route(route, frame)
                }
            }

          case Some(route)=>
            // we can re-use the existing producer route
            send_via_route(route, frame)

        }

      case None=>
        frame.release
        die("destination not set.")
    }
  }

  var message_id_counter = 0;
  def next_message_id = {
    message_id_counter += 1
    // TODO: properly generate mesage ids
    new AsciiBuffer("msg:"+message_id_counter);
  }

  def send_via_route(route:DeliveryProducerRoute, frame:StompFrame) = {
    var storeBatch:StoreUOW=null
    // User might be asking for ack that we have prcoessed the message..
    val receipt = frame.header(Stomp.Headers.RECEIPT_REQUESTED)

    if( !route.targets.isEmpty ) {

      // We may need to add some headers..
      var message = get( frame.headers, Stomp.Headers.Message.MESSAGE_ID) match {
        case None=>
          var updated_headers:HeaderMap=Nil;
          updated_headers ::= (Stomp.Headers.Message.MESSAGE_ID, next_message_id)
          StompFrameMessage(StompFrame(Stomp.Responses.MESSAGE, frame.headers, frame.content, updated_headers))
        case Some(id)=>
          StompFrameMessage(StompFrame(Stomp.Responses.MESSAGE, frame.headers, frame.content))
      }

      val delivery = new Delivery
      delivery.message = message
      delivery.size = message.frame.size

      if( receipt!=null ) {
        delivery.ack = { storeTx =>
          connection_sink.offer(StompFrame(Responses.RECEIPT, List((Stomp.Headers.Response.RECEIPT_ID, receipt))))
        }
      }

      // routes can always accept at least 1 delivery...
      assert( !route.full )
      route.offer(delivery)
      if( route.full ) {
        // but once it gets full.. suspend, so that we get more stomp messages
        // until it's not full anymore.
        connection.transport.suspendRead
      }

    } else {
      // info("Dropping message.  No consumers interested in message.")
      if( receipt!=null ) {
        connection_sink.offer(StompFrame(Responses.RECEIPT, List((Stomp.Headers.Response.RECEIPT_ID, receipt))))
      }
    }
    frame.release
  }

  def on_stomp_subscribe(headers:HeaderMap) = {
    get(headers, Headers.Subscribe.DESTINATION) match {
      case Some(dest)=>

        val destination:Destination = dest

        var id:AsciiBuffer = get(headers, Headers.Subscribe.ID) match {
          case None => dest
          case Some(x:AsciiBuffer)=> x
        }

        val topic = destination.getDomain == Router.TOPIC_DOMAIN

        var durable_name = if( topic && id.startsWith(DURABLE_PREFIX) ) {
          id
        } else {
          null
        }

        val ack:AsciiBuffer = get(headers, Headers.Subscribe.ACK_MODE) match {
          case None=> Headers.Subscribe.AckModeValues.AUTO
          case Some(x)=> x match {
            case Headers.Subscribe.AckModeValues.AUTO=> Headers.Subscribe.AckModeValues.AUTO
            case Headers.Subscribe.AckModeValues.CLIENT=> Headers.Subscribe.AckModeValues.CLIENT
            case ack:AsciiBuffer => die("Unsuported ack mode: "+ack); null
          }
        }

        val selector = get(headers, Headers.Subscribe.SELECTOR) match {
          case None=> null
          case Some(x)=> x
            try {
              (x, SelectorParser.parse(x.utf8.toString))
            } catch {
              case e:FilterException =>
                die("Invalid selector expression: "+e.getMessage)
              null
            }
        }

        consumers.get(id) match {
          case None=>
            info("subscribing to: %s", destination)

            val binding: BindingDTO = if( topic && durable_name==null ) {
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
                rc.subscription_id = durable_name
                rc.filter = if (selector == null) null else selector._1
                rc
              } else {
                val rc = new PointToPointBindingDTO
                rc.destination = destination.getName.toString
                rc
              }
            }

            val consumer = new StompConsumer(destination, ack, selector, binding);
            consumers += (id -> consumer)

            if( binding==null ) {

              // consumer is bind bound as a topic
              host.router.bind(destination, consumer)
              consumer.release

            } else {

              // create a queue and bind the consumer to it.
              host.router.create_queue(binding) { x=>
                x match {
                  case Some(queue:Queue) =>
                    queue.bind(consumer::Nil)
                    consumer.release
                  case None => throw new RuntimeException("case not yet implemented.")
                }
              }
            }



          case Some(_)=>
            die("A subscription with identified with '"+id+"' allready exists")
        }
      case None=>
        die("destination not set.")
    }

  }

  def on_stomp_ack(headers:HeaderMap) = {
    get(headers, Headers.Ack.MESSAGE_ID) match {
      case Some(messageId)=>
        pendingAcks.remove(messageId) match {
          case Some(ack) =>
            ack(null)
          case None =>
            // This can easily happen if the consumer is doing client acks on something like
            // a non-durable topic.
            // trace("The specified message id is not waiting for a client ack: %s", messageId)
        }
      case None=> die("message id header not set")
    }
  }


  private def die(msg:String) = {
    if( !connection.stopped ) {
      info("Shutting connection down due to: "+msg)
      connection.transport.suspendRead
      connection.transport.offer(StompFrame(Responses.ERROR, Nil, BufferContent(ascii(msg))) )
      ^ {
        connection.stop()
      } >>: queue
    }
  }

  override def onTransportFailure(error: IOException) = {
    if( !connection.stopped ) {
      connection.transport.suspendRead
      info(error, "Shutting connection down due to: %s", error)
      super.onTransportFailure(error);
    }
  }
}

