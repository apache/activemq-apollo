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

import _root_.org.apache.activemq.wireformat.{WireFormat}
import _root_.org.fusesource.hawtdispatch.{DispatchQueue, BaseRetained}
import _root_.org.fusesource.hawtbuf._
import collection.mutable.{ListBuffer, HashMap}
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._

import AsciiBuffer._
import org.apache.activemq.apollo.broker._
import Stomp._
import BufferConversions._
import StompFrameConstants._
import java.io.IOException


object StompConstants {

  val options = new ParserOptions
  options.queuePrefix = new AsciiBuffer("/queue/")
  options.topicPrefix = new AsciiBuffer("/topic/")
  options.defaultDomain = Domain.QUEUE_DOMAIN

  implicit def toDestination(value:AsciiBuffer):Destination = {
    val d = DestinationParser.parse(value, options)
    if( d==null ) {
      throw new ProtocolException("Invalid stomp destiantion name: "+value);
    }
    d
  }

}

import StompConstants._

object StompProtocolHandler extends Log

class StompProtocolHandler extends ProtocolHandler with DispatchLogging {

  override protected def log = StompProtocolHandler
  
  protected def dispatchQueue:DispatchQueue = connection.dispatchQueue

  class SimpleConsumer(val destination:Destination) extends BaseRetained with DeliveryConsumer {

    val queue = StompProtocolHandler.this.dispatchQueue
    val session_manager = new DeliverySessionManager(outboundChannel, queue)

    queue.retain
    setDisposer(^{
      session_manager.release
      queue.release
    })

    def matches(message:Delivery) = true

    def open_session(producer_queue:DispatchQueue) = new DeliverySession {
      val session = session_manager.session(producer_queue)

      val consumer = SimpleConsumer.this
      retain

      def deliver(delivery:Delivery) =  {
//        info("Delivering to consumer session")
        session.send(delivery)
      }

      def close = {
        session.close
        release
      }
    }
  }

  var outboundChannel:TransportDeliverySink = null
  var closed = false
  var consumer:SimpleConsumer = null

  var producerRoute:DeliveryProducerRoute=null
  var host:VirtualHost = null

  private def queue = connection.dispatchQueue

  override def onTransportConnected() = {
    outboundChannel = new TransportDeliverySink(connection.transport) {
      override def send(delivery: Delivery) = {
        if( !connection.stopped ) {
          transport.oneway(delivery.message.asInstanceOf[StompFrameMessage].frame, delivery)
        }
      }
    }
    connection.connector.broker.getDefaultVirtualHost(
      queue.wrap { (host)=>
        this.host=host
        connection.transport.resumeRead
      }
    )
  }


  override def onTransportDisconnected() = {
    if( !closed ) {
      closed=true;
      if( producerRoute!=null ) {
        host.router.disconnect(producerRoute)
        producerRoute=null
      }
      if( consumer!=null ) {
        host.router.unbind(consumer.destination, consumer::Nil)
        consumer=null
      }
      trace("stomp protocol resources released")
    }
  }


  def onTransportCommand(command:Any) = {
    try {
      command match {
        case StompFrame(Commands.SEND, headers, content, _) =>
          on_stomp_send(command.asInstanceOf[StompFrame])
        case StompFrame(Commands.ACK, headers, content, _) =>
          // TODO:
        case StompFrame(Commands.SUBSCRIBE, headers, content, _) =>
          info("got command: %s", command)
          on_stomp_subscribe(headers)
        case StompFrame(Commands.CONNECT, headers, _, _) =>
          info("got command: %s", command)
          on_stomp_connect(headers)
        case StompFrame(Commands.DISCONNECT, headers, content, _t) =>
          info("got command: %s", command)
          connection.stop
        case s:StompWireFormat =>
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
    connection.transport.oneway(StompFrame(Responses.CONNECTED), null)
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
        // create the producer route...
        if( producerRoute==null || producerRoute.destination!=destiantion ) {

          // clean up the previous producer..
          if( producerRoute!=null ) {
            host.router.disconnect(producerRoute)
            producerRoute=null
          }

          val producer = new DeliveryProducer() {
            override def collocate(value:DispatchQueue):Unit = ^{
//              TODO:
//              if( value.getTargetQueue ne queue.getTargetQueue ) {
//                println("sender on "+queue.getLabel+" co-locating with: "+value.getLabel);
//                queue.setTargetQueue(value.getTargetQueue)
//                write_source.setTargetQueue(queue);
//                read_source.setTargetQueue(queue)
//              }

            } >>: queue
          }

          // don't process frames until we are connected..
          connection.transport.suspendRead
          host.router.connect(destiantion, queue, producer) {
            (route) =>
              if( !connection.stopped ) {
                connection.transport.resumeRead
                producerRoute = route
                send_via_route(producerRoute, frame)
              }
          }
        } else {
          // we can re-use the existing producer route
          send_via_route(producerRoute, frame)
        }
      case None=>
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
    if( !route.targets.isEmpty ) {

      // We may need to add some headers..
      var message = if( frame.header(Stomp.Headers.Message.MESSAGE_ID)==null ) {
        var updated_headers:HeaderMap=Nil;
        updated_headers ::= (Stomp.Headers.Message.MESSAGE_ID, next_message_id)
        StompFrameMessage(StompFrame(Stomp.Responses.MESSAGE, frame.headers, frame.content, updated_headers))
      } else {
        StompFrameMessage(StompFrame(Stomp.Responses.MESSAGE, frame.headers, frame.content))
      }
      
      val delivery = Delivery(message, message.frame.size)
      connection.transport.suspendRead
      delivery.setDisposer(^{
        connection.transport.resumeRead
      })
      route.targets.foreach(consumer=>{
        consumer.deliver(delivery)
      })
      delivery.release;
    } else {
      // info("Dropping message.  No consumers interested in message.")
    }
  }

  def on_stomp_subscribe(headers:HeaderMap) = {
    get(headers, Headers.Subscribe.DESTINATION) match {
      case Some(dest)=>
        val destiantion:Destination = dest
        if( consumer !=null ) {
          die("Only one subscription supported.")

        } else {
          info("subscribing to: %s", destiantion)
          consumer = new SimpleConsumer(destiantion);
          host.router.bind(destiantion, consumer :: Nil)
          consumer.release
        }
      case None=>
        die("destination not set.")
    }

  }

  private def die(msg:String) = {
    if( !connection.stopped ) {
      info("Shutting connection down due to: "+msg)
      connection.transport.suspendRead
      connection.transport.oneway(StompFrame(Responses.ERROR, Nil, ascii(msg)), null)
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

