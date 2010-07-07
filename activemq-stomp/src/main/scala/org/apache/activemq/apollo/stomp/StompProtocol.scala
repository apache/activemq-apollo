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

import _root_.org.apache.activemq.apollo.broker._

import _root_.org.apache.activemq.wireformat.{WireFormat}
import _root_.org.fusesource.hawtdispatch.{DispatchQueue, BaseRetained}
import _root_.org.apache.activemq.util.buffer._
import collection.mutable.{ListBuffer, HashMap}
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._

import AsciiBuffer._
import Stomp._
import BufferConversions._
import StompFrameConstants._
import org.apache.activemq.transport.CompletionCallback
import java.io.IOException


class StompProtocolException(msg:String) extends Exception(msg)

object StompConstants {
  val QUEUE_PREFIX = new AsciiBuffer("/queue/")
  val TOPIC_PREFIX = new AsciiBuffer("/topic/")

  implicit def toDestination(value:AsciiBuffer):Destination = {
    if( value.startsWith(QUEUE_PREFIX) ) {
      new SingleDestination(Domain.QUEUE_DOMAIN, value.slice(QUEUE_PREFIX.length, -QUEUE_PREFIX.length))
    } else if( value.startsWith(TOPIC_PREFIX) ) {
      new SingleDestination(Domain.TOPIC_DOMAIN, value.slice(TOPIC_PREFIX.length, -TOPIC_PREFIX.length))
    } else {
      throw new StompProtocolException("Invalid stomp destiantion name: "+value);
    }
  }

}

import StompConstants._

object StompProtocolHandler extends Log

class StompProtocolHandler extends ProtocolHandler with DispatchLogging {

  override protected def log = StompProtocolHandler
  
  protected def dispatchQueue:DispatchQueue = connection.dispatchQueue

  class SimpleConsumer(val dest:AsciiBuffer) extends BaseRetained with DeliveryConsumer {


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

      def deliver(delivery:Delivery) = session.send(delivery)

      def close = {
        session.close
        release
      }
    }
  }

  val outboundChannel = new DeliveryBuffer
  var closed = false
  var consumer:SimpleConsumer = null

  var producerRoute:DeliveryProducerRoute=null
  var host:VirtualHost = null

  outboundChannel.eventHandler = ^{
    var delivery = outboundChannel.receive
    while( delivery!=null ) {
      connection.transport.oneway(delivery.message, new CompletionCallback() {
        def onCompletion() = {
          outboundChannel.ack(delivery)
        }
        def onFailure(e:Exception) = {
          connection.onFailure(e)
        }
      });
    }
  }

  private def queue = connection.dispatchQueue

  override def onTransportConnected() = {
    connection.broker.runtime.getDefaultVirtualHost(
      queue.wrap { (host)=>
        info("got host.. resuming")
        this.host=host
        connection.transport.resumeRead
      }
    )
  }


  override def onTransportDisconnected() = {
    if( !closed ) {
      info("stop")
      closed=true;
      if( producerRoute!=null ) {
        host.router.disconnect(producerRoute)
        producerRoute=null
      }
      if( consumer!=null ) {
        host.router.unbind(consumer.dest, consumer::Nil)
        consumer=null
      }
    }
  }


  def onTransportCommand(command:Any) = {
    try {
      command match {
        case StompFrame(Commands.SEND, headers, content) =>
          on_stomp_send(command.asInstanceOf[StompFrame])
        case StompFrame(Commands.ACK, headers, content) =>
          // TODO:
        case StompFrame(Commands.SUBSCRIBE, headers, content) =>
          info("got command: %s", command)
          on_stomp_subscribe(headers)
        case StompFrame(Commands.CONNECT, headers, _) =>
          info("got command: %s", command)
          on_stomp_connect(headers)
        case StompFrame(Commands.DISCONNECT, headers, content) =>
          info("got command: %s", command)
          connection.stop
        case s:StompWireFormat =>
          // this is passed on to us by the protocol discriminator
          // so we know which wire format is being used.
        case StompFrame(unknown, _, _) =>
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
    connection.transport.oneway(StompFrame(Responses.CONNECTED))
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
        // create the producer route...
        if( producerRoute==null || producerRoute.destination!= dest ) {

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

            } ->: queue
          }

          // don't process frames until we are connected..
          connection.transport.suspendRead
          host.router.connect(dest, queue, producer) {
            (route) =>
              connection.transport.resumeRead
              producerRoute = route
              send_via_route(producerRoute, frame)
          }
        } else {
          // we can re-use the existing producer route
          send_via_route(producerRoute, frame)
        }
      case None=>
        die("destination not set.")
    }
  }

  def send_via_route(route:DeliveryProducerRoute, frame:StompFrame) = {
    if( !route.targets.isEmpty ) {
      val delivery = Delivery(frame, frame.size)
      connection.transport.suspendRead
      delivery.setDisposer(^{
        connection.transport.resumeRead
      })
      route.targets.foreach(consumer=>{
        consumer.deliver(delivery)
      })
      delivery.release;
    }
  }

  def on_stomp_subscribe(headers:HeaderMap) = {
    get(headers, Headers.Subscribe.DESTINATION) match {
      case Some(dest)=>
        if( consumer !=null ) {
          die("Only one subscription supported.")

        } else {
          info("subscribing to: %s", dest)
          consumer = new SimpleConsumer(dest);
          host.router.bind(dest, consumer :: Nil)
          consumer.release
        }
      case None=>
        die("destination not set.")
    }

  }

  private def die(msg:String) = {
    info("Shutting connection down due to: "+msg)
    connection.transport.suspendRead
    connection.transport.oneway(StompFrame(Responses.ERROR, Nil, ascii(msg)))
    ^ {
      connection.stop()
    } ->: queue
  }

  override def onTransportFailure(error: IOException) = {
    info(error, "Shutting connection down due to: %s", error)
    super.onTransportFailure(error);
  }
}

