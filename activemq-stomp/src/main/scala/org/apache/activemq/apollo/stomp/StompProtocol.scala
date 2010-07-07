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

import _root_.java.util.{LinkedList, ArrayList}
import _root_.org.apache.activemq.apollo.broker._

import _root_.org.apache.activemq.wireformat.WireFormat
import _root_.org.fusesource.hawtdispatch.{DispatchQueue, BaseRetained}
import java.nio.channels.{SocketChannel}
import java.nio.ByteBuffer
import java.io.{EOFException, IOException}
import _root_.org.apache.activemq.util.buffer._
import collection.mutable.{ListBuffer, HashMap}
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._

import AsciiBuffer._
import Stomp._
import Stomp.Headers._

import BufferConversions._
import _root_.scala.collection.JavaConversions._
import StompFrameConstants._;


class StompProtocolException(msg:String) extends Exception(msg)

object StompConstants {
  val QUEUE_PREFIX = new AsciiBuffer("/topic/")
  val TOPIC_PREFIX = new AsciiBuffer("/queue/")

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

class StompProtocolHandler extends ProtocolHandler {


  class SimpleConsumer(val dest:AsciiBuffer) extends BaseRetained with DeliveryConsumer {

    val queue = StompProtocolHandler.this.dispatchQueue
    queue.retain
    setDisposer(^{ queue.release  })

    val deliveryQueue = new DeliveryCreditBufferProtocol(outboundChannel, queue)

    def matches(message:Delivery) = true

    def open_session(producer_queue:DispatchQueue) = new DeliverySession {
      val session = deliveryQueue.session(producer_queue)

      val consumer = SimpleConsumer.this
      retain

      def deliver(delivery:Delivery) = session.send(delivery)

      def close = {
        session.close
        release
      }
    }
  }

  def dispatchQueue = connection.dispatchQueue
  val outboundChannel  = new DeliveryBuffer
  var closed = false
  var consumer:SimpleConsumer = null

  var connection:BrokerConnection = null
  var wireformat:WireFormat = null
  var producerRoute:DeliveryProducerRoute=null
  var host:VirtualHost = null

  private def queue = connection.dispatchQueue

  def setConnection(connection:BrokerConnection) = {
    this.connection = connection

    // We will be using the default virtual host
    connection.broker.getDefaultVirtualHost(
      queue.wrap { (host)=>
        this.host=host
      }
    )
  }

  def setWireFormat(wireformat:WireFormat) = { this.wireformat = wireformat}

  def onCommand(command:Any) = {
    val frame = command.asInstanceOf[StompFrame]
    frame match {
      case StompFrame(Commands.CONNECT, headers, _) =>
        on_stomp_connect(headers)
      case StompFrame(Commands.SEND, headers, content) =>
        on_stomp_send(frame)
      case StompFrame(Commands.SUBSCRIBE, headers, content) =>
        on_stomp_subscribe(headers)
      case StompFrame(Commands.ACK, headers, content) =>
        // TODO:
      case StompFrame(Commands.DISCONNECT, headers, content) =>
        stop
      case StompFrame(unknown, _, _) =>
        die("Unsupported STOMP command: "+unknown);
    }
  }


  def on_stomp_connect(headers:HeaderMap) = {
    println("connected on: "+Thread.currentThread.getName);
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
    println("Consumer on "+Thread.currentThread.getName)
    get(headers, Headers.Subscribe.DESTINATION) match {
      case Some(dest)=>
        if( consumer !=null ) {
          die("Only one subscription supported.")

        } else {
          consumer = new SimpleConsumer(dest);
          host.router.bind(dest, consumer :: Nil)
          consumer.release
        }
      case None=>
        die("destination not set.")
    }

  }

  private def die(msg:String) = {
    println("Shutting connection down due to: "+msg)
    connection.transport.suspendRead
    connection.transport.oneway(StompFrame(Responses.ERROR, Nil, ascii(msg)))
    ^ {
      stop
    } ->: queue
  }

  def onException(error:Exception) = {
    println("Shutting connection down due to: "+error)
    stop
  }

  def start = {
  }
  
  def stop = {
    if( !closed ) {
      closed=true;
      if( producerRoute!=null ) {
        host.router.disconnect(producerRoute)
        producerRoute=null
      }
      if( consumer!=null ) {
        host.router.unbind(consumer.dest, consumer::Nil)
        consumer=null
      }
      connection.stop
    }
  }
}

object StompWireFormat {
    val READ_BUFFFER_SIZE = 1024*64;
    val MAX_COMMAND_LENGTH = 1024;
    val MAX_HEADER_LENGTH = 1024 * 10;
    val MAX_HEADERS = 1000;
    val MAX_DATA_LENGTH = 1024 * 1024 * 100;
    val TRIM=false
    val SIZE_CHECK=false
  }

class StompWireFormat {
  import StompWireFormat._

  implicit def wrap(x: Buffer) = ByteBuffer.wrap(x.data, x.offset, x.length);
  implicit def wrap(x: Byte) = {
    ByteBuffer.wrap(Array(x));
  }

  var outbound_frame: ByteBuffer = null
  /**
   * @retruns true if the source has been drained of StompFrame objects and they are fully written to the socket
   */
  def drain_source(socket:SocketChannel)(source: =>StompFrame ):Boolean = {
    while(true) {
      // if we have a pending frame that is being sent over the socket...
      if( outbound_frame!=null ) {
        socket.write(outbound_frame)
        if( outbound_frame.remaining != 0 ) {
          // non blocking socket returned before the buffers were fully written to disk..
          // we are not yet fully drained.. but need to quit now.
          return false
        } else {
          outbound_frame = null
        }
      } else {

        // marshall all the available frames..
        val buffer = new ByteArrayOutputStream()
        var frame = source
        while( frame!=null ) {
          marshall(buffer, frame)
          frame = source
        }


        if( buffer.size() ==0 ) {
          // the source is now drained...
          return true
        } else {
          val b = buffer.toBuffer;
          outbound_frame = ByteBuffer.wrap(b.data, b.offset, b.length)
        }
      }
    }
    true
  }

  def marshall(buffer:ByteArrayOutputStream, frame:StompFrame) = {
    buffer.write(frame.action)
    buffer.write(NEWLINE)

    // we can optimize a little if the headers and content are in the same buffer..
    if( !frame.headers.isEmpty && !frame.content.isEmpty &&
            ( frame.headers.head._1.data eq frame.content.data ) ) {

      val offset = frame.headers.head._1.offset;
      val buffer1 = frame.headers.head._1;
      val buffer2 = frame.content;
      val length = (buffer2.offset-buffer1.offset)+buffer2.length
      buffer.write( buffer1.data, offset, length)

    } else {
      for( (key, value) <- frame.headers ) {
        buffer.write(key)
        buffer.write(SEPERATOR)
        buffer.write(value)
        buffer.write(NEWLINE)
      }

      buffer.write(NEWLINE)
      buffer.write(frame.content)
    }
    buffer.write(END_OF_FRAME_BUFFER)
  }


  var read_pos = 0
  var read_offset = 0
  var read_data:Array[Byte] = new Array[Byte](READ_BUFFFER_SIZE)
  var read_bytebuffer:ByteBuffer = ByteBuffer.wrap(read_data)

  def drain_socket(socket:SocketChannel)(handler:(StompFrame)=>Boolean) = {
    var done = false

    // keep going until the socket buffer is drained.
    while( !done ) {
      val frame = unmarshall()
      if( frame!=null ) {
        // the handler might want us to stop looping..
        done = handler(frame)
      } else {

        // do we need to read in more data???
        if( read_pos==read_bytebuffer.position ) {

          // do we need a new data buffer to read data into??
          if(read_bytebuffer.remaining==0) {

            // The capacity needed grows by powers of 2...
            val new_capacity = if( read_offset != 0 ) { READ_BUFFFER_SIZE } else { read_data.length << 2 }
            val tmp_buffer = new Array[Byte](new_capacity)

            // If there was un-consummed data.. copy it over...
            val size = read_pos - read_offset
            if( size > 0 ) {
              System.arraycopy(read_data, read_offset, tmp_buffer, 0, size)
            }
            read_data = tmp_buffer
            read_bytebuffer = ByteBuffer.wrap(read_data)
            read_bytebuffer.position(size)
            read_offset = 0;
            read_pos = size

          }

          // Try to fill the buffer with data from the nio socket..
          var p = read_bytebuffer.position
          if( socket.read(read_bytebuffer) == -1 ) {
            throw new EOFException();
          }
          // we are done if there was no data on the socket.
          done = read_bytebuffer.position==p
        }
      }
    }
  }


  type FrameReader = ()=>StompFrame
  var unmarshall:FrameReader = read_action

  def read_line( maxLength:Int, errorMessage:String):Buffer = {
      val read_limit = read_bytebuffer.position
      while( read_pos < read_limit ) {
        if( read_data(read_pos) =='\n') {
          var rc = new Buffer(read_data, read_offset, read_pos-read_offset)
          read_pos += 1;
          read_offset = read_pos;
          return rc
        }
        if (SIZE_CHECK && read_pos-read_offset > maxLength) {
            throw new IOException(errorMessage);
        }
        read_pos += 1;
      }
      return null;
  }


  def read_action:FrameReader = ()=> {
    val line = read_line(MAX_COMMAND_LENGTH, "The maximum command length was exceeded")
    if( line !=null ) {
      var action = line
      if( TRIM ) {
          action = action.trim();
      }
      if (action.length() > 0) {
          unmarshall = read_headers(action)
      }
    }
    null
  }

  def read_headers(action:Buffer, headers:HeaderMap=Nil):FrameReader = ()=> {
    val line = read_line(MAX_HEADER_LENGTH, "The maximum header length was exceeded")
    if( line !=null ) {
      if( line.trim().length() > 0 ) {

        if (SIZE_CHECK && headers.size > MAX_HEADERS) {
            throw new IOException("The maximum number of headers was exceeded");
        }

        try {
            val seperatorIndex = line.indexOf(SEPERATOR);
            if( seperatorIndex<0 ) {
                throw new IOException("Header line missing seperator [" + ascii(line) + "]");
            }
            var name = line.slice(0, seperatorIndex);
            if( TRIM ) {
                name = name.trim();
            }
            var value = line.slice(seperatorIndex + 1, line.length());
            if( TRIM ) {
                value = value.trim();
            }
            headers.add((ascii(name), ascii(value)));
        } catch {
            case e:Exception=>
              throw new IOException("Unable to parser header line [" + line + "]");
        }

      } else {
        val contentLength = get(headers, CONTENT_LENGTH)
        if (contentLength.isDefined) {
          // Bless the client, he's telling us how much data to read in.
          var length=0;
          try {
              length = Integer.parseInt(contentLength.get.trim().toString());
          } catch {
            case e:NumberFormatException=>
              throw new IOException("Specified content-length is not a valid integer");
          }

          if (SIZE_CHECK && length > MAX_DATA_LENGTH) {
              throw new IOException("The maximum data length was exceeded");
          }
          unmarshall = read_binary_body(action, headers, length)

        } else {
          unmarshall = read_text_body(action, headers)
        }
      }
    }
    null
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


  def read_binary_body(action:Buffer, headers:HeaderMap, contentLength:Int):FrameReader = ()=> {
    val content:Buffer=read_content(contentLength)
    if( content != null ) {
      unmarshall = read_action
      new StompFrame(ascii(action), headers, content)
    } else {
      null
    }
  }


  def read_content(contentLength:Int):Buffer = {
      val read_limit = read_bytebuffer.position
      if( (read_limit-read_offset) < contentLength+1 ) {
        read_pos = read_limit;
        null
      } else {
        if( read_data(read_offset+contentLength)!= 0 ) {
           throw new IOException("Exected null termintor after "+contentLength+" content bytes");
        }
        var rc = new Buffer(read_data, read_offset, contentLength)
        read_pos = read_offset+contentLength+1;
        read_offset = read_pos;
        rc;
      }
  }

  def read_to_null():Buffer = {
      val read_limit = read_bytebuffer.position
      while( read_pos < read_limit ) {
        if( read_data(read_pos) ==0) {
          var rc = new Buffer(read_data, read_offset, read_pos-read_offset)
          read_pos += 1;
          read_offset = read_pos;
          return rc;
        }
        read_pos += 1;
      }
      return null;
  }


  def read_text_body(action:Buffer, headers:HeaderMap):FrameReader = ()=> {
    val content:Buffer=read_to_null
    if( content != null ) {
      unmarshall = read_action
      new StompFrame(ascii(action), headers, content)
    } else {
      null
    }
  }

}
