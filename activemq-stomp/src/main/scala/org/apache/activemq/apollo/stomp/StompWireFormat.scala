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

import _root_.org.apache.activemq.wireformat.{WireFormatFactory, WireFormat}
import java.nio.ByteBuffer
import _root_.org.fusesource.hawtbuf._
import collection.mutable.{ListBuffer, HashMap}
import AsciiBuffer._
import Stomp._
import Stomp.Headers._

import BufferConversions._
import _root_.scala.collection.JavaConversions._
import StompFrameConstants._
import java.io.{EOFException, DataOutput, DataInput, IOException}
import java.nio.channels.{SocketChannel, WritableByteChannel, ReadableByteChannel}
import org.apache.activemq.apollo.{MemoryAllocation, MemoryPool}

/**
 * Creates WireFormat objects that marshalls the <a href="http://activemq.apache.org/stomp/">Stomp</a> protocol.
 */
class StompWireFormatFactory extends WireFormatFactory {
  import Stomp.Commands.CONNECT

    def createWireFormat() = new StompWireFormat();

    def isDiscriminatable() = true

    def maxWireformatHeaderLength() = CONNECT.length+10;

    def matchesWireformatHeader(header:Buffer) = {
        if( header.length < CONNECT.length) {
          false
        } else {
          // the magic can be preceded with newlines / whitespace..
          header.trimFront.startsWith(CONNECT);
        }
    }
}

object StompWireFormat extends Log {
    val READ_BUFFFER_SIZE = 1024*64;
    val MAX_COMMAND_LENGTH = 1024;
    val MAX_HEADER_LENGTH = 1024 * 10;
    val MAX_HEADERS = 1000;
    val MAX_DATA_LENGTH = 1024 * 1024 * 100;
    val TRIM=true
    val SIZE_CHECK=false
  }

class StompWireFormat extends WireFormat with DispatchLogging {

  import StompWireFormat._
  override protected def log: Log = StompWireFormat

  var memory_pool:MemoryPool = null

  implicit def wrap(x: Buffer) = ByteBuffer.wrap(x.data, x.offset, x.length);
  implicit def wrap(x: Byte) = {
    ByteBuffer.wrap(Array(x));
  }


  def marshal(command:Any, os:DataOutput) = {
    marshal(command.asInstanceOf[StompFrame], os)
  }

  def marshal(command:Any):Buffer= {
    val frame = command.asInstanceOf[StompFrame]
    val os = new DataByteArrayOutputStream(frame.size);
    marshal(frame, os)
    os.toBuffer
  }

  def unmarshal(packet:Buffer):AnyRef = {
    read_start = packet.offset
    read_end = packet.offset
    val bb = packet.toByteBuffer
    bb.position(packet.offset + packet.length)
    unmarshalNB(bb)
  }

  def unmarshal(in: DataInput):AnyRef = {
    throw new UnsupportedOperationException
  }

  def marshal(frame:StompFrame, os:DataOutput) = {
    frame.action.writeTo(os)
    os.write(NEWLINE)

    // Write any updated headers first...
    if( !frame.updated_headers.isEmpty ) {
      for( (key, value) <- frame.updated_headers ) {
        key.writeTo(os)
        os.write(SEPERATOR)
        value.writeTo(os)
        os.write(NEWLINE)
      }
    }

    // we can optimize a little if the headers and content are in the same buffer..
    if( frame.are_headers_in_content_buffer ) {

      val offset = frame.headers.head._1.offset;
      val buffer1 = frame.headers.head._1;
      val buffer2 = frame.content.asInstanceOf[BufferStompContent].content;
      val length = (buffer2.offset-buffer1.offset)+buffer2.length
      os.write( buffer1.data, offset, length)                                                                                            

    } else {
      for( (key, value) <- frame.headers ) {
        key.writeTo(os)
        os.write(SEPERATOR)
        value.writeTo(os)
        os.write(NEWLINE)
      }
      os.write(NEWLINE)
      frame.content.writeTo(os)
    }
    END_OF_FRAME_BUFFER.writeTo(os)
  }

  def getName() = "stomp"

  
  /////////////////////////////////////////////////////////////////////
  //
  // Non blocking write imp
  //
  /////////////////////////////////////////////////////////////////////

  var write_buffer_size = 1024*64;
  var write_counter = 0L
  var write_channel:WritableByteChannel = null

  var next_write_buffer = new DataByteArrayOutputStream(write_buffer_size)
  var next_write_direct:ByteBuffer = null

  var write_buffer = ByteBuffer.allocate(0)
  var write_direct:ByteBuffer = null

  def is_full = next_write_direct!=null || next_write_buffer.size() >= (write_buffer_size >> 2)
  def is_empty = write_buffer.remaining() == 0 && write_direct==null

  def setWritableByteChannel(channel: WritableByteChannel) = {
    this.write_channel = channel
    if( this.write_channel.isInstanceOf[SocketChannel] ) {
      this.write_channel.asInstanceOf[SocketChannel].socket().setSendBufferSize(write_buffer_size);
    }
  }

  def getWriteCounter = write_counter


  def write(command: Any):WireFormat.BufferState =  {
    if ( is_full) {
      WireFormat.BufferState.FULL
    } else {
      val was_empty = is_empty
      marshalX(command.asInstanceOf[StompFrame], next_write_buffer);
      if( was_empty ) {
        WireFormat.BufferState.WAS_EMPTY
      } else {
        WireFormat.BufferState.NOT_EMPTY
      }
    }
  }

  def marshalX(frame:StompFrame, os:DataOutput) = {
    frame.action.writeTo(os)
    os.write(NEWLINE)

    // Write any updated headers first...
    if( !frame.updated_headers.isEmpty ) {
      for( (key, value) <- frame.updated_headers ) {
        key.writeTo(os)
        os.write(SEPERATOR)
        value.writeTo(os)
        os.write(NEWLINE)
      }
    }

    // we can optimize a little if the headers and content are in the same buffer..
    if( frame.are_headers_in_content_buffer ) {

      val offset = frame.headers.head._1.offset;
      val buffer1 = frame.headers.head._1;
      val buffer2 = frame.content.asInstanceOf[BufferStompContent].content;
      val length = (buffer2.offset-buffer1.offset)+buffer2.length
      os.write( buffer1.data, offset, length)

    } else {
      for( (key, value) <- frame.headers ) {
        key.writeTo(os)
        os.write(SEPERATOR)
        value.writeTo(os)
        os.write(NEWLINE)
      }
      os.write(NEWLINE)

      frame.content match {
        case x:DirectStompContent=>
          next_write_direct = x.direct.buffer.duplicate
          next_write_direct.limit(next_write_direct.limit-1)
        case x:BufferStompContent=>
          x.content.writeTo(os)
        case _=>
      }

    }
    END_OF_FRAME_BUFFER.writeTo(os)
  }


  def flush():WireFormat.BufferState = {

    // if we have a pending write that is being sent over the socket...
    if ( write_buffer.remaining() != 0 ) {
      write_counter += write_channel.write(write_buffer)
    }
    if ( write_buffer.remaining() == 0 && write_direct!=null ) {
      write_counter += write_channel.write(write_direct)
    }

    // if it is now empty try to refill...
    if ( is_empty && next_write_buffer.size()!=0 ) {
        // size of next buffer is based on how much was used in the previous buffer.
        val prev_size = (write_buffer.position()+512).max(512).min(write_buffer_size)
        write_buffer = next_write_buffer.toBuffer().toByteBuffer()
        write_direct = next_write_direct
        next_write_buffer = new DataByteArrayOutputStream(prev_size)
        next_write_direct = null
    }

    if ( is_empty ) {
      WireFormat.BufferState.EMPTY
    } else {
      WireFormat.BufferState.NOT_EMPTY
    }

  }


  /////////////////////////////////////////////////////////////////////
  //
  // Non blocking read impl 
  //
  /////////////////////////////////////////////////////////////////////
  
  type FrameReader = (ByteBuffer)=>StompFrame

  var read_counter = 0L
  var read_buffer_size = 1024*64
  var read_channel:ReadableByteChannel = null

  var read_buffer = ByteBuffer.allocate(read_buffer_size)
  var read_end = 0
  var read_start = 0
  var next_action:FrameReader = read_action

  def setReadableByteChannel(channel: ReadableByteChannel) = {
    this.read_channel = channel
    if( this.read_channel.isInstanceOf[SocketChannel] ) {
      this.read_channel.asInstanceOf[SocketChannel].socket().setReceiveBufferSize(read_buffer_size);
    }
  }

  def unread(buffer: Buffer) = {
    assert(read_counter == 0)
    read_buffer.put(buffer.data, buffer.offset, buffer.length)
    read_counter += buffer.length
  }

  def getReadCounter = read_counter

  override def read():Object = {

    var command:Object = null
    while( command==null ) {
      // do we need to read in more data???
      if (read_end == read_buffer.position()) {

          // do we need a new data buffer to read data into??
          if (read_buffer.remaining() == 0) {

              // How much data is still not consumed by the wireformat
              var size = read_end - read_start

              var new_capacity = if(read_start == 0) {
                size+read_buffer_size
              } else {
                if (size > read_buffer_size) {
                  size+read_buffer_size
                } else {
                  read_buffer_size
                }
              }

              var new_buffer = new Array[Byte](new_capacity)

              if (size > 0) {
                  System.arraycopy(read_buffer.array(), read_start, new_buffer, 0, size)
              }

              read_buffer = ByteBuffer.wrap(new_buffer)
              read_buffer.position(size)
              read_start = 0
              read_end = size
          }

          // Try to fill the buffer with data from the socket..
          var p = read_buffer.position()
          var count = read_channel.read(read_buffer)
          if (count == -1) {
              throw new EOFException("Peer disconnected")
          } else if (count == 0) {
              return null
          }
          read_counter += count
      }

      command = next_action(read_buffer)

      // Sanity checks to make sure the wireformat is behaving as expected.
      assert(read_start <= read_end)
      assert(read_end <= read_buffer.position())
    }
    return command
  }

  def unmarshalNB(buffer:ByteBuffer):Object = {
    // keep running the next action until
    // a frame is decoded or we run out of input
    var rc:StompFrame = null
    while( rc == null && read_end!=buffer.position ) {
      rc = next_action(buffer)
    }

//      trace("unmarshalled: "+rc+", start: "+start+", end: "+end+", buffer position: "+buffer.position)
    rc
  }

  def read_line(buffer:ByteBuffer, maxLength:Int, errorMessage:String):Buffer = {
      val read_limit = buffer.position
      while( read_end < read_limit ) {
        if( buffer.array()(read_end) =='\n') {
          var rc = new Buffer(buffer.array, read_start, read_end-read_start)
          read_end += 1
          read_start = read_end
          return rc
        }
        if (SIZE_CHECK && read_end-read_start > maxLength) {
            throw new IOException(errorMessage)
        }
        read_end += 1
      }
      return null
  }

  def read_action:FrameReader = (buffer)=> {
    val line = read_line(buffer, MAX_COMMAND_LENGTH, "The maximum command length was exceeded")
    if( line !=null ) {
      var action = line
      if( TRIM ) {
          action = action.trim()
      }
      if (action.length() > 0) {
          next_action = read_headers(action)
      }
    }
    null
  }

  def read_headers(action:Buffer, headers:HeaderMapBuffer=new HeaderMapBuffer()):FrameReader = (buffer)=> {
    var rc:StompFrame = null
    val line = read_line(buffer, MAX_HEADER_LENGTH, "The maximum header length was exceeded")
    if( line !=null ) {
      if( line.trim().length() > 0 ) {

        if (SIZE_CHECK && headers.size > MAX_HEADERS) {
            throw new IOException("The maximum number of headers was exceeded")
        }

        try {
            val seperatorIndex = line.indexOf(SEPERATOR)
            if( seperatorIndex<0 ) {
                throw new IOException("Header line missing seperator [" + ascii(line) + "]")
            }
            var name = line.slice(0, seperatorIndex)
            if( TRIM ) {
                name = name.trim()
            }
            var value = line.slice(seperatorIndex + 1, line.length())
            if( TRIM ) {
                value = value.trim()
            }
            headers.add((ascii(name), ascii(value)))
        } catch {
            case e:Exception=>
              e.printStackTrace
              throw new IOException("Unable to parser header line [" + line + "]")
        }

      } else {
        val contentLength = get(headers, CONTENT_LENGTH)
        if (contentLength.isDefined) {
          // Bless the client, he's telling us how much data to read in.
          var length=0
          try {
              length = Integer.parseInt(contentLength.get.trim().toString())
          } catch {
            case e:NumberFormatException=>
              throw new IOException("Specified content-length is not a valid integer")
          }

          if (SIZE_CHECK && length > MAX_DATA_LENGTH) {
              throw new IOException("The maximum data length was exceeded")
          }

          if( length > 1024 && memory_pool!=null ) {

            val ma = memory_pool.alloc(length+1)

            val read_limit = buffer.position
            if( (read_limit-read_start) < length+1 ) {
              // buffer did not contain the fully stomp body

              ma.buffer.put( buffer.array, read_start, read_limit-read_start )

              read_buffer = ma.buffer
              read_end = read_limit-read_start
              read_start = 0

              next_action = read_binary_body_direct(action, headers, ma)

            } else {
              // The current buffer already read in all the data...

              if( buffer.array()(read_start+length)!= 0 ) {
                 throw new IOException("Expected null termintor after "+length+" content bytes")
              }

              // copy the body out to the direct buffer
              ma.buffer.put( buffer.array, read_start, read_limit-read_start )

              // and reposition to reuse non-direct space.
              buffer.position(read_start)
              read_end = read_start

              next_action = read_action
              rc = new StompFrame(ascii(action), headers.toList, DirectStompContent(ma))
            }

          } else {
            next_action = read_binary_body(action, headers, length)
          }

        } else {
          next_action = read_text_body(action, headers)
        }
      }
    }
    rc
  }

  def get(headers:HeaderMapBuffer, name:AsciiBuffer):Option[AsciiBuffer] = {
    val i = headers.iterator
    while( i.hasNext ) {
      val entry = i.next
      if( entry._1 == name ) {
        return Some(entry._2)
      }
    }
    None
  }


  def read_binary_body_direct(action:Buffer, headers:HeaderMapBuffer, ma:MemoryAllocation):FrameReader = (buffer)=> {
    if( read_content_direct(ma) ) {
      next_action = read_action
      new StompFrame(ascii(action), headers.toList, DirectStompContent(ma))
    } else {
      null
    }
  }

  def read_content_direct(ma:MemoryAllocation) = {
      val read_limit = ma.buffer.position
      if( read_limit < ma.size ) {
        read_end = read_limit
        false
      } else {
        ma.buffer.position(ma.size-1)
        if( ma.buffer.get != 0 ) {
           throw new IOException("Expected null termintor after "+(ma.size-1)+" content bytes")
        }
        ma.buffer.rewind
        ma.buffer.limit(ma.size-1)

        read_buffer = ByteBuffer.allocate(read_buffer_size)
        read_end = 0
        read_start = 0
        true
      }
  }

  def read_binary_body(action:Buffer, headers:HeaderMapBuffer, contentLength:Int):FrameReader = (buffer)=> {
    val content:Buffer=read_content(buffer, contentLength)
    if( content != null ) {
      next_action = read_action
      new StompFrame(ascii(action), headers.toList, BufferStompContent(content))
    } else {
      null
    }
  }

  def read_content(buffer:ByteBuffer, contentLength:Int):Buffer = {
      val read_limit = buffer.position
      if( (read_limit-read_start) < contentLength+1 ) {
        read_end = read_limit
        null
      } else {
        if( buffer.array()(read_start+contentLength)!= 0 ) {
           throw new IOException("Expected null termintor after "+contentLength+" content bytes")
        }
        var rc = new Buffer(buffer.array, read_start, contentLength)
        read_end = read_start+contentLength+1
        read_start = read_end
        rc
      }
  }

  def read_to_null(buffer:ByteBuffer):Buffer = {
      val read_limit = buffer.position
      while( read_end < read_limit ) {
        if( buffer.array()(read_end) ==0) {
          var rc = new Buffer(buffer.array, read_start, read_end-read_start)
          read_end += 1
          read_start = read_end
          return rc
        }
        read_end += 1
      }
      return null
  }


  def read_text_body(action:Buffer, headers:HeaderMapBuffer):FrameReader = (buffer)=> {
    val content:Buffer=read_to_null(buffer)
    if( content != null ) {
      next_action = read_action
      new StompFrame(ascii(action), headers.toList, BufferStompContent(content))
    } else {
      null
    }
  }

}
