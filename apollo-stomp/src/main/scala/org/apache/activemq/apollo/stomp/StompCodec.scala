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

import java.nio.ByteBuffer
import collection.mutable.{ListBuffer, HashMap}
import Stomp._

import BufferConversions._
import _root_.scala.collection.JavaConversions._
import java.io.{EOFException, DataOutput, DataInput, IOException}
import java.nio.channels.{SocketChannel, WritableByteChannel, ReadableByteChannel}
import org.apache.activemq.apollo.transport._
import _root_.org.fusesource.hawtbuf._
import Buffer._
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.broker.store.{ZeroCopyBuffer, ZeroCopyBufferAllocator, MessageRecord}

object StompCodec extends Log {

  def encode(message: StompFrameMessage):MessageRecord = {
    val frame = message.frame

    val rc = new MessageRecord
    rc.protocol = PROTOCOL
    rc.size = frame.size
    rc.expiration = message.expiration

    if( frame.content.isInstanceOf[ZeroCopyContent] ) {
      rc.zero_copy_buffer = frame.content.asInstanceOf[ZeroCopyContent].zero_copy_buffer
    }

    def buffer_size = if (rc.zero_copy_buffer!=null) { frame.size - (rc.zero_copy_buffer.size - 1) } else { frame.size }
    val os = new ByteArrayOutputStream(buffer_size)

    frame.action.writeTo(os)
    os.write(NEWLINE)

    // Write any updated headers first...
    if( !frame.updated_headers.isEmpty ) {
      for( (key, value) <- frame.updated_headers ) {
        key.writeTo(os)
        os.write(COLON)
        value.writeTo(os)
        os.write(NEWLINE)
      }
    }

    // we can optimize a little if the headers and content are in the same buffer..
    if( frame.are_headers_in_content_buffer ) {

      val offset = frame.headers.head._1.offset;
      val buffer1 = frame.headers.head._1;
      val buffer2 = frame.content.asInstanceOf[BufferContent].content;
      val length = (buffer2.offset-buffer1.offset)+buffer2.length
      os.write( buffer1.data, offset, length)

    } else {
      for( (key, value) <- frame.headers ) {
        key.writeTo(os)
        os.write(COLON)
        value.writeTo(os)
        os.write(NEWLINE)
      }
      os.write(NEWLINE)
      if ( rc.zero_copy_buffer==null ) {
        frame.content.writeTo(os)
      }
    }
    rc.buffer = os.toBuffer
    rc
  }

  def decode(message: MessageRecord):StompFrameMessage = {

    val buffer = message.buffer.buffer
    def read_line = {
      val pos = buffer.indexOf('\n'.toByte)
      if( pos<0 ) {
        throw new IOException("expected a new line")
      } else {
        val rc = buffer.slice(0, pos).ascii
        buffer.offset += (pos+1)
        buffer.length -= (pos+1)
        rc
      }
    }


    val action = read_line

    val headers = new HeaderMapBuffer()

    var line = read_line
    while( line.length() > 0 ) {
      try {
          val seperatorIndex = line.indexOf(COLON)
          if( seperatorIndex<0 ) {
              throw new IOException("Header line missing seperator.")
          }
          var name = line.slice(0, seperatorIndex)
          var value = line.slice(seperatorIndex + 1, line.length)
          headers.add((name, value))
      } catch {
          case e:Exception=>
            e.printStackTrace
            throw new IOException("Unable to parser header line [" + line + "]")
      }
      line = read_line
    }

    if( message.zero_copy_buffer==null ) {
      new StompFrameMessage(new StompFrame(action, headers.toList, BufferContent(buffer)))
    } else {
      new StompFrameMessage(new StompFrame(action, headers.toList, ZeroCopyContent(message.zero_copy_buffer)))
    }
  }

}

class StompCodec extends ProtocolCodec {

  import StompCodec._

  var max_command_length = 1024
  var max_header_length = 1024*10
  var max_headers = 1000
  var max_data_length = 1024 * 1024 * 100

  var zero_copy_buffer_allocator:ZeroCopyBufferAllocator = null

  implicit def wrap(x: Buffer) = ByteBuffer.wrap(x.data, x.offset, x.length);
  implicit def wrap(x: Byte) = {
    ByteBuffer.wrap(Array(x));
  }

  def protocol() = "stomp"

  
  /////////////////////////////////////////////////////////////////////
  //
  // Non blocking write imp
  //
  /////////////////////////////////////////////////////////////////////

  var write_buffer_size = 1024*64;
  var write_counter = 0L
  var write_channel:WritableByteChannel = null

  var next_write_buffer = new DataByteArrayOutputStream(write_buffer_size)
  var next_write_direct:ZeroCopyBuffer = null

  var write_buffer = ByteBuffer.allocate(0)
  var write_direct:ZeroCopyBuffer = null
  var write_direct_pos = 0

  def full = next_write_direct!=null || next_write_buffer.size >= (write_buffer_size >> 1)
  def is_empty = write_buffer.remaining == 0 && write_direct==null

  def setWritableByteChannel(channel: WritableByteChannel) = {
    this.write_channel = channel
    if( this.write_channel.isInstanceOf[SocketChannel] ) {
      this.write_channel.asInstanceOf[SocketChannel].socket().setSendBufferSize(write_buffer_size);
    }
  }

  def getWriteCounter = write_counter


  def write(command: Any):ProtocolCodec.BufferState =  {
    if ( full) {
      ProtocolCodec.BufferState.FULL
    } else {
      val was_empty = is_empty
      command match {
        case buffer:Buffer=>
          buffer.writeTo(next_write_buffer.asInstanceOf[DataOutput])        
        case frame:StompFrame=>
          encode(frame, next_write_buffer);
      }
      if( was_empty ) {
        ProtocolCodec.BufferState.WAS_EMPTY
      } else {
        ProtocolCodec.BufferState.NOT_EMPTY
      }
    }
  }

  def encode(frame:StompFrame, os:DataOutput) = {
    frame.action.writeTo(os)
    os.write(NEWLINE)

    // Write any updated headers first...
    if( !frame.updated_headers.isEmpty ) {
      for( (key, value) <- frame.updated_headers ) {
        key.writeTo(os)
        os.write(COLON)
        value.writeTo(os)
        os.write(NEWLINE)
      }
    }

    // we can optimize a little if the headers and content are in the same buffer..
    if( frame.are_headers_in_content_buffer ) {

      val offset = frame.headers.head._1.offset;
      val buffer1 = frame.headers.head._1;
      val buffer2 = frame.content.asInstanceOf[BufferContent].content;
      val length = (buffer2.offset-buffer1.offset)+buffer2.length
      os.write( buffer1.data, offset, length)
      END_OF_FRAME_BUFFER.writeTo(os)

    } else {
      for( (key, value) <- frame.headers ) {
        key.writeTo(os)
        os.write(COLON)
        value.writeTo(os)
        os.write(NEWLINE)
      }
      os.write(NEWLINE)

      frame.content match {
        case x:ZeroCopyContent=>
          assert(next_write_direct==null)
          next_write_direct = x.zero_copy_buffer
        case x:BufferContent=>
          x.content.writeTo(os)
          END_OF_FRAME_BUFFER.writeTo(os)
        case _=>
          END_OF_FRAME_BUFFER.writeTo(os)
      }
    }
  }


  def flush():ProtocolCodec.BufferState = {

    // if we have a pending write that is being sent over the socket...
    if ( write_buffer.remaining() != 0 ) {
      write_counter += write_channel.write(write_buffer)
    }
    if ( write_buffer.remaining() == 0 && write_direct!=null ) {
      val count = write_direct.read(write_direct_pos, write_channel)
      write_direct_pos += count
      write_counter += count

      if( write_direct.remaining(write_direct_pos) == 0 ) {
        write_direct.release
        write_direct = null
        write_direct_pos = 0

        write_buffer = ByteBuffer.wrap(END_OF_FRAME_BUFFER.data)
      }
    }

    // if it is now empty try to refill...
    if ( is_empty && write_direct==null ) {
        // size of next buffer is based on how much was used in the previous buffer.
        val prev_size = (write_buffer.position()+512).max(512).min(write_buffer_size)
        write_buffer = next_write_buffer.toBuffer().toByteBuffer()
        write_direct = next_write_direct

        next_write_buffer = new DataByteArrayOutputStream(prev_size)
        next_write_direct = null
    }

    if ( is_empty ) {
      ProtocolCodec.BufferState.EMPTY
    } else {
      ProtocolCodec.BufferState.NOT_EMPTY
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

  var read_direct:ZeroCopyBuffer = null
  var read_direct_pos = 0

  var next_action:FrameReader = read_action
  var trim = true

  def setReadableByteChannel(channel: ReadableByteChannel) = {
    this.read_channel = channel
    if( this.read_channel.isInstanceOf[SocketChannel] ) {
      read_buffer_size = this.read_channel.asInstanceOf[SocketChannel].socket().getReceiveBufferSize
      read_buffer = ByteBuffer.allocate(read_buffer_size)
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
      if( read_direct!=null && read_direct.remaining(read_direct_pos) > 0) {
        val count = read_direct.write(read_channel, read_direct_pos)
        if (count == -1) {
            throw new EOFException("Peer disconnected")
        } else if (count == 0) {
            return null
        }
        read_direct_pos += count
      } else if (read_end == read_buffer.position() ) {

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

  def read_line(buffer:ByteBuffer, max:Int, errorMessage:String):Buffer = {
      val read_limit = buffer.position
      while( read_end < read_limit ) {
        if( buffer.array()(read_end) =='\n') {
          var rc = new Buffer(buffer.array, read_start, read_end-read_start)
          read_end += 1
          read_start = read_end
          return rc
        }
        if (max != -1 && read_end-read_start > max) {
            throw new IOException(errorMessage)
        }
        read_end += 1
      }
      return null
  }

  def read_action:FrameReader = (buffer)=> {
    val line = read_line(buffer, max_command_length, "The maximum command length was exceeded")
    if( line !=null ) {
      var action = line
      if( trim ) {
          action = action.trim()
      }
      if (action.length() > 0) {
          next_action = read_headers(action.ascii)
      }
    }
    null
  }

  def read_headers(action:AsciiBuffer, headers:HeaderMapBuffer=new HeaderMapBuffer()):FrameReader = (buffer)=> {
    var rc:StompFrame = null
    val line = read_line(buffer, max_header_length, "The maximum header length was exceeded")
    if( line !=null ) {
      if( line.trim().length > 0 ) {

        if (max_headers != -1 && headers.size > max_headers) {
            throw new IOException("The maximum number of headers was exceeded")
        }

        try {
            val seperatorIndex = line.indexOf(COLON)
            if( seperatorIndex<0 ) {
                throw new IOException("Header line missing seperator [" + ascii(line) + "]")
            }
            var name = line.slice(0, seperatorIndex)
            if( trim ) {
                name = name.trim()
            }
            var value = line.slice(seperatorIndex + 1, line.length())
            if( trim ) {
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

          if (max_data_length != -1 && length > max_data_length) {
              throw new IOException("The maximum data length was exceeded")
          }

          // lets try to keep the content of big message outside of the JVM's garbage collection
          // to keep the number of GCs down when moving big messages.
          def is_message = action == SEND || action == MESSAGE
          if( length > 1024 && zero_copy_buffer_allocator!=null && is_message) {

            read_direct = zero_copy_buffer_allocator.alloc(length)

            val dup = buffer.duplicate
            dup.position(read_start)
            dup.limit(buffer.position)

            // copy in the body the was read so far...
            read_direct_pos = read_direct.write(dup, 0)

            // since it was copied.. reposition to re-use the copied area..
            dup.compact
            buffer.position(buffer.position - read_direct_pos)
            read_end = read_start

            next_action = read_binary_body_direct(action, headers, length)
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

  def read_binary_body_direct(action:AsciiBuffer, headers:HeaderMapBuffer, contentLength:Int):FrameReader = (buffer)=> {
    if( read_direct.remaining(read_direct_pos)==0 ) {
      next_action = read_direct_terminator(action, headers, contentLength, read_direct)
      read_direct = null
      read_direct_pos = 0
    }
    null
  }

  def read_direct_terminator(action:AsciiBuffer, headers:HeaderMapBuffer, contentLength:Int, ma:ZeroCopyBuffer):FrameReader = (buffer)=> {
    if( read_frame_terminator(buffer, contentLength) ) {
      next_action = read_action
      new StompFrame(ascii(action), headers.toList, ZeroCopyContent(ma))
    } else {
      null
    }
  }

  def read_frame_terminator(buffer:ByteBuffer, contentLength:Int):Boolean = {
      val read_limit = buffer.position
      if( (read_limit-read_start) < 1 ) {
        read_end = read_limit
        false
      } else {
        if( buffer.array()(read_start)!= 0 ) {
           throw new IOException("Expected null termintor after "+contentLength+" content bytes")
        }
        read_end = read_start+1
        read_start = read_end
        true
      }
  }

  def read_binary_body(action:AsciiBuffer, headers:HeaderMapBuffer, contentLength:Int):FrameReader = (buffer)=> {
    val content:Buffer=read_content(buffer, contentLength)
    if( content != null ) {
      next_action = read_action
      new StompFrame(ascii(action), headers.toList, BufferContent(content))
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


  def read_text_body(action:AsciiBuffer, headers:HeaderMapBuffer):FrameReader = (buffer)=> {
    val content:Buffer=read_to_null(buffer)
    if( content != null ) {
      next_action = read_action
      new StompFrame(ascii(action), headers.toList, BufferContent(content))
    } else {
      null
    }
  }

}
