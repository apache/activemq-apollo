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

import Stomp._

import BufferConversions._
import _root_.scala.collection.JavaConversions._
import java.io.{DataOutput, IOException}
import org.fusesource.hawtdispatch.transport._
import _root_.org.fusesource.hawtbuf._
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.broker.store.{DirectBuffer, MessageRecord}
import java.lang.String
import collection.mutable.ListBuffer

class StompProtocolException(message:String) extends IOException(message)

object StompCodec extends Log {

  var max_command_length = 20

  def encode(message: StompFrameMessage):MessageRecord = {
    val frame = message.frame

    val rc = new MessageRecord
    rc.codec = PROTOCOL

    if( frame.content.isInstanceOf[ZeroCopyContent] ) {
      rc.direct_buffer = frame.content.asInstanceOf[ZeroCopyContent].zero_copy_buffer
    }

    def buffer_size = if (rc.direct_buffer!=null) { frame.size - (rc.direct_buffer.size - 1) } else { frame.size }
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
    if( frame.are_headers_in_content_buffer && frame.contiguous ) {

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
      if ( rc.direct_buffer==null ) {
        frame.content.writeTo(os)
      }
    }
    rc.buffer = os.toBuffer
    rc
  }

  def decode(message: MessageRecord):StompFrameMessage = {
    new StompFrameMessage(decode_frame(message.buffer, message.direct_buffer, false))
  }

  def decode_frame(buffer: Buffer, direct_buffer:DirectBuffer=null, end_check:Boolean=true):StompFrame = {
    def read_line = {
      val pos = buffer.indexOf('\n'.toByte)
      if( pos<0 ) {
        throw new StompProtocolException("expected a new line")
      } else {
        val rc = buffer.slice(0, pos).ascii
        buffer.offset += (pos+1)
        buffer.length -= (pos+1)
        rc
      }
    }


    val action = read_line

    val headers = new HeaderMapBuffer()
    var contentLength:AsciiBuffer = null

    var line = read_line
    while( line.length() > 0 ) {
      try {
          val seperatorIndex = line.indexOf(COLON)
          if( seperatorIndex<0 ) {
              throw new StompProtocolException("Header line missing separator.")
          }
          var name = line.slice(0, seperatorIndex)
          var value = line.slice(seperatorIndex + 1, line.length)
          headers.add((name, value))
          if (end_check && contentLength==null && name == CONTENT_LENGTH ) {
            contentLength = value
          }
      } catch {
          case e:Exception=>
            throw new StompProtocolException("Unable to parse header line [" + Log.escape(line) + "]")
      }
      line = read_line
    }

    if ( end_check ) {
      buffer.length = if (contentLength != null) {
        val length = try {
          contentLength.toString.toInt
        } catch {
          case e: NumberFormatException =>
            throw new StompProtocolException("Specified content-length is not a valid integer")
        }
        if( length > buffer.length ) {
          throw new StompProtocolException("Frame did not contain enough bytes to satisfy the content-length")
        }
        length
      } else {
        val pos = buffer.indexOf(0.toByte)
        if( pos < 0 ) {
          throw new StompProtocolException("Frame is not null terminated")
        }
        pos
      }
    }

    if( direct_buffer==null ) {
      new StompFrame(action, headers.toList, BufferContent(buffer), true)
    } else {
      new StompFrame(action, headers.toList, ZeroCopyContent(direct_buffer), true)
    }
  }

}

class StompCodec extends AbstractProtocolCodec {
  this.bufferPools = Broker.buffer_pools
  var max_header_length: Int = 1024 * 10
  var max_headers: Int = 1000
  var max_data_length: Int = 1024 * 1024 * 100
  var trim = true
  var trim_cr = false

  protected def encode(command: AnyRef) = command match {
    case buffer:Buffer=> buffer.writeTo(nextWriteBuffer.asInstanceOf[DataOutput])
    case frame:StompFrame=> encode(frame, nextWriteBuffer);
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
    if( frame.are_headers_in_content_buffer && frame.contiguous) {

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
//        case x:ZeroCopyContent=>
//          assert(next_write_direct==null)
//          next_write_direct = x.zero_copy_buffer
        case x:BufferContent=>
          x.content.writeTo(os)
          END_OF_FRAME_BUFFER.writeTo(os)
        case _=>
          END_OF_FRAME_BUFFER.writeTo(os)
      }
    }
  }

  import StompCodec._

  protected def initialDecodeAction = read_action


  private final val read_action: AbstractProtocolCodec.Action = new AbstractProtocolCodec.Action {
    def apply: AnyRef = {
      var line = readUntil(NEWLINE, max_command_length, "The maximum command length was exceeded")
      if (line != null) {
        var action = line.moveTail(-1)
        var contiguous = true
        if (trim) {
          action = action.trim
        } else if( trim_cr && action.length > 0 && action.get(action.length-1)=='\r'.toByte ) {
          action.moveTail(-1)
          contiguous = false
        }
        if (action.length > 0) {
          nextDecodeAction = read_headers(action.ascii, contiguous)
          return nextDecodeAction();
        }
      }
      return null
    }
  }

  private def read_headers(command: AsciiBuffer, c:Boolean): AbstractProtocolCodec.Action = new AbstractProtocolCodec.Action {
    var contentLength:AsciiBuffer = _
    val headers = new ListBuffer[(AsciiBuffer, AsciiBuffer)]()
    var contiguous = c;

    def apply: AnyRef = {
      var line = readUntil(NEWLINE, max_header_length, "The maximum header length was exceeded")
      if (line != null) {

        // Strip off the \n
        line.moveTail(-1)
        // 1.0 and 1.2 spec trims off the \r
        if ( (trim || trim_cr) && line.length > 0 && line.get(line.length-1)=='\r'.toByte ) {
          contiguous = false
          line.moveTail(-1)
        }

        if (line.length > 0) {
          if (max_headers != -1 && headers.size > max_headers) {
            throw new StompProtocolException("The maximum number of headers was exceeded")
          }
          try {
            var seperatorIndex: Int = line.indexOf(COLON)
            if (seperatorIndex < 0) {
              throw new StompProtocolException("Header line missing separator [" +  Log.escape(line.ascii) + "]")
            }
            var name: Buffer = line.slice(0, seperatorIndex)
            if (trim) {
              name = name.trim
            }
            if( name.length() == 0 ) {
              throw new StompProtocolException("Header line header name is empty: [" +  Log.escape(line.ascii) + "]")
            }
            var value: Buffer = line.slice(seperatorIndex + 1, line.length)
            if (trim) {
              value = value.trim
            }
            var entry = (name.ascii, value.ascii)
            if (contentLength==null && entry._1 == CONTENT_LENGTH) {
              contentLength = entry._2
            }
            headers.add(entry)
          } catch {
            case e: Exception => {
              throw new StompProtocolException("Unable to parser header line [" +  Log.escape(line.ascii) + "]")
            }
          }
        } else {
          val h = headers.toList
          if (contentLength != null) {
            var length = try {
              contentLength.toString.toInt
            } catch {
              case e: NumberFormatException =>
                throw new StompProtocolException("Specified content-length is not a valid integer")
            }
            if (max_data_length != -1 && length > max_data_length) {
              throw new StompProtocolException("The maximum data length was exceeded")
            }
            nextDecodeAction = read_binary_body(command, h, length, contiguous)
          } else {
            nextDecodeAction = read_text_body(command, h, contiguous)
          }
          return nextDecodeAction.apply()
        }
      }
      return null
    }
  }

  private def read_binary_body(command: AsciiBuffer, headers:HeaderMap, contentLength: Int, contiguous:Boolean): AbstractProtocolCodec.Action = {
    return new AbstractProtocolCodec.Action {
      def apply: AnyRef = {
        var content = readBytes(contentLength + 1)
        if (content != null) {
          if (content.get(contentLength) != 0) {
            throw new StompProtocolException("Expected null terminator after " + contentLength + " content bytes")
          }
          nextDecodeAction = read_action
          content.moveTail(-1)
          val body = if( content.length() == 0) NilContent else BufferContent(content)
          return new StompFrame(command, headers, body, contiguous)
        }
        else {
          return null
        }
      }
    }
  }

  private def read_text_body(command: AsciiBuffer, headers:HeaderMap, contiguous:Boolean): AbstractProtocolCodec.Action = {
    return new AbstractProtocolCodec.Action {
      def apply: AnyRef = {
        var content: Buffer = readUntil(0.asInstanceOf[Byte])
        if (content != null) {
          nextDecodeAction = read_action
          content.moveTail(-1)
          val body = if( content.length() == 0) NilContent else BufferContent(content)
          return new StompFrame(command, headers, body, contiguous)
        }
        else {
          return null
        }
      }
    }
  }


}