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

import _root_.java.io.{DataOutput, DataInput, IOException}
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

  def marshal(frame:StompFrame, os:DataOutput) = {
    frame.action.writeTo(os)
    os.write(NEWLINE)

    // we can optimize a little if the headers and content are in the same buffer..
    if( !frame.headers.isEmpty && !frame.content.isEmpty &&
            ( frame.headers.head._1.data eq frame.content.data ) ) {

      val offset = frame.headers.head._1.offset;
      val buffer1 = frame.headers.head._1;
      val buffer2 = frame.content;
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

  def unmarshal(packet:Buffer) = {
    throw new UnsupportedOperationException
  }
  def unmarshal(in: DataInput):Object = {
    throw new UnsupportedOperationException
  }

  def getName() = "stomp"

  //
  // state associated with un-marshalling stomp frames from
  // with the  unmarshalNB method.
  //
  type FrameReader = (ByteBuffer)=>StompFrame

  var next_action:FrameReader = read_action
  var end = 0
  var start = 0

  def unmarshalStartPos() = start
  def unmarshalStartPos(pos:Int):Unit = {start=pos}

  def unmarshalEndPos() = end
  def unmarshalEndPos(pos:Int):Unit = { end = pos }

  def unmarshalNB(buffer:ByteBuffer):Object = {
    // keep running the next action until
    // a frame is decoded or we run out of input
    var rc:StompFrame = null
    while( rc == null && end!=buffer.position ) {
      rc = next_action(buffer)
    }

//      trace("unmarshalled: "+rc+", start: "+start+", end: "+end+", buffer position: "+buffer.position)
    rc
  }

  def read_line(buffer:ByteBuffer, maxLength:Int, errorMessage:String):Buffer = {
      val read_limit = buffer.position
      while( end < read_limit ) {
        if( buffer.array()(end) =='\n') {
          var rc = new Buffer(buffer.array, start, end-start)
          end += 1;
          start = end;
          return rc
        }
        if (SIZE_CHECK && end-start > maxLength) {
            throw new IOException(errorMessage);
        }
        end += 1;
      }
      return null;
  }

  def read_action:FrameReader = (buffer)=> {
    val line = read_line(buffer, MAX_COMMAND_LENGTH, "The maximum command length was exceeded")
    if( line !=null ) {
      var action = line
      if( TRIM ) {
          action = action.trim();
      }
      if (action.length() > 0) {
          next_action = read_headers(action)
      }
    }
    null
  }

  def read_headers(action:Buffer, headers:HeaderMapBuffer=new HeaderMapBuffer()):FrameReader = (buffer)=> {
    val line = read_line(buffer, MAX_HEADER_LENGTH, "The maximum header length was exceeded")
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
            headers.add((ascii(name), ascii(value)))
        } catch {
            case e:Exception=>
              e.printStackTrace
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
          next_action = read_binary_body(action, headers, length)

        } else {
          next_action = read_text_body(action, headers)
        }
      }
    }
    null
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


  def read_binary_body(action:Buffer, headers:HeaderMapBuffer, contentLength:Int):FrameReader = (buffer)=> {
    val content:Buffer=read_content(buffer, contentLength)
    if( content != null ) {
      next_action = read_action
      new StompFrame(ascii(action), headers.toList, content)
    } else {
      null
    }
  }


  def read_content(buffer:ByteBuffer, contentLength:Int):Buffer = {
      val read_limit = buffer.position
      if( (read_limit-start) < contentLength+1 ) {
        end = read_limit;
        null
      } else {
        if( buffer.array()(start+contentLength)!= 0 ) {
           throw new IOException("Exected null termintor after "+contentLength+" content bytes");
        }
        var rc = new Buffer(buffer.array, start, contentLength)
        end = start+contentLength+1;
        start = end;
        rc;
      }
  }

  def read_to_null(buffer:ByteBuffer):Buffer = {
      val read_limit = buffer.position
      while( end < read_limit ) {
        if( buffer.array()(end) ==0) {
          var rc = new Buffer(buffer.array, start, end-start)
          end += 1;
          start = end;
          return rc;
        }
        end += 1;
      }
      return null;
  }


  def read_text_body(action:Buffer, headers:HeaderMapBuffer):FrameReader = (buffer)=> {
    val content:Buffer=read_to_null(buffer)
    if( content != null ) {
      next_action = read_action
      new StompFrame(ascii(action), headers.toList, content)
    } else {
      null
    }
  }

}
