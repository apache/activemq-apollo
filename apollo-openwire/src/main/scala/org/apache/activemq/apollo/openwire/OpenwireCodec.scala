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

package org.apache.activemq.apollo.openwire

import org.apache.activemq.apollo.broker.store.MessageRecord
import org.apache.activemq.apollo.transport.ProtocolCodec
import OpenwireConstants._
import java.nio.ByteBuffer
import java.nio.channels.{SocketChannel, WritableByteChannel, ReadableByteChannel}
import java.io.EOFException
import org.fusesource.hawtbuf.{BufferEditor, DataByteArrayOutputStream, Buffer}
import org.apache.activemq.apollo.broker.{Sizer, Message}
import org.apache.activemq.apollo.openwire.codec.OpenWireFormat
import org.apache.activemq.apollo.openwire.command._

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object OpenwireCodec extends Sizer[Command] {
  def encode(message: Message):MessageRecord = {
    throw new UnsupportedOperationException
  }
  def decode(message: MessageRecord) = {
    throw new UnsupportedOperationException
  }

  def size(value: Command) = {
    value match {
      case x:ActiveMQMessage => x.getSize
      case _ => 100
    }
  }
}

class OpenwireCodec extends ProtocolCodec {

  implicit def toBuffer(value:Array[Byte]):Buffer = new Buffer(value)

  def protocol = PROTOCOL

  var write_buffer_size = 1024*64;
  var write_counter = 0L
  var write_channel:WritableByteChannel = null

  var next_write_buffer = new DataByteArrayOutputStream(write_buffer_size)
  var write_buffer = ByteBuffer.allocate(0)

  val format = new OpenWireFormat();


  def full = next_write_buffer.size() >= (write_buffer_size >> 1)
  def is_empty = write_buffer.remaining() == 0

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
        case frame:Command=>
          format.marshal(frame, next_write_buffer)
      }
      if( was_empty ) {
        ProtocolCodec.BufferState.WAS_EMPTY
      } else {
        ProtocolCodec.BufferState.NOT_EMPTY
      }
    }
  }

  def flush():ProtocolCodec.BufferState = {
    // if we have a pending write that is being sent over the socket...
    if ( write_buffer.remaining() != 0 ) {
      write_counter += write_channel.write(write_buffer)
    }

    // if it is now empty try to refill...
    if ( is_empty && next_write_buffer.size()!=0 ) {
        // size of next buffer is based on how much was used in the previous buffer.
        val prev_size = (write_buffer.position()+512).max(512).min(write_buffer_size)
        write_buffer = next_write_buffer.toBuffer().toByteBuffer()
        next_write_buffer = new DataByteArrayOutputStream(prev_size)
    }

    if ( is_empty ) {
      ProtocolCodec.BufferState.EMPTY
    } else {
      ProtocolCodec.BufferState.NOT_EMPTY
    }
  }

  var read_counter = 0L
  var read_buffer_size = 1024*64
  var read_channel:ReadableByteChannel = null

  var read_buffer:ByteBuffer = ByteBuffer.allocate(4)
  var read_waiting_on = 4

  var next_action:()=>Command = read_header

  def setReadableByteChannel(channel: ReadableByteChannel) = {
    this.read_channel = channel
    if( this.read_channel.isInstanceOf[SocketChannel] ) {
      this.read_channel.asInstanceOf[SocketChannel].socket().setReceiveBufferSize(read_buffer_size);
    }
  }

  def unread(buffer: Buffer) = {
    assert(read_counter == 0)
    read_buffer = buffer.toByteBuffer
    read_buffer.position(read_buffer.limit)
    read_counter += buffer.length
    read_waiting_on -= buffer.length
    if ( read_waiting_on <= 0 ) {
      read_buffer.flip
    }
  }

  def getReadCounter = read_counter

  override def read():Object = {

    var command:Object = null
    while( command==null ) {
      // do we need to read in more data???
      if ( read_waiting_on > 0 ) {

        // Try to fill the buffer with data from the socket..
        var p = read_buffer.position()
        var count = read_channel.read(read_buffer)
        if (count == -1) {
            throw new EOFException("Peer disconnected")
        } else if (count == 0) {
            return null
        }
        read_counter += count
        read_waiting_on -= count

        if ( read_waiting_on <= 0 ) {
          read_buffer.flip
        }

      } else {
        command = next_action()
        if ( read_waiting_on > 0 ) {
          val next_buffer = ByteBuffer.allocate(read_buffer.remaining+read_waiting_on)
          next_buffer.put(read_buffer)
          read_buffer = next_buffer
        }
      }
    }
    return command
  }

  def read_header:()=>Command = ()=> {

    read_buffer.mark
    val size = read_buffer.getInt
    read_buffer.reset

    read_waiting_on += (size)

    next_action = read_command(size+4)
    null
  }

  def read_command(size:Int) = ()=> {

    val buf = new Buffer(read_buffer.array, read_buffer.position, size)
    val rc = format.unmarshal(buf)
    read_buffer.position(read_buffer.position+size)

    read_waiting_on += 4
    next_action = read_header
    rc.asInstanceOf[Command]
  }

  def getLastWriteSize = 0

  def getLastReadSize = 0

  def getWriteBufferSize = write_buffer_size

  def getReadBufferSize = read_buffer_size
}
