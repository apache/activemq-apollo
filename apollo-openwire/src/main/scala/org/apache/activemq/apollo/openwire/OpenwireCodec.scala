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
import OpenwireConstants._
import org.apache.activemq.apollo.openwire.codec.OpenWireFormat
import org.apache.activemq.apollo.openwire.command._
import org.apache.activemq.apollo.broker.BufferConversions._
import org.fusesource.hawtdispatch.transport.AbstractProtocolCodec
import org.fusesource.hawtbuf._
import org.apache.activemq.apollo.broker.{Broker, Sizer, Message}

case class CachedEncoding(tight:Boolean, version:Int, buffer:Buffer) extends CachedEncodingTrait

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object OpenwireCodec extends Sizer[Command] {

  final val DB_VERSION = OpenWireFormat.DEFAULT_VERSION
  final val DB_TIGHT_ENCODING = false

  def encode(message: Message):MessageRecord = {
    val rc = new MessageRecord
    rc.codec = PROTOCOL

    val msg = message.asInstanceOf[OpenwireMessage];
    rc.buffer = msg.message.getCachedEncoding match {
      case CachedEncoding(tight, version, buffer) =>

        val boas = new DataByteArrayOutputStream(
          1 +
          AbstractVarIntSupport.computeVarIntSize(version)+
          buffer.length()
        )

        boas.writeBoolean(tight)
        boas.writeVarInt(version)
        boas.write(buffer)
        boas.toBuffer

      case _ =>

        val db_format = new OpenWireFormat();
        db_format.setCacheEnabled(false)
        db_format.setTightEncodingEnabled(DB_TIGHT_ENCODING)
        db_format.setVersion(DB_VERSION)

        val size = msg.message.getEncodedSize
        val boas = new DataByteArrayOutputStream(if(size==0) 1024 else size + 20)
        boas.writeBoolean(DB_TIGHT_ENCODING)
        boas.writeVarInt(DB_VERSION)
        db_format.marshal(msg.message, boas);
        boas.toBuffer

    }
    rc
  }
  
  def decode(message: MessageRecord) = {
    val buffer = message.buffer.buffer();
    val bais = new DataByteArrayInputStream(message.buffer)
    var tight: Boolean = bais.readBoolean()
    var version: Int = bais.readVarInt()
    buffer.moveHead(bais.getPos-buffer.offset)

    val db_format = new OpenWireFormat();
    db_format.setCacheEnabled(false)
    db_format.setTightEncodingEnabled(tight)
    db_format.setVersion(version)

    val msg = db_format.unmarshal(bais).asInstanceOf[ActiveMQMessage]
    msg.setEncodedSize(buffer.length)
    msg.setCachedEncoding(CachedEncoding(tight, version, buffer))
    new OpenwireMessage(msg)
  }

  def size(value: Command) = {
    value match {
      case x:ActiveMQMessage => x.getSize
      case _ => 100
    }
  }
}

class OpenwireCodec extends AbstractProtocolCodec {

  this.bufferPools = Broker.buffer_pools
  val format = new OpenWireFormat(1);

  protected def encode(command: AnyRef) = {
    format.marshal(command, nextWriteBuffer)
  }

  private final val readHeader:AbstractProtocolCodec.Action = new AbstractProtocolCodec.Action {
    def apply = {
      val header = peekBytes(4)
      if( header==null ) {
        null
      } else {
        val length = header.bigEndianEditor().readInt()
        nextDecodeAction = new AbstractProtocolCodec.Action {
          def apply() = {
            val frame = readBytes(4+length)
            if( frame==null ) {
              null
            } else {
              val command = format.unmarshal(frame)
              nextDecodeAction = readHeader
              // If value caching is NOT enabled, then we potentially re-use the encode
              // value of the message.
              command match {
                case message:ActiveMQMessage =>
                  message.setEncodedSize(length)
                  if( !format.isCacheEnabled ) {
                    message.setCachedEncoding(CachedEncoding(format.isTightEncodingEnabled, format.getVersion, frame))
                  }
                case _ =>
              }
              command
            }
          }
        }
        nextDecodeAction.apply()
      }
    }
  }
  
  protected def initialDecodeAction = readHeader
}
