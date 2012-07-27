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
package org.apache.activemq.apollo.broker.protocol

import java.nio.ByteBuffer
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.broker._
import java.lang.{Class, String}
import org.apache.activemq.apollo.broker.store.MessageRecord
import org.fusesource.hawtbuf.{AsciiBuffer, Buffer}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class RawProtocolFactory extends ProtocolFactory {
  def create() = RawProtocol
  def create(config: String): Protocol = {
    config match {
      case "raw" => RawProtocol
      case _ => null
    }
  }
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object RawProtocol extends Protocol {
  
  val PROTOCOL_ID = new AsciiBuffer(id)
  def id = "raw"

  def encode(message: Message):MessageRecord = {
    message match {
      case message:RawMessage =>
        val rc = new MessageRecord
        rc.protocol = PROTOCOL_ID
        rc.buffer = message.payload
        rc
      case _ => throw new RuntimeException("Invalid message type");
    }
  }

  def decode(message: MessageRecord) = {
    assert( message.protocol == PROTOCOL_ID )
    RawMessage(message.buffer)
  }

  def createProtocolCodec = throw new UnsupportedOperationException()
  def createProtocolHandler = throw new UnsupportedOperationException()
  def isIdentifiable = false
  def maxIdentificaionLength = throw new UnsupportedOperationException()
  def matchesIdentification(buffer: Buffer) = throw new UnsupportedOperationException()
}

case class RawMessage(payload:Buffer) extends Message {

  def getBodyAs[T](toType : Class[T]) = {
    if( toType.isAssignableFrom(classOf[Buffer]) ) {
      toType.cast(payload)
    } else if( toType == classOf[Array[Byte]] ) {
      toType.cast(payload.toByteArray)
    } else if( toType == classOf[ByteBuffer] ) {
      toType.cast(payload.toByteBuffer)
    } else {
      null.asInstanceOf[T]
    }
  }

  def getLocalConnectionId = null
  def getProperty(name: String) = null
  def expiration = 0L
  def persistent = false
  def priority = 0
  def protocol = RawProtocol
  def release() {}
  def retain() {}
  def retained() = 0
}