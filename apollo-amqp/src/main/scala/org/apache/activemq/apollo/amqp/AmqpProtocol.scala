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
package org.apache.activemq.apollo.amqp

import _root_.org.fusesource.hawtbuf._
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.broker.protocol.Protocol
import org.apache.qpid.proton.hawtdispatch.impl.AmqpProtocolCodec

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object AmqpProtocol extends Protocol {

  def id = "amqp"
  val PROTOCOL_ID = Buffer.ascii(id)
  val PROTOCOL_MAGIC = new Buffer(Array[Byte]('A', 'M', 'Q', 'P'))

  def createProtocolCodec(connector:Connector) = new AmqpProtocolCodec();

  def isIdentifiable() = true

  def maxIdentificaionLength() = PROTOCOL_MAGIC.length;

  def matchesIdentification(header: Buffer):Boolean = {
    if (header.length < PROTOCOL_MAGIC.length) {
      false
    } else {
      header.startsWith(PROTOCOL_MAGIC)
    }
  }

  def createProtocolHandler(connector:Connector) = new AmqpProtocolHandler
}

//object AmqpMessageCodecFactory extends MessageCodecFactory.Provider {
//  def create = Array[MessageCodec](AmqpMessageCodec)
//}
//
//  /**
// * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
// */
//object AmqpMessageCodec extends MessageCodec {
//  def id = AmqpProtocol.id
//  def encode(message: Message) = AmqpCodec.encode(message)
//  def decode(message: MessageRecord) = AmqpCodec.decode(message)
//}


