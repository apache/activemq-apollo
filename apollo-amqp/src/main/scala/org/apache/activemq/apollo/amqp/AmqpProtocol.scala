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
import org.apache.activemq.apollo.broker.protocol.{MessageCodecFactory, MessageCodec, ProtocolCodecFactory, Protocol}
import org.apache.activemq.apollo.broker.store._
import AmqpCodec._
import org.fusesource.amqp.codec.AMQPProtocolCodec

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
/**
 * Creates AmqpCodec objects that encode/decode the
 * <a href="http://activemq.apache.org/amqp/">Amqp</a> protocol.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class AmqpProtocolCodecFactory extends ProtocolCodecFactory.Provider {
  def id = PROTOCOL

  def createProtocolCodec(connector:Connector) = new AMQPProtocolCodec();

  def isIdentifiable() = true

  def maxIdentificaionLength() = PROTOCOL_MAGIC.length;

  def matchesIdentification(header: Buffer):Boolean = {
    if (header.length < PROTOCOL_MAGIC.length) {
      false
    } else {
      header.startsWith(PROTOCOL_MAGIC)
    }
  }
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object AmqpProtocol extends AmqpProtocolCodecFactory with Protocol {
  def createProtocolHandler = new AmqpProtocolHandler
}

object AmqpMessageCodecFactory extends MessageCodecFactory.Provider {
  def create = Array[MessageCodec](AmqpMessageCodec)
}

  /**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object AmqpMessageCodec extends MessageCodec {
  def id = PROTOCOL
  def encode(message: Message) = AmqpCodec.encode(message)
  def decode(message: MessageRecord) = AmqpCodec.decode(message)
}


