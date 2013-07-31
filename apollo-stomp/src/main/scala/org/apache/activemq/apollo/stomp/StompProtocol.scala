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

import _root_.org.fusesource.hawtbuf._
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.broker.protocol.{MessageCodecFactory, MessageCodec, ProtocolCodecFactory, Protocol}
import Stomp._
import org.apache.activemq.apollo.broker.store._
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
/**
 * Creates StompCodec objects that encode/decode the
 * <a href="http://activemq.apache.org/stomp/">Stomp</a> protocol.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class StompProtocolCodecFactory extends ProtocolCodecFactory.Provider {

  def id = PROTOCOL

  def createProtocolCodec(connector:Connector) = new StompCodec();

  def isIdentifiable() = true

  def maxIdentificaionLength() = CONNECT.length;

  def matchesIdentification(header: Buffer):Boolean = {
    if (header.length < CONNECT.length) {
      false
    } else {
      header.startsWith(CONNECT) || header.startsWith(STOMP)
    }
  }
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object StompProtocol extends StompProtocolCodecFactory with Protocol {
  def createProtocolHandler(connector:Connector) = new StompProtocolHandler
}

object StompMessageCodecFactory extends MessageCodecFactory.Provider {
  def create = Array[MessageCodec](StompMessageCodec)
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object StompMessageCodec extends MessageCodec{

  def id = "stomp"

  def encode(message: Message):MessageRecord = {
    StompCodec.encode(message.asInstanceOf[StompFrameMessage])
  }

  def decode(message: MessageRecord) = {
    StompCodec.decode(message)
  }

}




