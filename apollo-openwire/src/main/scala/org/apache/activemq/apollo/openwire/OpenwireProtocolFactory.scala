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
import org.apache.activemq.apollo.broker.Message
import OpenwireConstants._
import org.apache.activemq.apollo.broker.protocol.{ProtocolCodecFactory, Protocol, ProtocolFactory}
import org.fusesource.hawtbuf.Buffer
import org.apache.activemq.apollo.util.Log

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object OpenwireProtocolFactory extends ProtocolFactory {

  def create() = OpenwireProtocol

  def create(config: String) = if(config == PROTOCOL) {
    OpenwireProtocol
  } else {
    null
  }

}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object OpenwireProtocol extends OpenwireProtocolCodecFactory with Protocol with Log {

  def createProtocolHandler = new OpenwireProtocolHandler

  def encode(message: Message):MessageRecord = {
    OpenwireCodec.encode(message)
  }

  def decode(message: MessageRecord) = {
    OpenwireCodec.decode(message)
  }

  lazy val log_exerimental_warning = {
    warn("The OpenWire protocol implementation is still experimental and not recommended for production use.  Production users should use ActiveMQ instead.")
    null
  }
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class OpenwireProtocolCodecFactory extends ProtocolCodecFactory.Provider {


  def id = PROTOCOL

  def createProtocolCodec() = {
    OpenwireProtocol.log_exerimental_warning
    new OpenwireCodec();
  }

  def isIdentifiable() = true

  def maxIdentificaionLength() = 5 + MAGIC.length

  def matchesIdentification(buffer: Buffer):Boolean = {
    buffer.length >= 4 + MAGIC.length && buffer.containsAt(MAGIC, 5)
  }
}
