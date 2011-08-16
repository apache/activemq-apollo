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

import org.apache.activemq.apollo.broker.Message
import java.lang.{String, Class}
import org.fusesource.hawtdispatch.BaseRetained
import org.fusesource.hawtbuf.Buffer._
import OpenwireConstants._
import org.fusesource.hawtbuf.{UTF8Buffer, AsciiBuffer, Buffer}
import command.{ActiveMQBytesMessage, ActiveMQTextMessage, ActiveMQMessage}
import org.apache.activemq.apollo.broker.protocol.Protocol

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class OpenwireMessage(val message:ActiveMQMessage) extends BaseRetained with Message {

  val _id = ascii(message.getMessageId.toString)

  def getProperty(name: String) = message.getProperty(name)

  def getLocalConnectionId = message.getProducerId.getConnectionId

  def protocol = OpenwireProtocol

  def producer = ascii(message.getProducerId.toString)

  def priority = message.getPriority

  def persistent = message.isPersistent

  def id = _id

  def expiration = message.getExpiration

  def getBodyAs[T](toType : Class[T]) = {
    (message match {
      case x:ActiveMQTextMessage =>
        if( toType == classOf[String] ) {
          x.getText
        } else if (toType == classOf[Buffer]) {
          utf8(x.getText)
        } else if (toType == classOf[AsciiBuffer]) {
          ascii(x.getText)
        } else if (toType == classOf[UTF8Buffer]) {
          utf8(x.getText)
        } else {
          null
        }
      case x:ActiveMQBytesMessage =>
        null
      case x:ActiveMQMessage =>
        if( toType == classOf[String] ) {
          ""
        } else if (toType == classOf[Buffer]) {
          ascii("")
        } else if (toType == classOf[AsciiBuffer]) {
          ascii("")
        } else if (toType == classOf[UTF8Buffer]) {
          utf8("")
        } else {
          null
        }
    }).asInstanceOf[T]
  }

}

object EndOfBrowseMessage extends Message {
  def retained(): Int = 0
  def retain() {}
  def release() {}
  def protocol: Protocol = null
  def producer: AsciiBuffer = null
  def priority: Byte = 0
  def persistent: Boolean = false
  def id: AsciiBuffer = null
  def expiration: Long = 0L
  def getProperty(name: String): AnyRef = null
  def getLocalConnectionId: AnyRef = null
  def getBodyAs[T](toType : Class[T]) = null.asInstanceOf[T]
}