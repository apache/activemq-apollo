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
import org.fusesource.hawtbuf.{UTF8Buffer, AsciiBuffer, Buffer}
import command.{ActiveMQBytesMessage, ActiveMQTextMessage, ActiveMQMessage}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class OpenwireMessage(val message:ActiveMQMessage) extends BaseRetained with Message {

  val _id = ascii(message.getMessageId.toString)

  def toString(buffer:AnyRef) = if (buffer==null) null else buffer.toString

  override def message_group: String = if(message.getGroupID!=null ) message.getGroupID.toString else null

  def getProperty(name: String) = {
    name match {
      case "JMSDeliveryMode" =>
        if( message.isPersistent) "PERSISTENT" else "NON_PERSISTENT"
      case "JMSPriority" =>
        new java.lang.Integer(message.getPriority)
      case "JMSType" =>
        toString(message.getType)
      case "JMSMessageID" =>
        toString(message.getMessageId)
      case "JMSDestination" =>
        toString(message.getDestination)
      case "JMSReplyTo" =>
        toString(message.getReplyTo)
      case "JMSCorrelationID" =>
        toString(message.getCorrelationId)
      case "JMSExpiration" =>
        new java.lang.Long(message.getExpiration)
      case "JMSXDeliveryCount" =>
        new java.lang.Integer(message.getRedeliveryCounter)
      case "JMSXUserID" =>
        toString(message.getUserID)
      case "JMSXGroupID" =>
        toString(message.getGroupID)
      case "JMSXGroupSeq" =>
        if ( message.getGroupID!=null ) {
          new java.lang.Integer(message.getGroupSequence)
        } else {
          null
        }
      case x =>
        message.getProperty(name)
    }
  }


  override def headers_as_json: java.util.HashMap[String, Object] = {
    val rc = new java.util.HashMap[String, Object]()
    import collection.JavaConversions._

    def fillin(name:String) = {
      val t = getProperty(name)
      if ( t !=null ) {
        rc.put(name, t.asInstanceOf[Object])
      }
    }

    fillin("JMSDeliveryMode")
    fillin("JMSPriority")
    fillin("JMSType")
    fillin("JMSMessageID")
    fillin("JMSDestination")
    fillin("JMSReplyTo")
    fillin("JMSCorrelationID")
    fillin("JMSExpiration")
    fillin("JMSXDeliveryCount")
    fillin("JMSXUserID")
    fillin("JMSXGroupID")
    fillin("JMSXGroupSeq")

    for( (x,y) <- message.getProperties ) {
      rc.put(x,y)
    }
    rc
  }

  def getLocalConnectionId = message.getProducerId.getConnectionId

  def codec = OpenwireMessageCodec

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
  def codec = null
  def priority: Byte = 0
  def persistent: Boolean = false
  def expiration: Long = 0L
  def getProperty(name: String): AnyRef = null
  def getLocalConnectionId: AnyRef = null
  def getBodyAs[T](toType : Class[T]) = null.asInstanceOf[T]
}