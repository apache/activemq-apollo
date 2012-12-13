/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.amqp


import org.apache.activemq.apollo.broker.protocol
import protocol.{MessageCodecFactory, MessageCodec}
import java.nio.ByteBuffer
import org.apache.qpid.proton.codec.{WritableBuffer, CompositeWritableBuffer}
import org.fusesource.hawtbuf.Buffer._
import org.apache.activemq.apollo.broker.Message
import org.apache.activemq.apollo.broker.store.MessageRecord
import org.fusesource.hawtbuf.Buffer
import org.fusesource.hawtbuf.AsciiBuffer
import org.fusesource.hawtbuf.UTF8Buffer
import org.apache.qpid.proton.hawtdispatch.impl.DroppingWritableBuffer
import org.apache.qpid.proton.`type`.{UnsignedLong, UnsignedInteger}
import org.apache.qpid.proton.`type`.messaging.{Properties, Header}
import org.apache.activemq.apollo.filter.Filterable

object AmqpMessageCodecFactory extends MessageCodecFactory.Provider {
  def create = Array[MessageCodec](AmqpMessageCodec)
}

object AmqpMessageCodec extends MessageCodec {

  def ascii_id = ascii("amqp-1.0")
  def id = "amqp-1.0"

  def encode(message: Message):MessageRecord = {
    val rc = new MessageRecord
    rc.codec = ascii_id
    rc.buffer = message.encoded
    rc
  }

  def decode(message: MessageRecord) = {
    assert( message.codec == ascii_id )
    new AmqpMessage(message.buffer, null)
  }

}


object AmqpMessage {
  val SENDER_CONTAINER_KEY = "sender-container"

  val prefixVendor = "JMS_AMQP_";
  val prefixDeliveryAnnotationsKey = prefixVendor+"DA_";
  val prefixMessageAnnotationsKey= prefixVendor+"MA_";
  val prefixFooterKey = prefixVendor+"FT_";

  val firstAcquirerKey = prefixVendor + "FirstAcquirer";
  val subjectKey =  prefixVendor +"Subject";
  val contentTypeKey = prefixVendor +"ContentType";
  val contentEncodingKey = prefixVendor +"ContentEncoding";
  val replyToGroupIDKey = prefixVendor +"ReplyToGroupID";

}
import AmqpMessage._

class AmqpMessage(private var encoded_buffer:Buffer, private var decoded_message:org.apache.qpid.proton.message.Message=null) extends org.apache.activemq.apollo.broker.Message {

  /**
   * The encoder/decoder of the message
   */
  def codec = AmqpMessageCodec

  def decoded = {
    if( decoded_message==null ) {
      val amqp = new org.apache.qpid.proton.message.Message();
      var offset = encoded_buffer.offset
      var len = encoded_buffer.length
      while( len > 0 ) {
          var decoded = amqp.decode(encoded_buffer.data, offset, len);
          assert(decoded > 0, "Make progress decoding the message")
          offset += decoded;
          len -= decoded;
      }
      decoded_message = amqp

    }
    decoded_message
  }

  override def encoded = {
    if( encoded_buffer == null ) {
      var buffer = ByteBuffer.wrap(new Array[Byte](1024*4));
      val overflow = new DroppingWritableBuffer();
      var c = decoded_message.encode(new CompositeWritableBuffer(new WritableBuffer.ByteBufferWrapper(buffer), overflow));
      if( overflow.position() > 0 ) {
          buffer = ByteBuffer.wrap(new Array[Byte](1024*4+overflow.position()));
          c = decoded_message.encode(new WritableBuffer.ByteBufferWrapper(buffer));
      }
      encoded_buffer = new Buffer(buffer.array(), 0, c)
    }
    encoded_buffer
  }

  def getBodyAs[T](toType : Class[T]): T = {
    if (toType == classOf[Buffer]) {
      encoded
    } else if( toType == classOf[String] ) {
      encoded.utf8
    } else if (toType == classOf[AsciiBuffer]) {
      encoded.ascii
    } else if (toType == classOf[UTF8Buffer]) {
      encoded.utf8
    } else {
      null
    }
  }.asInstanceOf[T]

  def getLocalConnectionId: AnyRef = {
    if ( decoded.getDeliveryAnnotations!=null ) {
      decoded.getDeliveryAnnotations.getValue.get(SENDER_CONTAINER_KEY) match {
        case x:String => x
        case _ => null
      }
    } else {
      null
    }
  }

  def getApplicationProperty(name:String) = {
    if( decoded.getApplicationProperties !=null ) {
      decoded.getApplicationProperties.getValue.get(name).asInstanceOf[AnyRef]
    } else {
      null
    }
  }

  def getMessageAnnotationProperty(name:String) = {
    if( decoded.getMessageAnnotations !=null ) {
      var ma = decoded.getMessageAnnotations
      var rc = ma.getValue.get(name)
      if( rc == null ) {
        rc = ma.getValue.get(org.apache.qpid.proton.`type`.Symbol.valueOf(name))
      }
      rc.asInstanceOf[AnyRef]
    } else {
      null
    }
  }

  def getDeliveryAnnotationProperty(name:String) = {
    if( decoded.getDeliveryAnnotations !=null ) {
      decoded.getDeliveryAnnotations.getValue.get(name).asInstanceOf[AnyRef]
    } else {
      null
    }
  }
  def getFooterProperty(name:AnyRef) = {
    if( decoded.getFooter !=null ) {
      decoded.getFooter.getValue.get(name).asInstanceOf[AnyRef]
    } else {
      null
    }
  }

  def getHeader[T](default:T)(func: (Header)=>T) = {
    if( decoded.getHeader == null ) {
      default
    } else {
      func(decoded.getHeader)
    }
  }
  def getProperties[T](default:T)(func: (Properties)=>T) = {
    if( decoded.getProperties == null ) {
      default
    } else {
      func(decoded.getProperties)
    }
  }

//  object JMSFilterable extends Filterable {
//    def getBodyAs[T](kind: Class[T]): T = AmqpMessage.this.getBodyAs(kind)
//    def getLocalConnectionId: AnyRef = AmqpMessage.this.getLocalConnectionId
//    def getProperty(name: String) = {
//    }
//  }

  def getProperty(name: String) = {
    val rc:AnyRef = (name match {
      case "JMSDeliveryMode" =>
        getHeader[AnyRef](null)(header=> if(header.getDurable) "PERSISTENT" else "NON_PERSISTENT" )
      case "JMSPriority" =>
        new java.lang.Integer(decoded.getPriority)
      case "JMSType" =>
        getMessageAnnotationProperty("x-opt-jms-type")
      case "JMSMessageID" =>
        getProperties[AnyRef](null)(_.getMessageId)
      case "JMSDestination" =>
        getProperties[String](null)(_.getTo)
      case "JMSReplyTo" =>
        getProperties[String](null)(_.getReplyTo)
      case "JMSCorrelationID" =>
        getProperties[AnyRef](null)(_.getCorrelationId)
//      case "JMSExpiration" =>
//        new java.lang.Long(decoded.getTtl)
      case "JMSExpiration" =>
        getProperties[AnyRef](null)(x=> Option(x.getAbsoluteExpiryTime()).map(y=> new java.lang.Long(y.getTime)).getOrElse(null))
      case "JMSXDeliveryCount" =>
        getHeader[AnyRef](null)(_.getDeliveryCount)
      case "JMSXUserID" =>
        getProperties[AnyRef](null)(_.getUserId)
      case "JMSXGroupID" =>
        getProperties[AnyRef](null)(_.getGroupId)
      case "JMSXGroupSeq" =>
        getProperties[AnyRef](null)(_.getGroupSequence)
      case x if x == firstAcquirerKey =>
        getHeader[AnyRef](null)(_.getFirstAcquirer)
      case x if x == subjectKey =>
        getProperties[AnyRef](null)(_.getSubject)
      case x if x == contentTypeKey =>
        getProperties[AnyRef](null)(_.getContentType)
      case x if x == contentEncodingKey =>
        getProperties[AnyRef](null)(_.getContentEncoding)
      case x if x == replyToGroupIDKey =>
        getProperties[AnyRef](null)(_.getReplyToGroupId)
      case x if x.startsWith(prefixDeliveryAnnotationsKey) =>
        getDeliveryAnnotationProperty(x)
      case x if x.startsWith(prefixMessageAnnotationsKey)  =>
        getMessageAnnotationProperty(x)
      case x if x.startsWith(prefixFooterKey)  =>
        getFooterProperty(x)
      case x =>
        getApplicationProperty(x)
    }) match {
      case x:UnsignedInteger => new java.lang.Long(x.longValue());
      case x:UnsignedLong => new java.lang.Long(x.longValue());
      case x => x
    }
    rc
  }

  def release() {}
  def retain() {}
  def retained(): Int = 0
}
