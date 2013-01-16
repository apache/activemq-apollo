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
import org.fusesource.hawtbuf.Buffer._
import org.apache.activemq.apollo.broker.Message
import org.apache.activemq.apollo.broker.store.MessageRecord
import org.fusesource.hawtbuf.Buffer
import org.fusesource.hawtbuf.AsciiBuffer
import org.fusesource.hawtbuf.UTF8Buffer
import org.apache.qpid.proton.amqp.{UnsignedByte, UnsignedShort, UnsignedLong, UnsignedInteger}
import org.apache.qpid.proton.amqp.messaging.{Properties, Header}
import org.apache.qpid.proton.message.impl.MessageImpl

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
      val amqp = new MessageImpl();
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
      var buffer = new Array[Byte](1024);
      var c = decoded_message.asInstanceOf[MessageImpl].encode2(buffer, 0, buffer.length);
      if( c >  buffer.length) {
        buffer = new Array[Byte](c);
        decoded_message.encode(buffer, 0, c);
      }
      encoded_buffer = new Buffer(buffer, 0, c)
    }
    encoded_buffer
  }


  override def message_group = decoded.getGroupId

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
        rc = ma.getValue.get(org.apache.qpid.proton.amqp.Symbol.valueOf(name))
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


  override def headers_as_json: java.util.HashMap[String, Object] = {
    val rc = new java.util.HashMap[String, Object]()
    import collection.JavaConversions._

    def convert(v: AnyRef) = (v match {
      case v: UnsignedByte => new java.lang.Integer(v.shortValue())
      case v: UnsignedShort => new java.lang.Integer(v.intValue())
      case v: UnsignedInteger => new java.lang.Long(v.longValue())
      case v: UnsignedLong => new java.lang.Long(v.longValue())
      case _ => v
    })

    if ( decoded.getHeader!=null ) {
      val header = decoded.getHeader
      if ( header.getDeliveryCount !=null ) {
        rc.put("header.delivery_count", new java.lang.Long(header.getDeliveryCount.longValue()))
      }
      if ( header.getDurable !=null ) {
        rc.put("header.durable", new java.lang.Boolean(header.getDurable.booleanValue()))
      }
      if ( header.getFirstAcquirer !=null ) {
        rc.put("header.first_acquirer", new java.lang.Boolean(header.getFirstAcquirer.booleanValue()))
      }
      if ( header.getPriority !=null ) {
        rc.put("header.priority", new java.lang.Integer(header.getPriority.intValue()))
      }
      if ( header.getTtl !=null ) {
        rc.put("header.ttl", new java.lang.Long(header.getTtl.longValue()))
      }
    }

    if( decoded.getProperties != null ) {
      val properties = decoded.getProperties
      if ( properties.getAbsoluteExpiryTime !=null ) {
        rc.put("property.absolute_expiry_time", new java.lang.Long(properties.getAbsoluteExpiryTime.getTime()))
      }
      if ( properties.getContentEncoding !=null ) {
        rc.put("property.content_encoding", properties.getContentEncoding.toString)
      }
      if ( properties.getContentType !=null ) {
        rc.put("property.content_type", properties.getContentType.toString)
      }
      if ( properties.getCorrelationId !=null ) {
        rc.put("property.correlation_id", properties.getCorrelationId.toString)
      }
      if ( properties.getCreationTime !=null ) {
        rc.put("property.creation_time",  new java.lang.Long(properties.getCreationTime.getTime))
      }
      if ( properties.getGroupId !=null ) {
        rc.put("property.group_id", properties.getGroupId)
      }
      if ( properties.getGroupSequence !=null ) {
        rc.put("property.group_sequence", new java.lang.Long(properties.getGroupSequence.longValue()))
      }
      if ( properties.getMessageId !=null ) {
        rc.put("property.message_id", properties.getMessageId)
      }
      if ( properties.getReplyTo !=null ) {
        rc.put("property.reply_to", properties.getReplyTo)
      }
      if ( properties.getReplyToGroupId !=null ) {
        rc.put("property.reply_to_group_id", properties.getReplyToGroupId)
      }
      if ( properties.getSubject !=null ) {
        rc.put("property.subject", properties.getSubject)
      }
      if ( properties.getTo !=null ) {
        rc.put("property.to", properties.getTo)
      }
      if ( properties.getUserId !=null ) {
        rc.put("property.user_id", properties.getUserId.toString)
      }
    }

    if( decoded.getDeliveryAnnotations !=null ) {
      val annotations = decoded.getDeliveryAnnotations
      for( (k,v:AnyRef) <- annotations.getValue ) {
        rc.put("annotation."+k, convert(v))
      }
    }

    if( decoded.getApplicationProperties !=null ) {
      val properties = decoded.getApplicationProperties
      for( (k,v:AnyRef) <- properties.getValue ) {
        rc.put("app."+k, convert(v))
      }
    }

    if( decoded.getFooter !=null ) {
      val footer = decoded.getFooter
      for( (k,v:AnyRef) <- footer.getValue ) {
        rc.put("footer."+k, convert(v))
      }
    }
    rc
  }

  def release() {}
  def retain() {}
  def retained(): Int = 0
}
