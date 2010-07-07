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
package org.apache.activemq.apollo.stomp

import _root_.java.util.LinkedList
import _root_.org.apache.activemq.filter.{Expression, Filterable}
import _root_.org.fusesource.hawtbuf._
import collection.mutable.ListBuffer
import org.apache.activemq.apollo.broker.{Sizer, Destination, BufferConversions, Message}
import java.lang.{String, Class}

/**
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
object StompFrameConstants {
  type HeaderMap = List[(AsciiBuffer, AsciiBuffer)]
  type HeaderMapBuffer = ListBuffer[(AsciiBuffer, AsciiBuffer)]
  val NO_DATA = new Buffer(0);

}

import StompFrameConstants._
import StompConstants._;
import BufferConversions._
import Buffer._

case class StompFrameMessage(frame:StompFrame) extends Message {
  
  def protocol = PROTOCOL

  /**
   * the globally unique id of the message
   */
  var id: AsciiBuffer = null

  /**
   * the globally unique id of the producer
   */
  var producer: AsciiBuffer = null

  /**
   *  the message priority.
   */
  var priority:Byte = 4;

  /**
   * a positive value indicates that the delivery has an expiration
   * time.
   */
  var expiration: Long = -1;

  /**
   * true if the delivery is persistent
   */
  var persistent = false

  /**
   * where the message was sent to.
   */
  var destination: Destination = null

  for( header <- (frame.updated_headers ::: frame.headers).reverse ) {
    header match {
      case (Stomp.Headers.Message.MESSAGE_ID, value) =>
        id = value
      case (Stomp.Headers.Send.PRIORITY, value) =>
        priority = java.lang.Integer.parseInt(value).toByte
      case (Stomp.Headers.Send.DESTINATION, value) =>
        destination = value
      case (Stomp.Headers.Send.EXPIRATION_TIME, value) =>
        expiration = java.lang.Long.parseLong(value)
      case (Stomp.Headers.Send.PERSISTENT, value) =>
        persistent = java.lang.Boolean.parseBoolean(value)
      case _ =>
    }
  }

  def getBodyAs[T](toType : Class[T]) = {
    (if( toType == classOf[String] ) {
      frame.content.utf8
    } else if (toType == classOf[Buffer]) {
      frame.content
    } else if (toType == classOf[AsciiBuffer]) {
      frame.content.ascii
    } else if (toType == classOf[UTF8Buffer]) {
      frame.content.utf8
    } else {
      null
    }).asInstanceOf[T]
  }

  def getLocalConnectionId = {
    val pos = id.indexOf(':')
    assert(pos >0 )
    id.slice(id.offset, pos).toString
  }

  /* avoid paying the price of creating the header index. lots of times we don't need it */
  lazy val headerIndex: Map[AsciiBuffer, AsciiBuffer] =  {
    var rc = Map[AsciiBuffer, AsciiBuffer]()
    for( header <- (frame.updated_headers ::: frame.headers).reverse ) {
      rc += (header._1 -> header._2)
    }
    rc
  }

  def getProperty(name: String):AnyRef = {
    (name match {
      // TODO: handle more of the JMS Types that ActiveMQ 5 supports.
      case "JMSMessageID" =>
        Some(id)
      case "JMSType" =>
        headerIndex.get(ascii("type"))
      case _=>
        headerIndex.get(ascii(name))
    }) match {
      case Some(rc) => rc.utf8.toString
      case None => null
    }
  }

}



object StompFrame extends Sizer[StompFrame] {
  def size(value:StompFrame) = value.size   
}

/**
 * Represents all the data in a STOMP frame.
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
case class StompFrame(action:AsciiBuffer, headers:HeaderMap=Nil, content:Buffer=NO_DATA, updated_headers:HeaderMap=Nil) {

  def size_of_updated_headers = {
    size_of(updated_headers)
  }

  def size_of_original_headers = {
    if( headers.isEmpty ) {
      0
    } else {
      // if all the headers were part of the same input buffer.. size can be calculated by
      // subtracting positions in the buffer.
      val firstBuffer = headers.head._1
      val lastBuffer =  headers.last._2
      if( firstBuffer.data eq lastBuffer.data ) {
        (lastBuffer.offset-firstBuffer.offset)+lastBuffer.length+1
      } else {
        // gota do it the hard way
        size_of(headers)
      }
    }
  }

  private def size_of(headers:HeaderMap): Int = {
    var rc = 0;
    val i = headers.iterator
    while (i.hasNext) {
      val (key, value) = i.next
      rc += (key.length + value.length + 2)
    }
    rc
  }

  def size = {
     if( (action.data eq content.data) && updated_headers==Nil ) {
        (content.offset-action.offset)+content.length
     } else {
       action.length + 1 +
       size_of_updated_headers +
       size_of_original_headers + 1 + content.length
     }
  }

  def header(name:AsciiBuffer) = {
    updated_headers.filter( _._1 == name ).headOption.orElse(
      headers.filter( _._1 == name ).headOption
    ).map(_._2).getOrElse(null)
  }

}
