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
import _root_.org.apache.activemq.apollo.broker.{BufferConversions, Destination, Message}
import _root_.org.apache.activemq.filter.{Expression, MessageEvaluationContext}
import _root_.org.apache.activemq.util.buffer._
import collection.mutable.ListBuffer

/**
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
object StompFrameConstants {
  type HeaderMap = List[(AsciiBuffer, AsciiBuffer)]
  type HeaderMapBuffer = ListBuffer[(AsciiBuffer, AsciiBuffer)]
  var NO_DATA = new Buffer(0);
}

import StompFrameConstants._
import StompConstants._;
import BufferConversions._
  
/**
 * Represents all the data in a STOMP frame.
 *
 * @author <a href="http://hiramchirino.com">chirino</a>
 */
case class StompFrame(action:AsciiBuffer, headers:HeaderMap=Nil, content:Buffer=NO_DATA) extends Message {

  def headerSize = {
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
        var rc = 0;
        val i = headers.iterator
        while( i.hasNext ) {
          val (key, value) = i.next
          rc += (key.length + value.length+2)
        }
        rc
      }
    }
  }

  def size = {
     if( action.data eq content.data ) {
        (content.offset-action.offset)+content.length
     } else {
       action.length + 1 +
       headerSize + 1 + content.length
     }
  }

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

  /**
   * used to apply a selector against the message.
   */
  lazy val messageEvaluationContext = new MessageEvaluationContext() {

    def getBodyAs[T](clazz:Class[T]) = {
      throw new UnsupportedOperationException
    }

    def getPropertyExpression(name:String):Expression = {
      throw new UnsupportedOperationException
    }

    @deprecated("this should go away.")
    def getLocalConnectionId() = {
      throw new UnsupportedOperationException
    }

    def getDestination[T]():T = {
      throw new UnsupportedOperationException
    }

    def setDestination(destination:Any):Unit = {
      throw new UnsupportedOperationException
    }
  }

  for( header <- headers ) {
    header match {
      case (Stomp.Headers.Message.MESSAGE_ID, value) =>
        id = value
      case (Stomp.Headers.Message.PRORITY, value) =>
        priority = java.lang.Integer.parseInt(value).toByte
      case (Stomp.Headers.Message.DESTINATION, value) =>
        destination = value
      case (Stomp.Headers.Message.EXPIRATION_TIME, value) =>
        expiration = java.lang.Long.parseLong(value)
      case _ =>
    }
  }
}