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
package org.apache.activemq.apollo.broker

import _root_.java.lang.{String}
import _root_.org.fusesource.hawtdispatch._
import org.fusesource.hawtbuf._
import protocol.Protocol
import org.apache.activemq.apollo.filter.Filterable
import org.apache.activemq.apollo.store.{StoreUOW, MessageRecord}

/**
 * A producer which sends Delivery objects to a delivery consumer.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DeliveryProducer {

  def dispatchQueue:DispatchQueue

  def connection:Option[BrokerConnection] = None

  def collocate(value:DispatchQueue):Unit = {
    if( value.getTargetQueue ne dispatchQueue.getTargetQueue ) {
      println(dispatchQueue.getLabel+" co-locating with: "+value.getLabel);
      this.dispatchQueue.setTargetQueue(value.getTargetQueue)
    }
  }

}

/**
 * The delivery consumer accepts messages from a delivery producer.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DeliveryConsumer extends Retained {

  def connection:Option[BrokerConnection] = None

  def browser = false
  def dispatchQueue:DispatchQueue;
  def matches(message:Delivery):Boolean
  def connect(producer:DeliveryProducer):DeliverySession
  def is_persistent:Boolean
}

/**
 * Before a delivery producer can send Delivery objects to a delivery
 * consumer, it creates a Delivery session which it uses to send
 * the deliveries over.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DeliverySession extends Sink[Delivery] {
  def producer:DeliveryProducer
  def consumer:DeliveryConsumer
  def close:Unit
}


/**
 * Abstracts wire protocol message implementations.  Each wire protocol
 * will provide it's own type of Message.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Message extends Filterable with Retained {

  /**
   * the globally unique id of the message
   */
  def id: AsciiBuffer

  /**
   * the globally unique id of the producer
   */
  def producer: AsciiBuffer

  /**
   *  the message priority.
   */
  def priority:Byte

  /**
   * a positive value indicates that the delivery has an expiration
   * time.
   */
  def expiration: Long

  /**
   * true if the delivery is persistent
   */
  def persistent: Boolean

  /**
   * where the message was sent to.
   */
  def destination: Destination

  /**
   * The protocol of the message
   */
  def protocol:Protocol

}

/**
 * <p>
 * A new Delivery object is created every time a message is transfered between a producer and
 * it's consumer or consumers.  Consumers will retain the object to flow control the producer.
 * </p>
 * <p>
 * Once this object is disposed, the producer is free to send more deliveries to the consumers.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Delivery extends Sizer[Delivery] {
  def size(value:Delivery):Int = value.size
}

class Delivery extends BaseRetained {

  /**
   * Total size of the delivery.  Used for resource allocation tracking
   */
  var size:Int = 0

  /**
   *  the message being delivered
   */
  var message: Message = null

  /**
   * A reference to the stored version of the message.
   */
  var storeKey:Long = -1

  /**
   * The transaction the delivery is participating in.
   */
  var uow:StoreUOW = null

  /**
   * Set if the producer requires an ack to be sent back.  Consumer
   * should execute once the message is processed.
   */
  var ack:(StoreUOW)=>Unit = null

  def copy() = (new Delivery).set(this)

  def set(other:Delivery) = {
    size = other.size
    message = other.message
    storeKey = other.storeKey
    this
  }

  def createMessageRecord() = {
    val record = message.protocol.encode(message)
    assert( record.size == size )
    record
  }

}
