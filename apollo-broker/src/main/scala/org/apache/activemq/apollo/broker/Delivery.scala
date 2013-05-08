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

import _root_.org.fusesource.hawtdispatch._
import org.fusesource.hawtbuf._
import org.apache.activemq.apollo.filter.Filterable
import org.apache.activemq.apollo.broker.store.StoreUOW
import org.apache.activemq.apollo.util.Log
import java.util.concurrent.atomic.AtomicReference
import org.apache.activemq.apollo.broker.protocol.MessageCodec

object DeliveryProducer extends Log

/**
 * A producer which sends Delivery objects to a delivery consumer.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DeliveryProducer {
  import DeliveryProducer._

  def dispatch_queue:DispatchQueue

  def connection:Option[BrokerConnection] = None

  def send_buffer_size = 64*1024

  def collocate(value:DispatchQueue):Unit = {
    if( value.getTargetQueue ne dispatch_queue.getTargetQueue ) {
      debug("co-locating %s with %s", dispatch_queue.getLabel, value.getLabel);
      this.dispatch_queue.setTargetQueue(value.getTargetQueue)
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

  def receive_buffer_size = 64*1024

  def close_on_drain = browser
  def start_from_tail = false
  def set_starting_seq(seq:Long) = {}

  def user:String = null
  def jms_selector:String = null
  def browser = false
  def exclusive = false
  def dispatch_queue:DispatchQueue;
  def matches(message:Delivery):Boolean
  def connect(producer:DeliveryProducer):DeliverySession
  def is_persistent:Boolean
}

abstract class AbstractRetainedDeliveryConsumer extends BaseRetained with DeliveryConsumer

class DeliveryConsumerFilter(val next:DeliveryConsumer) extends DeliveryConsumer {
  override def browser: Boolean = next.browser
  override def close_on_drain: Boolean = next.close_on_drain
  override def connection: Option[BrokerConnection] = next.connection
  override def exclusive: Boolean = next.exclusive
  override def jms_selector: String = next.jms_selector
  override def receive_buffer_size: Int = next.receive_buffer_size
  override def set_starting_seq(seq: Long) { next.set_starting_seq(seq)  }
  override def start_from_tail: Boolean = next.start_from_tail
  override def user: String = next.user
  def connect(producer: DeliveryProducer): DeliverySession = next.connect(producer)
  def dispatch_queue: DispatchQueue = next.dispatch_queue
  def is_persistent: Boolean = next.is_persistent
  def matches(message: Delivery): Boolean = next.matches(message)
  def release() { next.release() }
  def retain() { next.retain() }
  def retained(): Int = next.retained()
}

/**
 * Before a delivery producer can send Delivery objects to a delivery
 * consumer, it creates a Delivery session which it uses to send
 * the deliveries over.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DeliverySession extends SessionSink[Delivery] {
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
   * The encoder/decoder of the message
   */
  def codec:MessageCodec

  def headers_as_json = new java.util.HashMap[String, Object]()

  def encoded:Buffer = codec.encode(this).buffer

  def message_group: String = null
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

sealed trait DeliveryResult

/**
 * message was delivered and processed, does not need redelivery
 */
object Consumed extends DeliveryResult

/**
 * The message was delivered but not consumed, it should be redelivered to another consumer ASAP.
 * The redelivery counter should increment.
 */
object Delivered extends DeliveryResult

/**
 * The message was not delivered so it should be redelivered to another consumer but not effect
 * it's redelivery counter.
 */
object Undelivered extends DeliveryResult

/**
 * message expired before it could be processed, does not need redelivery
 */
object Expired extends DeliveryResult

/**
 * The receiver thinks the message was poison message, it was not successfully
 * processed and it should not get redelivered..
 */
object Poisoned extends DeliveryResult


sealed trait RetainAction
object RetainSet extends RetainAction
object RetainRemove extends RetainAction
object RetainIgnore extends RetainAction

class Delivery {

  /**
   * Where the delivery is originating from.
   */
  var sender = List[DestinationAddress]()

  /**
   * Total size of the delivery.  Used for resource allocation tracking
   */
  var size:Int = 0

  /**
   * When the message will expire
   */
  var expiration:Long = 0

  /**
   * Is the delivery persistent?
   */
  var persistent:Boolean = false

  /**
   *  the message being delivered
   */
  var message: Message = null

  /**
   * The sequence id the destination assigned the message
   */
  var seq:Long = -1

  /**
   * The id the store assigned the message
   */
  var storeKey:Long = -1

  /**
   * After the store persists the message he may be able to supply us with  locator handle
   * which will load the message faster than looking it up via the store key.
   */
  var storeLocator:AtomicReference[Object] = null

  /**
   * The transaction the delivery is participating in.
   */
  var uow:StoreUOW = null

  /**
   * The number of redeliveries that this message has seen.
   */
  var redeliveries:Short = 0

  /**
   * Set if the producer requires an ack to be sent back.  Consumer
   * should execute once the message is processed.
   */
  var ack:(DeliveryResult, StoreUOW)=>Unit = null

  /**
   * Should this message get retained as the last image of a topic?
   */
  var retain:RetainAction = RetainIgnore


  def copy() = (new Delivery).set(this)

  def set(other:Delivery) = {
    sender = other.sender
    size = other.size
    persistent = other.persistent
    expiration = other.expiration
    size = other.size
    seq = other.seq
    message = other.message
    storeKey = other.storeKey
    storeLocator = other.storeLocator
    redeliveries = other.redeliveries
    retain = other.retain
    this
  }

  def createMessageRecord() = {
    val record = message.codec.encode(message)
    record.locator = storeLocator
    record
  }

  def redelivered = redeliveries = ((redeliveries+1).min(Short.MaxValue)).toShort

  override def toString = {
    "Delivery(" +
      "sender:"+sender+", "+
      "size:"+size+", "+
      "message codec:"+message.codec.id+", "+
      "expiration:"+expiration+", "+
      "persistent:"+persistent+", "+
      "redeliveries:"+redeliveries+", "+
      "seq:"+seq+", "+
      "storeKey:"+storeKey+", "+
      "storeLocator:"+storeLocator+", "+
      "uow:"+uow+", "+
      "ack:"+(ack!=null)+
    ")"
  }
}
