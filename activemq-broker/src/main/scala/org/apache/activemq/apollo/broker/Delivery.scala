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
package org.apache.activemq.apollo.broker

import _root_.org.apache.activemq.filter.{MessageEvaluationContext}
import _root_.java.lang.{String}
import _root_.org.fusesource.hawtdispatch._
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import org.apache.activemq.transport.Transport
import org.fusesource.hawtbuf._
import org.apache.activemq.util.TreeMap
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger}
import java.util.{HashSet, LinkedList}

/**
 * A producer which sends Delivery objects to a delivery consumer.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DeliveryProducer {
  def collocate(queue:DispatchQueue):Unit
}

/**
 * The delivery consumer accepts messages from a delivery producer.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DeliveryConsumer extends Retained {
  def matches(message:Delivery)
  val dispatchQueue:DispatchQueue;
  def connect(producer:DispatchQueue):DeliverySession
}

/**
 * Before a derlivery producer can send Delivery objects to a delivery
 * consumer, it creates a Delivery session which it uses to send
 * the deliveries over.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DeliverySession {
  val consumer:DeliveryConsumer
  def deliver(delivery:Delivery)
  def close:Unit
}


/**
 * Abstracts wire protocol message implementations.  Each wire protocol
 * will provide it's own type of Message.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Message {

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
   * used to apply a selector against the message.
   */
  def messageEvaluationContext:MessageEvaluationContext

}

case class StoredMessageRef(id:Long) extends BaseRetained

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
object Delivery {
//  def apply(o:Delivery) = {
//    rc new Delivery();
//    o.message, o.size, o.encoded, o.encoding, o.ack, o.tx_id, o.store_id)
//  }
}


class Delivery extends BaseRetained {

  /**
   * memory size of the delivery.  Used for resource allocation tracking
   */
  var size:Int = 0

  /**
   * the encoding format of the message
   */
  var encoding: String = null

  /**
   *  the message being delivered
   */
  var message: Message = null

  /**
   * the encoded form of the message being delivered.
   */
  var encoded: Buffer = null

  var ref:StoredMessageRef = null

  def copy() = (new Delivery).set(this)

  def set(other:Delivery) = {
    size = other.size
    encoding = other.encoding
    message = other.message
    encoded = other.encoded
    ref = other.ref
    this
  }

}

/**
 * <p>
 * Defines the interface for objects which you can send flow controlled deliveries to.
 * <p>
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DeliverySink {
  def full:Boolean
  def send(delivery:Delivery):Unit
}

/**
 * <p>
 * A delivery sink which is connected to a transport. It expects the caller's dispatch
 * queue to be the same as the transport's/
 * <p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class TransportDeliverySink(var transport:Transport) extends DeliverySink {
  def full:Boolean = transport.isFull
  def send(delivery:Delivery) = if( transport.isConnected ) { transport.oneway(delivery.message, delivery) }
}

/**
 * <p>
 * A delivery sink which buffers deliveries sent to it up to it's
 * maxSize settings after which it starts flow controlling the sender.
 * <p>
 *
 * <p>
 * It executes the eventHandler when it has buffered deliveries.  The event handler
 * should call receive to get the queued deliveries and then ack the delivery
 * once it has been processed.  The producer will now be resumed until
 * the ack occurs.
 * <p>
 *
 * <p>
 * This class should only be called from a single serial dispatch queue.
 * <p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DeliveryBuffer(var maxSize:Int=1024*32) extends DeliverySink {

  var deliveries = new LinkedList[Delivery]()
  private var size = 0
  var eventHandler: Runnable = null

  def full = size >= maxSize

  def drain = eventHandler.run

  def receive = deliveries.poll

  def isEmpty = deliveries.isEmpty

  def send(delivery:Delivery):Unit = {
    delivery.retain
    size += delivery.size
    deliveries.addLast(delivery)
    if( deliveries.size == 1 ) {
      drain
    }
  }

  def ack(delivery:Delivery) = {
    // When a message is delivered to the consumer, we release
    // used capacity in the outbound queue, and can drain the inbound
    // queue
    val wasBlocking = full
    size -= delivery.size
    delivery.release
    if( !isEmpty ) {
      drain
    }
  }

}

/**
 * Implements a delivery sink which sends to a 'down stream' delivery sink. If the
 * down stream delivery sink is full, this sink buffers the overflow deliveries. So that the
 * down stream sink does not need to worry about overflow.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DeliveryOverflowBuffer(val delivery_buffer:DeliverySink) extends DeliverySink {

  private var overflow = new LinkedList[Delivery]()

  protected def drainOverflow:Unit = {
    while( !overflow.isEmpty && !full ) {
      val delivery = overflow.removeFirst
      delivery.release
      send_to_delivery_buffer(delivery)
    }
  }

  def send(delivery:Delivery) = {
    if( full ) {
      // Deliveries in the overflow queue is remain acquired by us so that
      // producer that sent it to us gets flow controlled.
      delivery.retain
      overflow.addLast(delivery)
    } else {
      send_to_delivery_buffer(delivery)
    }
  }

  protected def send_to_delivery_buffer(value:Delivery) = {
    var delivery = value.copy
    delivery.setDisposer(^{
      drainOverflow
    })
    delivery_buffer.send(delivery)
    delivery.release
  }

  def full = delivery_buffer.full

}

/**
 * <p>
 * A DeliverySessionManager manages multiple credit based
 * transmission windows from multiple producers to a single consumer.
 * </p>
 *
 * <p>
 * Producers and consumers are typically executing on different threads and
 * the overhead of doing cross thread flow control is much higher than if
 * a credit window is used.  The credit window allows the producer to
 * send multiple messages to the consumer without needing to wait for
 * consumer events.  Only when the producer runs out of credit, does the
 * he start overflowing the producer.  This class makes heavy
 * use of Dispatch Source objects to coalesce cross thread events
 * like the sending messages or credits.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DeliverySessionManager(val targetSink:DeliverySink, val queue:DispatchQueue) extends BaseRetained {

  var sessions = List[Session]()

  var session_min_credits = 1024*4;
  var session_credit_capacity = 1024*32
  var session_max_credits = session_credit_capacity;

  queue.retain
  setDisposer(^{
    source.release
    queue.release
  })

  // use a event aggregating source to coalesce multiple events from the same thread.
  // all the sessions send to the same source.
  val source = createSource(new ListEventAggregator[Delivery](), queue)
  source.setEventHandler(^{drain_source});
  source.resume

  def drain_source = {
    val deliveries = source.getData
    deliveries.foreach { delivery=>
      targetSink.send(delivery)
      delivery.release
    }
  }

  /**
   * tracks one producer to consumer session / credit window.
   */
  class Session(val producer_queue:DispatchQueue) extends DeliveryOverflowBuffer(targetSink)  {

    // retain since the producer will be using this source to send messages
    // to the consumer
    source.retain

    ///////////////////////////////////////////////////
    // These members are used from the context of the
    // producer serial dispatch queue
    ///////////////////////////////////////////////////

    // create a source to coalesce credit events back to the producer side...
    val credit_adder = createSource(EventAggregators.INTEGER_ADD , producer_queue)
    credit_adder.setEventHandler(^{
      internal_credit(credit_adder.getData.intValue)
    });
    credit_adder.resume

    private var credits = 0;

    private var closed = false

    def close = {
      credit_adder.release
      source.release
      closed=true
    }

    override def full = credits <= 0

    override def send(delivery: Delivery) = {
      // retain the storage ref.. the target sink should
      // release once it no longer needs it.
      if( delivery.ref !=null ) {
        delivery.ref.retain
      }
      super.send(delivery)
    }

    override protected def send_to_delivery_buffer(value:Delivery) = {
      if( !closed ) {
        var delivery = value.copy
        credit_adder.retain
        delivery.setDisposer(^{
          // once the delivery is received by the consumer, event
          // the producer the credit.
          credit_adder.merge(delivery.size);
          credit_adder.release
        })
        internal_credit(-delivery.size)
        // use the source to send the consumer a delivery event.
        source.merge(delivery)
      }
    }

    def internal_credit(value:Int) = {
      credits += value;
      if( closed || credits <= 0 ) {
        credits = 0
      } else {
        drainOverflow
      }
    }

    ///////////////////////////////////////////////////
    // These members are used from the context of the
    // consumer serial dispatch queue
    ///////////////////////////////////////////////////

    private var _capacity = 0

    def credit(value:Int) = ^{
      internal_credit(value)
    } >>: producer_queue

    def capacity(value:Int) = {
      val change = value - _capacity;
      _capacity = value;
      credit(change)
    }

  }

  def open(producer_queue:DispatchQueue):DeliverySink = {
    val session = createSession(producer_queue)
    sessions = session :: sessions
    session.capacity(session_max_credits)
    session
  }

  def close(session:DeliverySink) = {
    session.asInstanceOf[DeliverySessionManager#Session].close
  }

  protected def createSession(producer_queue:DispatchQueue) = new Session(producer_queue)
}
