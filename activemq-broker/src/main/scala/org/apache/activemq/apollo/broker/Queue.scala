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

import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import collection.{SortedMap}
import java.util.LinkedList
import org.fusesource.hawtdispatch.{EventAggregators, DispatchQueue, BaseRetained}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait QueueLifecyleListener {

    /**
     * A destination has bean created
     *
     * @param queue
     */
    def onCreate(queue:Queue);

    /**
     * A destination has bean destroyed
     *
     * @param queue
     */
    def onDestroy(queue:Queue);

}

object QueueEntry extends Sizer[QueueEntry] {
  def size(value:QueueEntry):Int = value.delivery.size
}
class QueueEntry(val seq:Long, val delivery:Delivery)

object Queue extends Log {
  val maxOutboundSize = 1024*1204*5
}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Queue(val destination:Destination, val storeId:Long) extends BaseRetained with Route with DeliveryConsumer with DeliveryProducer with DispatchLogging {
  override protected def log = Queue

  override val dispatchQueue:DispatchQueue = createQueue("queue:"+destination);
  dispatchQueue.setTargetQueue(getRandomThreadQueue)
  dispatchQueue {
    debug("created queue for: "+destination)
  }
  setDisposer(^{
    dispatchQueue.release
    session_manager.release
  })

  // sequence numbers.. used to track what's in the store.
  var first_seq = -1L
  var last_seq = -1L
  var message_seq_counter=0L
  var count = 0

  val pending = new QueueSink[QueueEntry](QueueEntry)
  val session_manager = new SinkMux[Delivery](MapSink(pending){ x=>accept(x) }, dispatchQueue, Delivery)
  pending.drainer = ^{ drain }


  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the Route trait.  Allows consumers to bind/unbind
  // from this queue so that it can send messages to them.
  //
  /////////////////////////////////////////////////////////////////////

  class ConsumerState(val session:Session) extends Runnable {
    session.refiller = this
    var bound=true
    var ready=true

    def run() = {
      if( bound && !ready ) {
        ready = true
        readyConsumers.addLast(this)
        drain
      }
    }
  }

  var allConsumers = Map[DeliveryConsumer,ConsumerState]()
  val readyConsumers = new LinkedList[ConsumerState]()

  def connected(consumers:List[DeliveryConsumer]) = bind(consumers)
  def bind(consumers:List[DeliveryConsumer]) = retaining(consumers) {
      for ( consumer <- consumers ) {
        val cs = new ConsumerState(consumer.connect(Queue.this))
        allConsumers += consumer->cs
        readyConsumers.addLast(cs)
      }
      drain
    } >>: dispatchQueue

  def unbind(consumers:List[DeliveryConsumer]) = releasing(consumers) {
      for ( consumer <- consumers ) {
        allConsumers.get(consumer) match {
          case Some(cs)=>
            cs.bound = false
            cs.session.close
            allConsumers -= consumer
            readyConsumers.remove(cs)
          case None=>
        }
      }
    } >>: dispatchQueue

  def disconnected() = throw new RuntimeException("unsupported")

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the DeliveryConsumer trait.  Allows this queue
  // to receive messages from producers.
  //
  /////////////////////////////////////////////////////////////////////

  def matches(message:Delivery) = { true }

  def connect(p:DeliveryProducer) = new Session {
    retain

    override def consumer = Queue.this
    override def producer = p

    val session = session_manager.open(producer.dispatchQueue)

    def close = {
      session_manager.close(session)
      release
    }

    // Delegate all the flow control stuff to the session
    def full = session.full
    def offer(value:Delivery) = {
      if( session.full ) {
        false
      } else {
        val rc = session.offer(sent(value))
        assert(rc, "session should accept since it was not full")
        true
      }
    }
    
    def refiller = session.refiller
    def refiller_=(value:Runnable) = { session.refiller=value }
  }


  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the DeliveryProducer trait.
  // It mainly deals with handling message acks from bound consumers.
  //
  /////////////////////////////////////////////////////////////////////

  val ack_source = createSource(EventAggregators.INTEGER_ADD, dispatchQueue)
  ack_source.setEventHandler(^{drain_acks});
  ack_source.resume
  def drain_acks = {
    pending.ack(ack_source.getData.intValue)
  }
  override def ack(ack:Delivery) = {
    ack_source.merge(ack.size)
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation methods.
  //
  /////////////////////////////////////////////////////////////////////

  /**
   * Called from the producer thread before the delivery is
   * processed by the queues' thread.. therefore we don't
   * yet know the order of the delivery in the queue.
   */
  private def sent(delivery:Delivery) = {
    if( delivery.ref !=null ) {
      delivery.ref.retain
    }
    delivery
  }

  /**
   * Called from the queue thread.  At this point we
   * know the order.  Converts the delivery to a QueueEntry
   */
  private def accept(delivery:Delivery) = {
    val d = delivery.copy
    d.ack = true
    new QueueEntry(next_message_seq, d)
  }

  /**
   * Dispatches as many messages to ready consumers
   * as possible.
   */
  private def drain: Unit = {
    while (!readyConsumers.isEmpty && !pending.isEmpty) {
      val cs = readyConsumers.removeFirst
      val queueEntry = pending.poll
      if( cs.session.offer(queueEntry.delivery) ) {
        // consumer was not full.. keep him in the ready list
        readyConsumers.addLast(cs)
      } else {
        // consumer full.
        cs.ready = false
        pending.unpoll(queueEntry)
      }
    }
  }

  private def next_message_seq = {
    val rc = message_seq_counter
    message_seq_counter += 1
    rc
  }


}
