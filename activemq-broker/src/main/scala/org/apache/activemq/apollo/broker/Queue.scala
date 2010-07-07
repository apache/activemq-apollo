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
import org.fusesource.hawtdispatch.{DispatchQueue, BaseRetained}
import collection.{SortedMap}
import java.util.LinkedList

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

  // sequence numbers.. used to track what's in the store.
  var first_seq = -1L
  var last_seq = -1L
  var message_seq_counter=0L
  def next_message_seq = {
    val rc = message_seq_counter
    message_seq_counter += 1
    rc
  }

  var count = 0

  val pending = new QueueSink[Delivery](Delivery) {
    override def offer(delivery: Delivery) = {
      val d = delivery.copy
      d.ack = true
      super.offer(d)
    }
  }
  val session_manager = new SinkMux[Delivery](pending, dispatchQueue, Delivery)

  pending.drainer = ^{ drain_delivery_buffer }

  def drain_delivery_buffer: Unit = {
    while (!readyConsumers.isEmpty && !pending.isEmpty) {
      val cs = readyConsumers.removeFirst
      val delivery = pending.poll
      if( cs.session.offer(delivery) ) {
        // consumer was not full.. keep him in the ready list
        readyConsumers.addLast(cs)
      } else {
        // consumer full.
        cs.ready = false
        pending.unpoll(delivery)
      }
    }
  }


  // Use an event source to coalesce cross thread synchronization.
  val ack_source = createSource(new ListEventAggregator[Delivery](), dispatchQueue)
  ack_source.setEventHandler(^{drain_acks});
  ack_source.resume
  def drain_acks = {
    ack_source.getData.foreach { ack =>
      pending.ack(ack)
    }
  }
  override def ack(ack:Delivery) = {
    ack_source.merge(ack)
  }

  setDisposer(^{
    dispatchQueue.release
    session_manager.release
  })

  class ConsumerState(val session:Session) extends Runnable {
    session.refiller = this
    var bound=true
    var ready=true

    def run() = {
      if( bound && !ready ) {
        ready = true
        readyConsumers.addLast(this)
        drain_delivery_buffer
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
      drain_delivery_buffer
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

  def collocate(value:DispatchQueue):Unit = {
    if( value.getTargetQueue ne dispatchQueue.getTargetQueue ) {
      println(dispatchQueue.getLabel+" co-locating with: "+value.getLabel);
      this.dispatchQueue.setTargetQueue(value.getTargetQueue)
    }
  }

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
        if( value.ref !=null ) {
          value.ref.retain
        }
        val rc = session.offer(value)
        assert(rc, "session should accept since it was not full")
        true
      }
    }
    
    def refiller = session.refiller
    def refiller_=(value:Runnable) = { session.refiller=value }
  }

  def matches(message:Delivery) = { true }

//  def open_session(producer_queue:DispatchQueue) = new ConsumerSession {
//    val consumer = StompQueue.this
//    val deliveryQueue = new DeliveryOverflowBuffer(delivery_buffer)
//    retain
//
//    def deliver(delivery:Delivery) = using(delivery) {
//      deliveryQueue.send(delivery)
//    } >>: queue
//
//    def close = {
//      release
//    }
//  }


}

class XQueue(val destination:Destination) {

// TODO:
//    private VirtualHost virtualHost;
//
//    Queue() {
//        this.queue = queue;
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see
//     * org.apache.activemq.broker.DeliveryTarget#deliver(org.apache.activemq
//     * .broker.MessageDelivery, org.apache.activemq.flow.ISourceController)
//     */
//    public void deliver(MessageDelivery message, ISourceController<?> source) {
//        queue.add(message, source);
//    }
//
//    public final void addSubscription(final Subscription<MessageDelivery> sub) {
//        queue.addSubscription(sub);
//    }
//
//    public boolean removeSubscription(final Subscription<MessageDelivery> sub) {
//        return queue.removeSubscription(sub);
//    }
//
//    public void start() throws Exception {
//        queue.start();
//    }
//
//    public void stop() throws Exception {
//        if (queue != null) {
//            queue.stop();
//        }
//    }
//
//    public void shutdown(Runnable onShutdown) throws Exception {
//        if (queue != null) {
//            queue.shutdown(onShutdown);
//        }
//    }
//
//    public boolean hasSelector() {
//        return false;
//    }
//
//    public boolean matches(MessageDelivery message) {
//        return true;
//    }
//
//    public VirtualHost getBroker() {
//        return virtualHost;
//    }
//
//    public void setVirtualHost(VirtualHost virtualHost) {
//        this.virtualHost = virtualHost;
//    }
//
//    public void setDestination(Destination destination) {
//        this.destination = destination;
//    }
//
//    public final Destination getDestination() {
//        return destination;
//    }
//
//    public boolean isDurable() {
//        return true;
//    }
//
//    public static class QueueSubscription implements BrokerSubscription {
//        Subscription<MessageDelivery> subscription;
//        final Queue queue;
//
//        public QueueSubscription(Queue queue) {
//            this.queue = queue;
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.broker.BrokerSubscription#connect(org.apache.
//         * activemq.broker.protocol.ProtocolHandler.ConsumerContext)
//         */
//        public void connect(ConsumerContext subscription) throws UserAlreadyConnectedException {
//            this.subscription = subscription;
//            queue.addSubscription(subscription);
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.broker.BrokerSubscription#disconnect(org.apache
//         * .activemq.broker.protocol.ProtocolHandler.ConsumerContext)
//         */
//        public void disconnect(ConsumerContext context) {
//            queue.removeSubscription(subscription);
//        }
//
//        /* (non-Javadoc)
//         * @see org.apache.activemq.broker.BrokerSubscription#getDestination()
//         */
//        public Destination getDestination() {
//            return queue.getDestination();
//        }
//    }

  // TODO:
  def matches(message:Delivery) = false
  def deliver(message:Delivery) = {
    // TODO:
  }

  def getDestination() = destination

  def shutdown = {}
}
