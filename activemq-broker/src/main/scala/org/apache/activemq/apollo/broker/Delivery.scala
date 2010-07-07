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

import _root_.java.util.{LinkedList, LinkedHashMap, HashMap}
import _root_.org.apache.activemq.filter.{MessageEvaluationContext}
import _root_.java.lang.{String}
import _root_.org.apache.activemq.util.buffer.{Buffer, AsciiBuffer}
import _root_.org.fusesource.hawtdispatch._
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._

trait DeliveryProducer {
  def collocate(queue:DispatchQueue):Unit
}

trait DeliverySession {
  val consumer:DeliveryConsumer
  def deliver(delivery:Delivery)
  def close:Unit
}

trait DeliveryConsumer extends Retained {
  def matches(message:Delivery)
  val queue:DispatchQueue;
  def open_session(producer_queue:DispatchQueue):DeliverySession
}

/**
 * Abstracts wire protocol message implementations.  Each wire protocol
 * will provide it's own type of Message.
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

object Delivery {
  def apply(o:Delivery) = new Delivery(o.message, o.encoded, o.encoding, o.size, o.ack, o.tx_id, o.store_id)
}

case class Delivery (

        /**
         *  the message being delivered
         */
  message: Message,

  /**
   * the encoded form of the message being delivered.
   */
  encoded: Buffer,

  /**
   * the encoding format of the message
   */
  encoding: String,

  /**
   * memory size of the delivery.  Used for resource allocation tracking
   */
  size:Int,

  /**
   *  true if this delivery requires acknowledgment.
   */
  ack:Boolean,

  /**
   * The id used to identify the transaction that the message
   * belongs to.
   */
  tx_id:Long,

  /**
   * The id used to identify this message in the message
   * store.
   *
   * @return The store tracking or -1 if not set.
   */
  store_id: Long

) extends BaseRetained {

}

//abstract class BrokerMessageDelivery extends MessageDelivery {
// TODO:
//    // True while the message is being dispatched to the delivery targets:
//    boolean dispatching = false;
//
//    // A non null pending save indicates that the message is the
//    // saver queue and that the message
//    OperationContext<?> pendingSave;
//
//    // List of persistent targets for which the message should be saved
//    // when dispatch is complete:
//    HashMap<QueueDescriptor, SaveableQueueElement<MessageDelivery>> persistentTargets;
//    SaveableQueueElement<MessageDelivery> singleTarget;
//
//    long storeTracking = -1;
//    BrokerDatabase store;
//    boolean fromStore = false;
//    boolean enableFlushDelay = true;
//    private int limiterSize = -1;
//    private long tid=-1;
//
//    public void setFromDatabase(BrokerDatabase database, MessageRecord mRecord) {
//        fromStore = true;
//        store = database;
//        storeTracking = mRecord.getKey();
//        limiterSize = mRecord.getSize();
//    }
//
//    public final int getFlowLimiterSize() {
//        if (limiterSize == -1) {
//            limiterSize = getMemorySize();
//        }
//        return limiterSize;
//    }
//
//    /**
//     * When an application wishes to include a message in a broker transaction
//     * it must set this the tid returned by {@link Transaction#getTid()}
//     *
//     * @param tid
//     *            Sets the tid used to identify the transaction at the broker.
//     */
//    public void setTransactionId(long tid) {
//        this.tid = tid;
//    }
//
//    /**
//     * @return The tid used to identify the transaction at the broker.
//     */
//    public final long getTransactionId() {
//        return tid;
//    }
//
//    public final void clearTransactionId() {
//        tid = -1;
//    }
//
//    /**
//     * Subclass must implement this to return their current memory size
//     * estimate.
//     *
//     * @return The memory size of the message.
//     */
//    public abstract int getMemorySize();
//
//    public final boolean isFromStore() {
//        return fromStore;
//    }
//
//    public final void persist(SaveableQueueElement<MessageDelivery> sqe, ISourceController<?> controller, boolean delayable) {
//        synchronized (this) {
//            // Can flush of this message to the store be delayed?
//            if (enableFlushDelay && !delayable) {
//                enableFlushDelay = false;
//            }
//            // If this message is being dispatched then add the queue to the
//            // list of queues for which to save the message when dispatch is
//            // finished:
//            if (dispatching) {
//                addPersistentTarget(sqe);
//                return;
//            }
//            // Otherwise, if it is still in the saver queue, we can add this
//            // queue to the queue list:
//            else if (pendingSave != null) {
//                addPersistentTarget(sqe);
//                if (!delayable) {
//                    pendingSave.requestFlush();
//                }
//                return;
//            }
//        }
//
//        store.saveMessage(sqe, controller, delayable);
//    }
//
//    public final void acknowledge(SaveableQueueElement<MessageDelivery> sqe) {
//        boolean firePersistListener = false;
//        boolean deleted = false;
//        synchronized (this) {
//            // If the message hasn't been saved to the database
//            // then we don't need to issue a delete:
//            if (dispatching || pendingSave != null) {
//
//                deleted = true;
//
//                removePersistentTarget(sqe.getQueueDescriptor());
//                // We get a save context when we place the message in the
//                // database queue. If it has been added to the queue,
//                // and we've removed the last queue, see if we can cancel
//                // the save:
//                if (pendingSave != null && !hasPersistentTargets()) {
//                    if (pendingSave.cancel()) {
//                        pendingSave = null;
//                        if (isPersistent()) {
//                            firePersistListener = true;
//                        }
//                    }
//                }
//            }
//        }
//
//        if (!deleted) {
//            store.deleteQueueElement(sqe);
//        }
//
//        if (firePersistListener) {
//            onMessagePersisted();
//        }
//
//    }
//
//    public final void setStoreTracking(long tracking) {
//        if (storeTracking == -1) {
//            storeTracking = tracking;
//        }
//    }
//
//    public final void beginDispatch(BrokerDatabase database) {
//        this.store = database;
//        dispatching = true;
//        setStoreTracking(database.allocateStoreTracking());
//    }
//
//    public long getStoreTracking() {
//        return storeTracking;
//    }
//
//    public synchronized Collection<SaveableQueueElement<MessageDelivery>> getPersistentQueues() {
//        if (singleTarget != null) {
//            ArrayList<SaveableQueueElement<MessageDelivery>> list = new ArrayList<SaveableQueueElement<MessageDelivery>>(1);
//            list.add(singleTarget);
//            return list;
//        } else if (persistentTargets != null) {
//            return persistentTargets.values();
//        }
//        return null;
//    }
//
//    public void beginStore() {
//        synchronized (this) {
//            pendingSave = null;
//        }
//    }
//
//    private final boolean hasPersistentTargets() {
//        return (persistentTargets != null && !persistentTargets.isEmpty()) || singleTarget != null;
//    }
//
//    private final void removePersistentTarget(QueueDescriptor queue) {
//        if (persistentTargets != null) {
//            persistentTargets.remove(queue);
//            return;
//        }
//
//        if (singleTarget != null && singleTarget.getQueueDescriptor().equals(queue)) {
//            singleTarget = null;
//        }
//    }
//
//    private final void addPersistentTarget(SaveableQueueElement<MessageDelivery> elem) {
//        if (persistentTargets != null) {
//            persistentTargets.put(elem.getQueueDescriptor(), elem);
//            return;
//        }
//
//        if (singleTarget == null) {
//            singleTarget = elem;
//            return;
//        }
//
//        if (elem.getQueueDescriptor() != singleTarget.getQueueDescriptor()) {
//            persistentTargets = new HashMap<QueueDescriptor, SaveableQueueElement<MessageDelivery>>();
//            persistentTargets.put(elem.getQueueDescriptor(), elem);
//            persistentTargets.put(singleTarget.getQueueDescriptor(), singleTarget);
//            singleTarget = null;
//        }
//    }
//
//    public final void finishDispatch(ISourceController<?> controller) throws IOException {
//        boolean firePersistListener = false;
//        synchronized (this) {
//            // If any of the targets requested save then save the message
//            // Note that this could be the case even if the message isn't
//            // persistent if a target requested that the message be spooled
//            // for some other reason such as queue memory overflow.
//            if (hasPersistentTargets()) {
//                pendingSave = store.persistReceivedMessage(this, controller);
//            }
//
//            // If none of the targets required persistence, then fire the
//            // persist listener:
//            if (pendingSave == null || !isPersistent()) {
//                firePersistListener = true;
//            }
//            dispatching = false;
//        }
//
//        if (firePersistListener) {
//            onMessagePersisted();
//        }
//    }
//
//    public final MessageRecord createMessageRecord() {
//
//        MessageRecord record = new MessageRecord();
//        record.setEncoding(getStoreEncoding());
//        record.setBuffer(getStoreEncoded());
//        record.setStreamKey((long) 0);
//        record.setMessageId(getMsgId());
//        record.setSize(getFlowLimiterSize());
//        record.setKey(getStoreTracking());
//        return record;
//    }
//
//    /**
//     * @return A buffer representation of the message to be stored in the store.
//     * @throws
//     */
//    protected abstract Buffer getStoreEncoded();
//
//    /**
//     * @return The encoding scheme used to store the message.
//     */
//    protected abstract AsciiBuffer getStoreEncoding();
//
//    public boolean isFlushDelayable() {
//        // TODO Auto-generated method stub
//        return enableFlushDelay;
//    }
//}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DeliveryBuffer(var maxSize:Int=1024*32) {

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

class DeliveryOverflowBuffer(val delivery_buffer:DeliveryBuffer) {

  private var overflow = new LinkedList[Delivery]()

  protected def drainOverflow:Unit = {
    while( !overflow.isEmpty && !full ) {
      val delivery = overflow.removeFirst
      delivery.release
      send_to_delivery_queue(delivery)
    }
  }

  def send(delivery:Delivery) = {
    if( full ) {
      // Deliveries in the overflow queue is remain acquired by us so that
      // producer that sent it to us gets flow controlled.
      delivery.retain
      overflow.addLast(delivery)
    } else {
      send_to_delivery_queue(delivery)
    }
  }

  protected def send_to_delivery_queue(value:Delivery) = {
    var delivery = Delivery(value)
    delivery.setDisposer(^{
      drainOverflow
    })
    delivery_buffer.send(delivery)
    delivery.release
  }

  def full = delivery_buffer.full

}

class DeliveryCreditBufferProtocol(val delivery_buffer:DeliveryBuffer, val queue:DispatchQueue) extends BaseRetained {

  var sessions = List[CreditServer]()

  var session_min_credits = 1024*4;
  var session_credit_capacity = 1024*32
  var session_max_credits = session_credit_capacity;

  queue.retain
  setDisposer(^{
    source.release
    queue.release
  })

  // use a event aggregating source to coalesce multiple events from the same thread.
  val source = createSource(new ListEventAggregator[Delivery](), queue)
  source.setEventHandler(^{drain_source});
  source.resume

  def drain_source = {
    val deliveries = source.getData
    deliveries.foreach { delivery=>
      delivery_buffer.send(delivery)
      delivery.release
    }
  }


  class CreditServer(val producer_queue:DispatchQueue) {
    private var _capacity = 0

    def capacity(value:Int) = {
      val change = value - _capacity;
      _capacity = value;
      client.credit(change)
    }

    def drain(callback:Runnable) = {
      client.drain(callback)
    }

    val client = new CreditClient()

    class CreditClient() extends DeliveryOverflowBuffer(delivery_buffer) {

      producer_queue.retain
      val credit_adder = createSource(EventAggregators.INTEGER_ADD , producer_queue)
      credit_adder.setEventHandler(^{
        internal_credit(credit_adder.getData.intValue)
      });
      credit_adder.resume

      private var credits = 0;

      ///////////////////////////////////////////////////
      // These methods get called from the client/producer thread...
      ///////////////////////////////////////////////////
      def close = {
        credit_adder.release
        producer_queue.release
      }

      override def full = credits <= 0

      override protected def send_to_delivery_queue(value:Delivery) = {
        var delivery = Delivery(value)
        delivery.setDisposer(^{
          // This is called from the server/consumer thread
          credit_adder.merge(delivery.size);
        })
        internal_credit(-delivery.size)
        source.merge(delivery)
      }

      def internal_credit(value:Int) = {
        credits += value;
        if( credits <= 0 ) {
          credits = 0
        } else {
          drainOverflow
        }
      }

      ///////////////////////////////////////////////////
      // These methods get called from the server/consumer thread...
      ///////////////////////////////////////////////////
      def credit(value:Int) = ^{ internal_credit(value) } ->: producer_queue

      def drain(callback:Runnable) = {
        credits = 0
        if( callback!=null ) {
          queue << callback
        }
      }
    }
  }

  def session(queue:DispatchQueue) = {
    val session = new CreditServer(queue)
    sessions = session :: sessions
    session.capacity(session_max_credits)
    session.client
  }


}
