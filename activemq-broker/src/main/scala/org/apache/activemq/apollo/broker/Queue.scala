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
import java.util.{LinkedList}
import org.apache.activemq.util.TreeMap
import collection.{SortedMap}
import org.fusesource.hawtdispatch.{ScalaDispatch, DispatchQueue, BaseRetained}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait QueueLifecyleListener {

  /**
   * A destination has bean created
   *
   * @param queue
   */
  def onCreate(queue: Queue);

  /**
   * A destination has bean destroyed
   *
   * @param queue
   */
  def onDestroy(queue: Queue);

}


object Queue extends Log {
  val maxOutboundSize = 1024 * 1204 * 5
}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Queue(val host: VirtualHost, val destination: Destination, val storeId: Long) extends BaseRetained with Route with DeliveryConsumer with DispatchLogging {
  override protected def log = Queue

  import Queue._

  var consumerSubs = Map[DeliveryConsumer, Subscription]()

  override val dispatchQueue: DispatchQueue = createQueue("queue:" + destination);
  dispatchQueue.setTargetQueue(getRandomThreadQueue)
  dispatchQueue {
    debug("created queue for: " + destination)
  }
  setDisposer(^ {
    dispatchQueue.release
    session_manager.release
  })

  // sequence numbers.. used to track what's in the store.
  var first_seq = -1L
  var last_seq = -1L
  var message_seq_counter = 0L
  var count = 0

  var maxSize: Int = 1024 * 32

  val headEntry = new PagedEntry(this)
  headEntry.seq = -1
  var tailEntry = new PagedEntry(this)

  object messages extends Sink[PagedEntry] {
    var counter = 0
    val entries = new TreeMap[Long, PagedEntry]()
    entries.put(headEntry.seq, headEntry)

    private var size = 0
    var refiller: Runnable = null

    def full = size >= maxSize

    def offer(value: PagedEntry): Boolean = {

      if (full) {
        false
      } else {

        val ref = value.delivery.ref
        if (ref != null) {
          host.database.addMessageToQueue(storeId, value.seq, ref)
          ref.release
        }

        size += value.delivery.size
        entries.put(value.seq, value)
        counter += 1;

        if( !value.isEmpty ) {
          value.dispatch
        }
        true
      }
    }

    def ack(value: PagedEntry) = {

      if (value.delivery.ref != null) {
        host.database.removeMessageFromQueue(storeId, value.seq, null)
      }

      counter -= 1
      size -= value.delivery.size

      value.delivery = null

      // acked entries turn into a tombstone entry..  adjacent tombstones
      // aggregate into a single entry.
      var current = entries.getEntry(value.seq)
      assert(current != null)

      // Merge /w previous if possible
      var adj = current.previous
      if (adj.getValue.mergeTomestone(current.getValue)) {
        entries.removeEntry(current)
        current = adj
      }

      // Merge /w next if possible
      adj = current.next
      if (adj != null && current.getValue.mergeTomestone(adj.getValue)) {
        entries.removeEntry(adj)
      }


      if (counter == 0) {
        refiller.run
      }
    }


    def nack(values: List[PagedEntry]) = {
      for (v <- values) {
        v.unaquire;
        // TODO: re-dispatch em.
      }
    }

  }

  val session_manager = new SinkMux[Delivery](MapSink(messages) {x => accept(x)}, dispatchQueue, Delivery)

  val ack_source = createSource(new ListEventAggregator[(Subscription, Delivery)](), dispatchQueue)
  ack_source.setEventHandler(^ {drain_acks});
  ack_source.resume

  def drain_acks = {
    ack_source.getData.foreach {
      event =>
        event._1._ack(event._2)
    }
  }


  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the DeliveryConsumer trait.  Allows this queue
  // to receive messages from producers.
  //
  /////////////////////////////////////////////////////////////////////

  def matches(message: Delivery) = {true}

  def connect(p: DeliveryProducer) = new Session {
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

    def offer(value: Delivery) = {
      if (session.full) {
        false
      } else {
        val rc = session.offer(sent(value))
        assert(rc, "session should accept since it was not full")
        true
      }
    }

    def refiller = session.refiller

    def refiller_=(value: Runnable) = {session.refiller = value}
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the Route trait.  Allows consumers to bind/unbind
  // from this queue so that it can send messages to them.
  //
  /////////////////////////////////////////////////////////////////////

  def connected(consumers: List[DeliveryConsumer]) = bind(consumers)

  def bind(consumers: List[DeliveryConsumer]) = retaining(consumers) {
    for (consumer <- consumers) {
      val subscription = new Subscription(this)
      subscription.connect(consumer)
      consumerSubs += consumer -> subscription
    }
  } >>: dispatchQueue

  def unbind(consumers: List[DeliveryConsumer]) = releasing(consumers) {
    for (consumer <- consumers) {
      consumerSubs.get(consumer) match {
        case Some(cs) =>
          cs.close
          consumerSubs -= consumer
        case None =>
      }
    }
  } >>: dispatchQueue

  def disconnected() = throw new RuntimeException("unsupported")

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
  private def sent(delivery: Delivery) = {
    if (delivery.ref != null) {
      // retain the persistent ref so that the delivery is not
      // considered completed until this queue stores it
      delivery.ref.retain
    }
    delivery
  }

  /**
   * Called from the queue thread.  At this point we
   * know the order.  Converts the delivery to a QueueEntry
   */
  private def accept(delivery: Delivery) = {
    val d = delivery.copy
    d.ack = true
    val rc = tailEntry
    tailEntry = new PagedEntry(this)
    rc.seq = next_message_seq
    rc.delivery = d
    rc
  }


  private def next_message_seq = {
    val rc = message_seq_counter
    message_seq_counter += 1
    rc
  }


}

object PagedEntry extends Sizer[PagedEntry] {
  def size(value: PagedEntry): Int = value.delivery.size

}
class PagedEntry(val queue:Queue) extends Comparable[PagedEntry] with Runnable {
  def compareTo(o: PagedEntry) = {
    (seq - o.seq).toInt
  }

  var delivery: Delivery = null
  var seq: Long = -1
  var count: Long = 1
  var aquired = false
  var competing:List[Subscription] = Nil
  var browsing:List[Subscription] = Nil

  def aquire() = {
    if (aquired) {
      false
    } else {
      aquired = true
      true
    }
  }

  def unaquire() = {
    assert(aquired)
    aquired = false
  }


  def mergeTomestone(next: PagedEntry): Boolean = {
    if (tomestone && next.tomestone && seq + count == next.seq) {
      count += next.count
      if( next.browsing!=Nil || next.competing!=Nil ){
        addBrowsing(next.browsing)
        addCompeting(next.competing)
        next.browsing = Nil
        next.competing = Nil
      }
      true
    } else {
      false
    }
  }

  def tomestone = {
    delivery == null
  }

  def isEmpty = competing == Nil && browsing == Nil


  def run() = {
    var next = dispatch()
    while( next!=null ) {
      next = next.dispatch
    }
  }

  def dispatch():PagedEntry = {

    if( this == queue.tailEntry ) {

      // The tail entry does not hold data..
      null

    } else if( this == queue.headEntry ) {

      // The head entry does not hold any data.. so just move
      // any assigned subs to the next entry.
      
      val p = nextEntry
      p.addBrowsing(browsing)
      p.addCompeting(competing)
      browsing = Nil
      competing = Nil
      p

    } else {

      var browsingSlowSubs:List[Subscription] = Nil
      var browsingFastSubs:List[Subscription] = Nil
      var competingSlowSubs:List[Subscription] = Nil
      var competingFastSubs:List[Subscription] = Nil

      if( browsing!=Nil ) {
        browsing.foreach { sub =>
          if (sub.matches(this)) {
            if (sub.offer(this)) {
              browsingFastSubs ::= sub
            } else {
              browsingSlowSubs ::= sub
            }
          } else {
            browsingFastSubs ::= sub
          }
        }
      }

      if( competing!=Nil ) {
        if (!this.aquired) {
          this.aquire()

          var picked: Subscription = null
          var remaining = competing
          while( remaining!=Nil && picked == null ) {
            val sub = remaining.head
            remaining = remaining.drop(1)

            if (sub.matches(this)) {
              competingSlowSubs = competingSlowSubs ::: sub :: Nil
              if (sub.offer(this)) {
                picked = sub
              }
            } else {
              competingFastSubs = competingFastSubs ::: sub :: Nil
            }
          }

          if (picked == null) {
            this.unaquire()
          } else {
            competingFastSubs = remaining ::: competingFastSubs ::: competingSlowSubs
            competingSlowSubs = Nil
          }
        } else {
          competingFastSubs = competing
        }
      }

      // The slow subs stay on this entry..
      browsing = browsingSlowSubs
      competing = competingSlowSubs

      // the fast subs move on to the next entry...
      if ( browsingFastSubs!=null &&  competingFastSubs!=null) {
        val p = nextEntry
        p.addBrowsing(browsingFastSubs)
        p.addCompeting(competingFastSubs)
        p
      } else {
        null
      }
    }
  }

  def addBrowsing(l:List[Subscription]) = {
    l.foreach(x=>x.position(this))
    browsing :::= l
  }

  def addCompeting(l:List[Subscription]) = {
    l.foreach(x=>x.position(this))
    competing :::= l
  }

  def removeBrowsing(s:Subscription) = {
    s.position(null)
    browsing = browsing.filterNot(_ == s)
  }

  def removeCompeting(s:Subscription) = {
    s.position(null)
    competing = competing.filterNot(_ == s)
  }

  def nextEntry():PagedEntry = {
    var entry = queue.messages.entries.get(this.seq + 1)
    if (entry == null) {
      entry = queue.tailEntry
    }
    entry
  }

}

class Subscription(queue:Queue) extends DeliveryProducer {

  def dispatchQueue = queue.dispatchQueue

  var dispatched = List[PagedEntry]()
  var session: Session = null
  var pos:PagedEntry = null

  def position(value:PagedEntry):Unit = {
    pos = value
    session.refiller = pos
  }

  def connect(consumer: DeliveryConsumer) = {
    session = consumer.connect(this)
    queue.headEntry.addCompeting(this :: Nil)
    queue.dispatchQueue << queue.headEntry
  }

  def close() = {
    pos.removeCompeting(this)
    session.close
    session = null
    queue.messages.nack(dispatched)
  }

  def matches(entry:PagedEntry) = session.consumer.matches(entry.delivery)

  def offer(entry:PagedEntry) = {
    if (session.offer(entry.delivery)) {
      dispatched = dispatched ::: entry :: Nil
      true
    } else {
      false
    }
  }

  // called from the consumer thread.. send it to the ack_source
  // do that it calls _ack from the queue thread.
  override def ack(delivery: Delivery) = {
    queue.ack_source.merge((this, delivery))
  }

  def _ack(delivery: Delivery): Unit = {
    assert(!dispatched.isEmpty)
    val entry = if (dispatched.head.delivery == delivery) {
      // this should be the common case...
      val rc = dispatched.head
      dispatched = dispatched.drop(1)
      rc
    } else {
      // but lets also handle the case where we get an ack out of order.
      val rc = dispatched.partition(_.delivery == delivery)
      assert(rc._1.size == 1)
      dispatched = rc._2
      rc._1.head
    }
    queue.messages.ack(entry)
  }

}
