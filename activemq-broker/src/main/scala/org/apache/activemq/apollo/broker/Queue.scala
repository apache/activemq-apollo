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
import org.apache.activemq.util.TreeMap
import collection.{SortedMap}
import org.fusesource.hawtdispatch.{ScalaDispatch, DispatchQueue, BaseRetained}
import org.apache.activemq.util.TreeMap.TreeEntry
import java.util.{Collections, ArrayList, LinkedList}
import org.apache.activemq.util.list.LinkedNode
import org.apache.activemq.util.list.LinkedNodeList

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


  val headEntry = new QueueEntry(this).tombstone
  var tailEntry = new QueueEntry(this)

  var counter = 0
  val entries = new LinkedNodeList[QueueEntry]()
  entries.addFirst(headEntry)

  /**
   * Tunning options.
   */
  var tune_max_size = 1024 * 32
  var tune_subscription_prefetch = 1024*32
  var tune_max_outbound_size = 1024 * 1204 * 5

  private var size = 0

  def swap() = {

    class Prio(val entry:QueueEntry) extends Comparable[Prio] {
      var value = 0
      def compareTo(o: Prio) = o.value - value
    }

    val prios = new ArrayList[Prio](count)

    var entry = entries.getHead
    while( entry!=null ) {
      if( entry.value.asTombstone == null ) {
        prios.add(new Prio(entry))
      }
      entry = entry.getNext
    }


    /**
     * adds keep priority to the range of entries starting at x
     * and spanning the size provided.
     */
    def prioritize(i:Int, size:Int, p:Int):Unit = {
      val prio = prios.get(i)
      prio.value += p
      val remainingSize = size - prio.entry.value.size
      if( remainingSize > 0 ) {
        val next = i + 1
        if( next < prios.size ) {
          prioritize(next, remainingSize, p-1)
        }
      }
    }

    // Prioritize the entries so that higher priority entries are swapped in,
    // and lower priority entries are swapped out.
    var i = 0
    while( i < prios.size ) {
      val prio = prios.get(i)
      if( prio.entry.hasSubs ) {

        var credits =0;
        if( prio.entry.competing != Nil) {
          credits += prio.entry.competing.size * tune_subscription_prefetch
        } else{
          if( prio.entry.browsing != Nil ) {
            credits += tune_subscription_prefetch
          }
        }
        prioritize(i, credits, 1000)

      }
      i += 1
    }

    Collections.sort(prios)

    var remaining = tune_max_size / 2
    i = 0
    while( i < prios.size ) {
      val prio = prios.get(i)
      val entry = prio.entry
      if( remaining > 0 ) {
        remaining -= entry.value.size
      }

      i += 1
    }

  }

  object messages extends Sink[QueueEntry] {
    var refiller: Runnable = null

    def full = size >= tune_max_size

    def offer(value: QueueEntry): Boolean = {

      if (full) {
        false
      } else {

        val ref = value.value.ref
        if (ref != null) {
          host.database.addMessageToQueue(storeId, value.seq, ref)
          ref.release
        }

        size += value.value.size
        entries.addLast(value)
        counter += 1;

//        if( full ) {
//          swap
//        }

        if( value.hasSubs ) {
          value.dispatch
        }
        true
      }
    }
  }

  def ack(entry: QueueEntry) = {

    if (entry.value.ref != null) {
      host.database.removeMessageFromQueue(storeId, entry.seq, null)
    }

    counter -= 1
    size -= entry.value.size

    entry.tombstone

    if (counter == 0) {
      messages.refiller.run
    }
  }


  def nack(values: List[QueueEntry]) = {
    // TODO:
    for (v <- values) {
    }
  }
  

  val session_manager = new SinkMux[Delivery](MapSink(messages) {x => accept(x)}, dispatchQueue, Delivery)

  val ack_source = createSource(new ListEventAggregator[(Subscription, QueueEntry)](), dispatchQueue)
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
    val rc = tailEntry
    tailEntry = new QueueEntry(this)
    rc.loaded(next_message_seq, delivery)
  }


  private def next_message_seq = {
    val rc = message_seq_counter
    message_seq_counter += 1
    rc
  }


}


object QueueEntry extends Sizer[QueueEntry] {
  def size(value: QueueEntry): Int = value.value.size
}

class QueueEntry(val queue:Queue) extends LinkedNode[QueueEntry] with Comparable[QueueEntry] with Runnable {
  import QueueEntry._

  var seq: Long = -1L
  var competing:List[Subscription] = Nil
  var browsing:List[Subscription] = Nil
  var value:EntryType = null

  def compareTo(o: QueueEntry) = {
    (seq - o.seq).toInt
  }

  def loaded(seq:Long, delivery:Delivery) = {
    this.seq = seq
    this.value = new Loaded(delivery)
    this
  }

  def tombstone = {
    this.value = new Tombstone()
    if( seq != -1L ) {

      def merge(lv:QueueEntry, rv:QueueEntry):Boolean = {
        if( lv==null || rv==null) {
          return false
        }

        val lts = lv.value.asTombstone
        val rts = rv.value.asTombstone

        if( lts==null ||  rts==null ) {
          return false
        }

        if( lv.seq + lts.count  == rv.seq ) {
          lts.count += rts.count
          rts.count = 0

          if( rv.browsing!=Nil || rv.competing!=Nil ){
            lv.addBrowsing(rv.browsing)
            lv.addCompeting(rv.competing)
            rv.browsing = Nil
            rv.competing = Nil
          }

          return true
        } else {
          return false
        }
      }

      // Merge adjacent tombstones
      if( merge(this, getNext) ) {
        getNext.unlink
      }
      if( merge(getPrevious, this) ) {
        this.unlink
      }
    }
    this
  }


  def hasSubs = !(competing == Nil && browsing == Nil)

  def run() = {
    var next = dispatch()
    while( next!=null ) {
      next = next.dispatch
    }
  }

  def dispatch():QueueEntry = {
    if( value == null ) {
      // tail entry can't be dispatched.
      null
    } else {
      value.dispatch
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

  def nextEntry():QueueEntry = {
    var entry = getNext
    if (entry == null) {
      entry = queue.tailEntry
    }
    entry
  }


  trait EntryType {
    def size:Int
    def dispatch():QueueEntry
    def ref:StoredMessageRef

    def asTombstone:Tombstone = null
    def asStored:Stored = null
    def asLoaded:Loaded = null
  }

  class Tombstone extends EntryType {

    var count = 1L

    def size = 0
    def ref = null

    override def asTombstone = this

    def dispatch():QueueEntry = {
      val p = nextEntry
      p.addBrowsing(browsing)
      p.addCompeting(competing)
      browsing = Nil
      competing = Nil
      p
    }
    
  }

  class Stored extends EntryType {

    private var loading = false

    var ref:StoredMessageRef = null
    var size = 0

    override def asStored = this

    // Stored entries can't be dispatched until
    // they get loaded.
    def dispatch():QueueEntry = {
      null
    }
  }

  class Loaded(val delivery: Delivery) extends EntryType {

    var aquired = false
    def ref = delivery.ref
    def size = delivery.size
    
    override  def asLoaded = this

    def dispatch():QueueEntry = {
      if( delivery==null ) {
        // can't dispatch untill the delivery is set.
        null
      } else {

        var browsingSlowSubs:List[Subscription] = Nil
        var browsingFastSubs:List[Subscription] = Nil
        var competingSlowSubs:List[Subscription] = Nil
        var competingFastSubs:List[Subscription] = Nil

        if( browsing!=Nil ) {
          val offering = delivery.copy
          offering.ack = null

          browsing.foreach { sub =>
            if (sub.matches(offering)) {
              if (sub.offer(offering)) {
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

          val offering = delivery.copy
          offering.ack = QueueEntry.this

          if (!this.aquired) {
            aquired = true

            var picked: Subscription = null
            var remaining = competing
            while( remaining!=Nil && picked == null ) {
              val sub = remaining.head
              remaining = remaining.drop(1)

              if (sub.matches(offering)) {
                competingSlowSubs = competingSlowSubs ::: sub :: Nil
                if (sub.offer(offering)) {
                  picked = sub
                }
              } else {
                competingFastSubs = competingFastSubs ::: sub :: Nil
              }
            }

            if (picked == null) {
              aquired = false
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
  }

}

class Subscription(queue:Queue) extends DeliveryProducer {

  def dispatchQueue = queue.dispatchQueue

  var dispatched = List[QueueEntry]()
  var session: Session = null
  var pos:QueueEntry = null

  def position(value:QueueEntry):Unit = {
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
    queue.nack(dispatched)
  }

  def matches(entry:Delivery) = session.consumer.matches(entry)

  def offer(delivery:Delivery) = {
    if (session.offer(delivery)) {
      if( delivery.ack!=null ) {
        val entry = delivery.ack.asInstanceOf[QueueEntry]
        dispatched = dispatched ::: entry :: Nil
      }
      true
    } else {
      false
    }
  }

  // called from the consumer thread.. send it to the ack_source
  // do that it calls _ack from the queue thread.
  override def ack(value: Any) = {
    val entry = value.asInstanceOf[QueueEntry]
    queue.ack_source.merge((this, entry))
  }

  def _ack(entry: QueueEntry): Unit = {
    assert(!dispatched.isEmpty)
    if (dispatched.head == entry) {
      // this should be the common case...
      dispatched = dispatched.drop(1)
    } else {
      // but lets also handle the case where we get an ack out of order.
      val rc = dispatched.partition(_ == entry)
      assert(rc._1.size == 1)
      dispatched = rc._2
    }
    queue.ack(entry)
  }

}
