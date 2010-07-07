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
import org.apache.activemq.util.list.{LinkedNodeList, LinkedNode}
import org.apache.activemq.broker.store.{StoreBatch}
import protocol.ProtocolFactory
import org.apache.activemq.apollo.store.{QueueEntryRecord, MessageRecord}

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
    ack_source.release
    dispatchQueue.release
    session_manager.release
  })


  val ack_source = createSource(new ListEventAggregator[(LinkedQueueEntry, StoreBatch)](), dispatchQueue)
  ack_source.setEventHandler(^ {drain_acks});
  ack_source.resume

  val store_load_source = createSource(new ListEventAggregator[(QueueEntry, MessageRecord)](), dispatchQueue)
  store_load_source.setEventHandler(^ {drain_store_loads});
  store_load_source.resume

  val store_flush_source = createSource(new ListEventAggregator[QueueEntry](), dispatchQueue)
  store_flush_source.setEventHandler(^ {drain_store_flushes});
  store_flush_source.resume

  val session_manager = new SinkMux[Delivery](messages, dispatchQueue, Delivery)

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

  var flushingSize = 0

  /**
   * Tunning options.
   */
  var tune_max_size = 1024 * 32
  var tune_subscription_prefetch = 1024*32
  var tune_max_outbound_size = 1024 * 1204 * 5

  private var size = 0

  object messages extends Sink[Delivery] {

    var refiller: Runnable = null

    def full = if(size >= tune_max_size)
      true
    else
      false

    def offer(delivery: Delivery): Boolean = {
      if (full) {
        false
      } else {

        val entry = tailEntry
        tailEntry = new QueueEntry(Queue.this)
        entry.created(next_message_seq, delivery)

        if( delivery.ack!=null ) {
          delivery.ack(delivery.storeBatch)
        }
        if (delivery.storeKey != -1) {
          delivery.storeBatch.enqueue(entry.createQueueEntryRecord)
          delivery.storeBatch.release
        }

        size += entry.value.size
        entries.addLast(entry)
        counter += 1;

        if( full && host.store!=null ) {
//          swap
        }

        if( entry.hasSubs ) {
          entry.dispatch
        }
        true
      }
    }
  }

  def ack(entry: QueueEntry, sb:StoreBatch) = {
    if (entry.value.ref != -1) {
      val storeBatch = if( sb == null ) {
        host.store.createStoreBatch
      } else {
        sb
      }
      storeBatch.dequeue(entry.createQueueEntryRecord)
      if( sb == null ) {
        storeBatch.release
      }
    }
    if( sb != null ) {
      sb.release
    }

    counter -= 1
    size -= entry.value.size
    entry.tombstone

    if (counter == 0) {
      messages.refiller.run
    }
  }


  def nack(values: LinkedNodeList[LinkedQueueEntry]) = {
    // TODO:
  }


  def drain_acks = {
    ack_source.getData.foreach {
      case (entry, tx) =>
        entry.unlink
        ack(entry.value, tx)
    }
  }
  
  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the DeliveryConsumer trait.  Allows this queue
  // to receive messages from producers.
  //
  /////////////////////////////////////////////////////////////////////

  def matches(message: Delivery) = {true}

  def connect(p: DeliveryProducer) = new DeliverySession {
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

    def offer(delivery: Delivery) = {
      if (session.full) {
        false
      } else {

        // Called from the producer thread before the delivery is
        // processed by the queue's thread.. We don't
        // yet know the order of the delivery in the queue.
        if (delivery.storeKey != -1) {
          // If the message has a store id, then this delivery will
          // need a tx to track the store changes.
          if( delivery.storeBatch == null ) {
            delivery.storeBatch = host.store.createStoreBatch
          } else {
            delivery.storeBatch.retain
          }
        }

        val rc = session.offer(delivery)
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

  private def next_message_seq = {
    val rc = message_seq_counter
    message_seq_counter += 1
    rc
  }

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
        val stored = entry.value.asStored
        if( stored!=null && !stored.loading) {
          // start loading it back...
          stored.loading = true
          host.store.loadMessage(stored.ref) { delivery =>
            // pass off to a source so it can aggregate multiple
            // loads to reduce cross thread synchronization
            if( delivery.isDefined ) {
              store_load_source.merge((entry, delivery.get))
            }
          }
        }
      } else {
        // Chuck the reset out...
        val loaded = entry.value.asLoaded
        if( loaded!=null ) {
          var ref = loaded.delivery.storeKey
          if( ref == -1 ) {
            val tx = host.store.createStoreBatch
            loaded.delivery.storeKey = tx.store(loaded.delivery.createMessageRecord)
            tx.enqueue(entry.createQueueEntryRecord)
            tx.release
          }
          flushingSize += entry.value.size
          host.store.flushMessage(ref) {
            store_flush_source.merge(entry)
          }
        }
      }
      i += 1
    }
  }

  def drain_store_loads() = {
    val data = store_load_source.getData
    var ready = List[QueueEntry]()

    data.foreach { event =>
      val entry = event._1
      val stored = event._2

      val delivery = new Delivery()
      delivery.message = ProtocolFactory.get(stored.protocol).decode(stored.value)
      delivery.size = stored.size
      delivery.storeKey = stored.key

      entry.loaded(delivery)

      size += entry.value.size

      if( entry.hasSubs ) {
        ready ::= entry
      }
    }

    ready.foreach { entry =>
      entry.dispatch
    }
  }

  def drain_store_flushes() = {
    store_flush_source.getData.foreach { entry =>
      flushingSize -= entry.value.size

      // by the time we get called back, subs my be interested in the entry
      // or it may have been acked.
      if( !entry.hasSubs && entry.value.asLoaded!=null ) {
        size += entry.value.size
        entry.stored
      }
    }
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

  def createQueueEntryRecord = {
    val qer = new QueueEntryRecord
    qer.queueKey = queue.storeId
    qer.queueSeq = seq
    qer.messageKey = value.ref
    qer.size = value.size
    qer
  }

  def compareTo(o: QueueEntry) = {
    (seq - o.seq).toInt
  }

  def created(seq:Long, delivery:Delivery) = {
    this.seq = seq
    this.value = new Loaded(delivery)
    this
  }

  def loaded(delivery:Delivery) = {
    this.value = new Loaded(delivery)
    this
  }

  def stored() = {
    val loaded = value.asLoaded
    this.value = new Stored(loaded.delivery.storeKey, loaded.size)
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
    def ref:Long

    def asTombstone:Tombstone = null
    def asStored:Stored = null
    def asLoaded:Loaded = null
  }

  class Tombstone extends EntryType {

    var count = 1L

    def size = 0
    def ref = -1

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

  class Stored(val ref:Long, val size:Int) extends EntryType {

    var loading = false

    override def asStored = this

    // Stored entries can't be dispatched until
    // they get loaded.
    def dispatch():QueueEntry = {
      null
    }
  }

  class Loaded(val delivery: Delivery) extends EntryType {

    var aquired = false
    def ref = delivery.storeKey
    def size = delivery.size
    def flushing = false
    
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
          if (!this.aquired) {
            aquired = true

            var picked: Subscription = null
            var remaining = competing
            while( remaining!=Nil && picked == null ) {
              val sub = remaining.head
              remaining = remaining.drop(1)
              if (sub.matches(delivery)) {
                competingSlowSubs = competingSlowSubs ::: sub :: Nil

                if( !sub.full ) {
                  val node = sub.add(QueueEntry.this)
                  val offering = delivery.copy
                  offering.ack = (tx)=> {
                    queue.ack_source.merge((node, tx))
                  }
                  if (sub.offer(offering)) {
                    picked = sub
                  }
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


class LinkedQueueEntry(val value:QueueEntry) extends LinkedNode[LinkedQueueEntry]

class Subscription(queue:Queue) extends DeliveryProducer {

  def dispatchQueue = queue.dispatchQueue

  var dispatched = new LinkedNodeList[LinkedQueueEntry]
  var session: DeliverySession = null
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
  def full = session.full
  def offer(delivery:Delivery) = session.offer(delivery)

  def add(entry:QueueEntry) = {
    val rc = new LinkedQueueEntry(entry)
    dispatched.addLast(rc)
    rc
  }
}
