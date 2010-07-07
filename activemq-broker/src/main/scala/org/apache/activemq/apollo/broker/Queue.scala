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
import java.util.concurrent.TimeUnit

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
class Queue(val host: VirtualHost, val destination: Destination) extends BaseRetained with Route with DeliveryConsumer with DispatchLogging {
  override protected def log = Queue

  import Queue._

  var consumerSubs = Map[DeliveryConsumer, Subscription]()

  override val dispatchQueue: DispatchQueue = createQueue(destination.toString);
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
  var message_seq_counter = 1L

  val headEntry = new QueueEntry(this).tombstone
  var tailEntry = new QueueEntry(this)

  var counter = 0
  val entries = new LinkedNodeList[QueueEntry]()
  entries.addFirst(headEntry)

  var loadingSize = 0
  var flushingSize = 0
  var storeId: Long = -1L

  /**
   * Tunning options.
   */
  var tune_max_size = 1024 * 256
  var tune_subscription_prefetch = 1024*32
  var tune_max_outbound_size = 1024 * 1204 * 5
  var tune_swap_delay = 100L

  var enqueue_counter = 0L
  var dequeue_counter = 0L
  var enqueue_size = 0L
  var dequeue_size = 0L

  private var size = 0

  def restore(storeId: Long, records:Seq[QueueEntryRecord]) = ^{
    this.storeId = storeId
    if( !records.isEmpty ) {
      records.foreach { qer =>
        val entry = new QueueEntry(Queue.this).flushed(qer)
        entries.addLast(entry)
      }

      message_seq_counter = records.last.queueSeq+1

      counter = records.size
      enqueue_counter += records.size
      debug("restored: "+records.size )
    }
  } >>: dispatchQueue

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
        val queueDelivery = delivery.copy
        entry.created(next_message_seq, queueDelivery)
        queueDelivery.storeBatch = delivery.storeBatch

        size += entry.size
        entries.addLast(entry)
        counter += 1;
        enqueue_counter += 1
        enqueue_size += entry.size

        // Do we need to do a persistent enqueue???
        if (queueDelivery.storeBatch != null) {
          queueDelivery.storeBatch.enqueue(entry.createQueueEntryRecord)
        }

        var swap_check = false
        if( !entry.hasSubs ) {
          // we flush the entry out right away if it looks
          // it wont be needed.

          if( entry.getPrevious.isFlushedOrFlushing ) {
            // in this case take it out of memory too...
            flushingSize += entry.flush
          } else {
            if( slow_consumers ) {
              if( delivery.storeBatch!=null ) {
                // just make it hit the disk quick.. but keep it in memory.
                delivery.storeBatch.eagerFlush(^{})
              }
            } else {
              if( !checking_for_slow_consumers ) {
                checking_for_slow_consumers=true
                val tail_consumer_counter_copy = tail_consumer_counter
                dispatchQueue.dispatchAfter(tune_swap_delay, TimeUnit.MILLISECONDS, ^{
                  if( tail_consumer_counter_copy == tail_consumer_counter ) {
                    slow_consumers = true
                  }
                  checking_for_slow_consumers = false
                })
              }
            }
            swap_check=true
          }
        } else {
          slow_consumers = false
          tail_consumer_counter += 1
          //  entry.dispatch==null if the entry was fully dispatched
          swap_check = entry.dispatch!=null
        }

        // Does it look like we need to start swapping to make room
        // for more messages?
        if( swap_check && host.store!=null &&  full ) {
          val wasAt = dequeue_size
          dispatchQueue.dispatchAfter(tune_swap_delay, TimeUnit.MILLISECONDS, ^{
            // start swapping if was still blocked after a short delay
            if( dequeue_size == wasAt && full ) {
              println("swapping...")
              swap
            }
          })
        }

        // release the store batch...
        if (queueDelivery.storeBatch != null) {
          queueDelivery.storeBatch.release
          queueDelivery.storeBatch = null
        }

        true
      }
    }
  }

  var tail_consumer_counter = 0L
  var checking_for_slow_consumers = false
  var slow_consumers = false

  def ack(entry: QueueEntry, sb:StoreBatch) = {
    if (entry.ref != -1) {
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

    dequeue_counter += 1
    counter -= 1
    dequeue_size += entry.size
    size -= entry.size
    entry.tombstone

    messages.refiller.run
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

        if( delivery.storeBatch!=null ) {
          delivery.storeBatch.retain
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

  /**
   * Prioritizes all the queue entries so that entries most likely to be consumed
   * next are a higher priority.  All messages with the highest priority are loaded
   * and messages with the lowest priority are flushed to make room to accept more
   * messages from the producer.
   */
  def swap():Unit = {

    if( !host.serviceState.isStarted ) {
      return
    }

    class Prio(val entry:QueueEntry) extends Comparable[Prio] {
      var value = 0
      def compareTo(o: Prio) = o.value - value
    }

    val prios = new ArrayList[Prio](entries.size())

    var entry = entries.getHead
    while( entry!=null ) {
      if( entry.asTombstone == null ) {
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
      val remainingSize = size - prio.entry.size
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
        loadingSize += entry.load
        remaining -= entry.size
      } else {
        flushingSize += entry.flush
      }
      i += 1
    }
  }

  def drain_store_loads() = {
    val data = store_load_source.getData
    data.foreach { case (entry,flushed) =>

      loadingSize -= entry.size

      val delivery = new Delivery()
      delivery.message = ProtocolFactory.get(flushed.protocol).decode(flushed.value)
      delivery.size = flushed.size
      delivery.storeKey = flushed.key

      entry.loaded(delivery)

      size += entry.size

    }

    data.foreach { case (entry,_) =>
      if( entry.hasSubs ) {
        entry.run
      }
    }
  }

  def drain_store_flushes() = {
    val data = store_flush_source.getData
    data.foreach { entry =>
      flushingSize -= entry.size

      // by the time we get called back, subs my be interested in the entry
      // or it may have been acked.
      if( !entry.hasSubs && entry.asLoaded!=null ) {
        size -= entry.size
        entry.flushed
      }
    }
    
    messages.refiller.run

  }

}


object QueueEntry extends Sizer[QueueEntry] {
  def size(value: QueueEntry): Int = value.size
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

  def flushed() = {
    val loaded = value.asLoaded
    this.value = new Flushed(loaded.delivery.storeKey, loaded.size)
    this
  }

  def flushed(qer:QueueEntryRecord) = {
    this.seq = qer.queueSeq
    this.value = new Flushed(qer.messageKey, qer.size)
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

  def size = this.value.size
  def flush = this.value.flush
  def load = this.value.load
  def ref = this.value.ref

  def asTombstone = this.value.asTombstone
  def asFlushed = this.value.asFlushed
  def asLoaded = this.value.asLoaded
  def isFlushedOrFlushing = value.isFlushedOrFlushing

  def dispatch():QueueEntry = {
    if( value == null ) {
      // tail entry can't be dispatched.
      null
    } else {
      value.dispatch
    }
  }


  trait EntryType {
    def size:Int
    def dispatch():QueueEntry
    def ref:Long

    def asTombstone:Tombstone = null
    def asFlushed:Flushed = null
    def asLoaded:Loaded = null

    def flush:Int = 0
    def load:Int = 0
    def isFlushedOrFlushing = false
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

  class Flushed(val ref:Long, val size:Int) extends EntryType {

    var loading = false

    override def asFlushed = this

    override def isFlushedOrFlushing = true

    // Flushed entries can't be dispatched until
    // they get loaded.
    def dispatch():QueueEntry = {
      if( !loading ) {
        var remaining = queue.tune_subscription_prefetch - size
        load

        // make sure the next few entries are loaded too..
        var cur = getNext
        while( remaining>0 && cur!=null ) {
          remaining -= cur.size
          val flushed = cur.asFlushed
          if( flushed!=null && !flushed.loading) {
            flushed.load
          }
          cur = getNext
        }

      }
      null
    }

    override def load():Int = {
      if( loading ) {
        0
      } else {
        // start loading it back...
        loading = true
        queue.host.store.loadMessage(ref) { delivery =>
          // pass off to a source so it can aggregate multiple
          // loads to reduce cross thread synchronization
          if( delivery.isDefined ) {
            queue.store_load_source.merge((QueueEntry.this, delivery.get))
          }
        }
        size
      }
    }
  }

  class Loaded(val delivery: Delivery) extends EntryType {

    var aquired = false
    def ref = delivery.storeKey
    def size = delivery.size
    var flushing = false
    
    override def isFlushedOrFlushing = {
      flushing
    }

    override  def asLoaded = this

    def store() = {
      if( delivery.storeKey == -1 ) {
        val tx = queue.host.store.createStoreBatch
        delivery.storeKey = tx.store(delivery.createMessageRecord)
        tx.enqueue(createQueueEntryRecord)
        tx.release
      }
    }

    override def flush():Int = {
      if( flushing ) {
        0
      } else {
        flushing=true

        if( delivery.storeBatch!=null ) {
          delivery.storeBatch.eagerFlush(^{
            queue.store_flush_source.merge(QueueEntry.this)
          })
        } else {
          store
          queue.host.store.flushMessage(ref) {
            queue.store_flush_source.merge(QueueEntry.this)
          }
        }

        size
      }
    }

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
