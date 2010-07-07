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
  var fastSubs = List[Subscription]()

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

  val headEntry = new QueueEntry(this, 0L).tombstone
  var tailEntry = new QueueEntry(this, next_message_seq)

  var counter = 0
  val entries = new LinkedNodeList[QueueEntry]()
  entries.addFirst(headEntry)

  var loadingSize = 0
  var flushingSize = 0
  var storeId: Long = -1L

  //
  // Tuning options.
  //

  /**
   *  The amount of memory buffer space for receiving messages.
   */
  var tune_inbound_buffer = 1024 * 32

  /**
   *  The amount of memory buffer space to use per subscription.
   */
  var tune_subscription_buffer = 1024*32

  /**
   * Subscribers that consume slower than this rate per seconds will be considered
   * slow.
   */
  var tune_slow_subscription_rate = 1000*1024

  /**
   * The number of milliseconds between slow consumer checks.
   */
  var tune_slow_check_interval = 100L

  /**
   * The number of intervals that a consumer must not meeting the subscription rate before it is
   * flagged as a slow consumer. 
   */
  var tune_max_slow_intervals = 10

  var enqueue_counter = 0L
  var dequeue_counter = 0L
  var enqueue_size = 0L
  var dequeue_size = 0L

  private var capacity = tune_inbound_buffer
  private var size = 0

  schedualSlowConsumerCheck

  def restore(storeId: Long, records:Seq[QueueEntryRecord]) = ^{
    this.storeId = storeId
    if( !records.isEmpty ) {
      records.foreach { qer =>
        val entry = new QueueEntry(Queue.this,qer.queueSeq).flushed(qer)
        entries.addLast(entry)
      }

      message_seq_counter = records.last.queueSeq+1

      counter = records.size
      enqueue_counter += records.size
      debug("restored: "+records.size )
    }
  } >>: dispatchQueue

  def addCapacity(amount:Int) = {
    capacity += amount
  }

  object messages extends Sink[Delivery] {

    var refiller: Runnable = null

    def full = if(size >= capacity)
      true
    else
      false

    def offer(delivery: Delivery): Boolean = {
      if (full) {
        false
      } else {

        val entry = tailEntry
        tailEntry = new QueueEntry(Queue.this, next_message_seq)
        val queueDelivery = delivery.copy
        entry.created(queueDelivery)
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

//        var haveQuickConsumer = false
//        fastSubs.foreach{ sub=>
//          if( sub.pos.seq < entry.seq ) {
//            haveQuickConsumer = true
//          }
//        }

        def haveQuickConsumer = fastSubs.find( sub=> !sub.slow && sub.pos.seq <= entry.seq ).isDefined

        var dispatched = false
        if( entry.prefetched > 0 || haveQuickConsumer ) {
          // try to dispatch it directly...
          entry.dispatch
        } else {
//          println("flush: "+delivery.message.getProperty("color"))
          // we flush the entry out right away if it looks
          // it wont be needed.
          entry.flush
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

  def schedualSlowConsumerCheck:Unit = {

    def slowConsumerCheck = {
      if( retained > 0 ) {

        // target tune_min_subscription_rate / sec
        val slowCursorDelta = (((tune_slow_subscription_rate) * tune_slow_check_interval) / 1000).toInt

        var idleConsumerCount = 0


//        if( consumerSubs.isEmpty ) {
//          println("using "+size+" out of "+capacity+" buffer space.");
//          var cur = entries.getHead
//          while( cur!=null ) {
//            if( cur.asLoaded!=null ) {
//              println("  => "+cur)
//            }
//            cur = cur.getNext
//          }
//          println("tail: "+tailEntry)
//        }

        fastSubs = Nil

        consumerSubs.foreach{ case (consumer, sub)=>

          // Skip over new consumers...
          if( sub.cursoredCounter != 0 ) {

            val cursorDelta = sub.cursoredCounter - sub.prevCursoredCounter 
            sub.prevCursoredCounter = sub.cursoredCounter

            // If the subscription is NOT slow if it's been tail parked or
            // it's been parking and cursoring through the data at the tune_slow_subscription_rate 
            if( (sub.tailParked && sub.tailParkings==0) || ( sub.tailParkings > 0 && cursorDelta >= slowCursorDelta ) ) {
              if( sub.slow ) {
                debug("consumer is no longer slow: %s", consumer)
                sub.slowIntervals = 0
              }
            } else {
              if( !sub.slow ) {
                debug("slow interval: %d, %d, %d", sub.slowIntervals, sub.tailParkings, cursorDelta)
                sub.slowIntervals += 1
                if( sub.slow ) {
                  debug("consumer is slow: %s", consumer)
                }
              }
            }

            // has the consumer been stuck at the tail?
            if( sub.tailParked && sub.tailParkings==0 ) {
              idleConsumerCount += 1;
            }

            sub.tailParkings = 0
          }

          if( !sub.slow ) {
            fastSubs ::= sub
          }
        }

        // Trigger a swap if we have slow consumers and we are full..
        if( idleConsumerCount > 0 && messages.full && flushingSize==0 ) {
          swap
        }
        schedualSlowConsumerCheck
      }
    }

    dispatchQueue.dispatchAfter(tune_slow_check_interval, TimeUnit.MILLISECONDS, ^{
      slowConsumerCheck
    })
  }

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
    
//    println("acked... full: "+messages.full)
    messages.refiller.run
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
      fastSubs ::= subscription
      addCapacity( tune_subscription_buffer )
    }
  } >>: dispatchQueue

  def unbind(consumers: List[DeliveryConsumer]) = releasing(consumers) {
    for (consumer <- consumers) {
      consumerSubs.get(consumer) match {
        case Some(cs) =>
          cs.close
          consumerSubs -= consumer
          fastSubs = fastSubs.filterNot(_ eq cs)
          addCapacity( -tune_subscription_buffer )
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
    
    debug("swapping...")

    var entry = entries.getHead
    while( entry!=null ) {
      if( entry.asTombstone == null ) {

        val loaded = entry.asLoaded

        // Keep around prefetched and loaded entries.
        if( entry.prefetched < 0 || (loaded!=null && loaded.aquired)) {
          entry.load
        } else {
          // flush the the others out of memory.
          entry.flush
        }
      }

      entry = entry.getNext
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

class QueueEntry(val queue:Queue, val seq:Long) extends LinkedNode[QueueEntry] with Comparable[QueueEntry] with Runnable {
  import QueueEntry._

  var competing:List[Subscription] = Nil
  var browsing:List[Subscription] = Nil
  var prefetched = 0

  var value:EntryType = new Tail

  override def toString = {
    "{seq: "+seq+", prefetched: "+prefetched+", value: "+value+", competing: "+competing+", browsing: "+browsing+"}"
  }

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


  def created(delivery:Delivery) = {
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
    this.value = new Flushed(qer.messageKey, qer.size)
    this
  }

  def tombstone = {

    // remove from prefetch counters..
    var cur = this;
    while( prefetched > 0 ) {
      if( cur.hasSubs ) {
        (cur.browsing ::: cur.competing).foreach { sub => if( sub.prefetched(cur) ) { sub.removePrefetch(cur) } }
      }
      cur = cur.getPrevious
    }

    this.value = new Tombstone()
    if( seq != 0L ) {

      def merge(lv:QueueEntry, rv:QueueEntry):Unit = {
        if( lv==null || rv==null) {
          return
        }

        val lts = lv.value.asTombstone
        val rts = rv.value.asTombstone

        if( lts==null ||  rts==null ) {
          return
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

          rv.unlink
        }
      }

      // Merge adjacent tombstones
      merge(this, getNext)
      merge(getPrevious, this)
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

  def nextOrTail():QueueEntry = {
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
  def asTail = this.value.asTail
  def isFlushedOrFlushing = value.isFlushedOrFlushing

  def dispatch():QueueEntry = value.dispatch

  trait EntryType {
    def size:Int
    def dispatch():QueueEntry
    def ref:Long

    def asTail:Tail = null
    def asLoaded:Loaded = null
    def asFlushed:Flushed = null
    def asTombstone:Tombstone = null

    def flush = {}
    def load = {}
    def isFlushedOrFlushing = false
  }

  class Tail extends EntryType {
    override def asTail:Tail = this
    def size = 0
    def ref = -1

    def dispatch():QueueEntry = null
  }

  class Tombstone extends EntryType {

    var count = 1L

    def size = 0
    def ref = -1

    override def asTombstone = this

    def dispatch():QueueEntry = {
      val p = nextOrTail
      p.addBrowsing(browsing)
      p.addCompeting(competing)
      browsing = Nil
      competing = Nil
      p
    }

    override  def toString = { "ts:{ count: "+count+"}" }

  }

  class Flushed(val ref:Long, val size:Int) extends EntryType {

    var loading = false

    override def asFlushed = this

    override def isFlushedOrFlushing = true

    override def toString = { "flushed:{ loading: "+loading+", size:"+size+"}" }

    // Flushed entries can't be dispatched until
    // they get loaded.
    def dispatch():QueueEntry = {
      if( !loading ) {
        var remaining = queue.tune_subscription_buffer - size
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

    override def load() = {
      if( !loading ) {
        // start loading it back...
        loading = true
        queue.loadingSize += size
        queue.host.store.loadMessage(ref) { delivery =>
          // pass off to a source so it can aggregate multiple
          // loads to reduce cross thread synchronization
          if( delivery.isDefined ) {
            queue.store_load_source.merge((QueueEntry.this, delivery.get))
          }
        }
      }
    }
  }

  class Loaded(val delivery: Delivery) extends EntryType {

    var aquired = false
    def ref = delivery.storeKey
    def size = delivery.size
    var flushing = false
    
    override def toString = { "loaded:{ flushing: "+flushing+", aquired: "+aquired+", size:"+size+"}" }

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

    override def flush():Unit = {
      if( queue.host.store!=null && !flushing ) {
        flushing=true
        queue.flushingSize+=size

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
      }
    }

    def dispatch():QueueEntry = {
      if( delivery==null ) {
        // can't dispatch until the delivery is set.
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
          val p = nextOrTail
          p.addBrowsing(browsingFastSubs)
          p.addCompeting(competingFastSubs)

          // flush this entry out if it's not going to be needed soon.
          def haveQuickConsumer = queue.fastSubs.find( sub=> !sub.slow && sub.pos.seq <= seq ).isDefined
          if( !hasSubs && prefetched==0 && !aquired && !haveQuickConsumer ) {
            // then flush out to make space...
            flush
          }

          p
        } else {
          null
        }
      }
    }
  }

}


class LinkedQueueEntry(val value:QueueEntry) extends LinkedNode[LinkedQueueEntry]

class Subscription(queue:Queue) extends DeliveryProducer with DispatchLogging {
  override protected def log = Queue

  def dispatchQueue = queue.dispatchQueue

  var dispatched = new LinkedNodeList[LinkedQueueEntry]
  var session: DeliverySession = null
  var pos:QueueEntry = null

  var cursoredCounter = 0L

  // Vars used to detect slow consumers.
  var prevCursoredCounter = 0L
  var tailParkings = 0
  var slowIntervals = 0

  def slow = slowIntervals > queue.tune_max_slow_intervals

  var nextPrefetchPos:QueueEntry = null
  var prefetchSize = 0


  override def toString = "{ prefetchSize: "+prefetchSize+", pos: "+(if(pos==null) null else pos.seq)+" nextPrefetchPos: "+(if(nextPrefetchPos==null) null else nextPrefetchPos.seq)+" }"

  def position(value:QueueEntry):Unit = {
    if( value!=null ) {
      // setting a new position..
      if( pos!=null ) {
        // Remove the previous pos from the prefetch counters.
        removePrefetch(pos)
        cursoredCounter += pos.size
      }
    } else {
      // setting null pos, happens when the sub is closed.
      var cur = pos

      // clean up it's prefetch counters on the entries..
      while( cur!=nextPrefetchPos ) {
        cur.prefetched -= 1
        cur = cur.nextOrTail
      }
    }
    pos = value
    session.refiller = pos
    if( tailParked ) {
      tailParkings += 1
    }
  }


  def prefetched(value:QueueEntry) = {
    pos.seq <= value.seq && value.seq < nextPrefetchPos.seq
  }

  def removePrefetch(value:QueueEntry):Unit = {
//    trace("prefetch rm: "+value.seq)
    value.prefetched -= 1
    prefetchSize -= value.size
    fillPrefetch()
  }

  def fillPrefetch() = {
    // attempts to fill the prefetch...
    while(prefetchSize < queue.tune_subscription_buffer && nextPrefetchPos.asTail==null ) {
      addPrefetch(nextPrefetchPos)
    }
  }

  def addPrefetch(value:QueueEntry):Unit = {
//    trace("prefetch add: "+value.seq)
    prefetchSize += value.size
    value.prefetched += 1
    value.load
    nextPrefetchPos = value.nextOrTail
  }

  def tailParked = pos eq queue.tailEntry

  def connect(consumer: DeliveryConsumer) = {
    session = consumer.connect(this)
    addPrefetch(queue.headEntry)
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
