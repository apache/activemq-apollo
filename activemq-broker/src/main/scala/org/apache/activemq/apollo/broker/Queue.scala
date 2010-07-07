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

  val session_manager = new SinkMux[Delivery](messages, dispatchQueue, Delivery)

  // sequence numbers.. used to track what's in the store.
  var message_seq_counter = 1L

  var counter = 0

  val entries = new LinkedNodeList[QueueEntry]()
  val headEntry = new QueueEntry(this, 0L);
  var tailEntry = new QueueEntry(this, next_message_seq)

  entries.addFirst(headEntry)
  headEntry.tombstone

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
  var size = 0

  schedualSlowConsumerCheck

  def restore(storeId: Long, records:Seq[QueueEntryRecord]) = ^{
    this.storeId = storeId
    if( !records.isEmpty ) {
      records.foreach { qer =>
        val entry = new QueueEntry(Queue.this,qer.queueSeq).init(qer)
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
        entry.init(queueDelivery)
        queueDelivery.storeBatch = delivery.storeBatch

        entries.addLast(entry)
        counter += 1;
        enqueue_counter += 1
        enqueue_size += entry.size

        // Do we need to do a persistent enqueue???
        if (queueDelivery.storeBatch != null) {
          queueDelivery.storeBatch.enqueue(entry.toQueueEntryRecord)
        }


        def haveQuickConsumer = fastSubs.find( sub=> sub.pos.seq <= entry.seq ).isDefined

        var dispatched = false
        if( entry.hasSubs || haveQuickConsumer ) {
          // try to dispatch it directly...
          entry.dispatch
        } else {
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


  var checkCounter = 0
  def schedualSlowConsumerCheck:Unit = {

    def slowConsumerCheck = {
      if( retained > 0 ) {

        // Handy for periodically looking at the dispatch state...
//        checkCounter += 1
//        if( !consumerSubs.isEmpty && (checkCounter%100)==0 ) {
//          println("using "+size+" out of "+capacity+" buffer space.");
//          var cur = entries.getHead
//          while( cur!=null ) {
//            if( cur.asLoaded!=null || cur.hasSubs || cur.prefetched>0 ) {
//              println("  => "+cur)
//            }
//            cur = cur.getNext
//          }
//          println("tail: "+tailEntry)
//        }

        // target tune_min_subscription_rate / sec
        val slowCursorDelta = (((tune_slow_subscription_rate) * tune_slow_check_interval) / 1000).toInt
        var idleConsumerCount = 0
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
//                debug("slow interval: %d, %d, %d", sub.slowIntervals, sub.tailParkings, cursorDelta)
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

        // flush tail entries that are still loaded but which have no fast subs that can process them.
        var cur = entries.getTail
        while( cur!=null ) {
          def haveQuickConsumer = fastSubs.find( sub=> sub.pos.seq <= cur.seq ).isDefined
          if( !cur.hasSubs && cur.prefetched==0 && cur.asFlushed==null && !haveQuickConsumer ) {
            // then flush out to make space...
            cur.flush
            cur = cur.getPrevious
          } else {
            cur = null
          }
        }


        // Trigger a swap if we have consumers waiting for messages and we are full..
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
    if (entry.messageKey != -1) {
      val storeBatch = if( sb == null ) {
        host.store.createStoreBatch
      } else {
        sb
      }
      storeBatch.dequeue(entry.toQueueEntryRecord)
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


  val store_flush_source = createSource(new ListEventAggregator[QueueEntry#Loaded](), dispatchQueue)
  store_flush_source.setEventHandler(^ {drain_store_flushes});
  store_flush_source.resume

  def drain_store_flushes() = {
    val data = store_flush_source.getData
    data.foreach { loaded =>
      loaded.flushed
    }
    messages.refiller.run

  }

  val store_load_source = createSource(new ListEventAggregator[(QueueEntry#Flushed, MessageRecord)](), dispatchQueue)
  store_load_source.setEventHandler(^ {drain_store_loads});
  store_load_source.resume


  def drain_store_loads() = {
    val data = store_load_source.getData
    data.foreach { case (flushed,messageRecord) =>
      flushed.loaded(messageRecord)
    }

    data.foreach { case (flushed,_) =>
      if( flushed.entry.hasSubs ) {
        flushed.entry.run
      }
    }
  }

}

object QueueEntry extends Sizer[QueueEntry] {
  def size(value: QueueEntry): Int = value.size
}

class QueueEntry(val queue:Queue, val seq:Long) extends LinkedNode[QueueEntry] with Comparable[QueueEntry] with Runnable with DispatchLogging {
  override protected def log = Queue
  import QueueEntry._

  // Competing subscriptions try to exclusivly aquire the entry.
  var competing:List[Subscription] = Nil
  // These are subscriptions which will not be exclusivly aquiring the entry.
  var browsing:List[Subscription] = Nil
  // The number of subscriptions which have requested this entry to be prefetech (held in memory) so that it's
  // ready for them to get dispatched.
  var prefetched = 0

  // The current state of the entry: Tail | Loaded | Flushed | Tombstone
  var state:EntryState = new Tail



  def init(delivery:Delivery):QueueEntry = {
    this.state = new Loaded(delivery)
    queue.size += size
    this
  }

  def init(qer:QueueEntryRecord):QueueEntry = {
    this.state = new Flushed(qer.messageKey, qer.size)
    this
  }

  def hasSubs = !(competing == Nil && browsing == Nil)

  /**
   * Dispatches this entry to the consumers and continues dispatching subsequent
   * entries if it has subscriptions which accept the dispatch.
   */
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


  def compareTo(o: QueueEntry) = {
    (seq - o.seq).toInt
  }

  def toQueueEntryRecord = {
    val qer = new QueueEntryRecord
    qer.queueKey = queue.storeId
    qer.queueSeq = seq
    qer.messageKey = state.messageKey
    qer.size = state.size
    qer
  }

  override def toString = {
    "{seq: "+seq+", prefetched: "+prefetched+", value: "+state+", competing: "+competing+", browsing: "+browsing+"}"
  }

  /////////////////////////////////////////////////////
  //
  // State delegates..
  //
  /////////////////////////////////////////////////////

  // What state is it in?
  def asTombstone = this.state.asTombstone
  def asFlushed = this.state.asFlushed
  def asLoaded = this.state.asLoaded
  def asTail = this.state.asTail

  // These should not change the current state.
  def size = this.state.size
  def messageKey = this.state.messageKey
  def isFlushedOrFlushing = state.isFlushedOrFlushing
  def dispatch():QueueEntry = state.dispatch

  // These methods may cause a change in the current state.
  def flush:QueueEntry = this.state.flush
  def load:QueueEntry = this.state.load
  def tombstone:QueueEntry = this.state.tombstone

  trait EntryState {

    final def entry:QueueEntry = QueueEntry.this

    def asTail:Tail = null
    def asLoaded:Loaded = null
    def asFlushed:Flushed = null
    def asTombstone:Tombstone = null

    def size:Int
    def dispatch():QueueEntry
    def messageKey:Long
    def isFlushedOrFlushing = false

    def load = entry

    def flush = entry

    def tombstone = {

      // Update the prefetch counter to reflect that this entry is no longer being prefetched.
      var cur = entry
      while( cur!=null && prefetched > 0 ) {
        if( cur.hasSubs ) {
          (cur.browsing ::: cur.competing).foreach { sub => if( sub.prefetched(entry) ) { sub.removePrefetch(entry) } }
        }
        cur = cur.getPrevious
      }

      // if rv and lv are both adjacent tombstones, then this merges the rv
      // tombstone into lv, unlinks rv, and returns lv, otherwise it returns
      // rv.
      def merge(lv:QueueEntry, rv:QueueEntry):QueueEntry = {
        if( lv==null || rv==null) {
          return rv
        }

        val lts = lv.state.asTombstone
        val rts = rv.state.asTombstone

        if( lts==null ||  rts==null ) {
          return rv
        }

        // Sanity check: the the entries are adjacent.. this should
        // always be the case.
        if( lv.seq + lts.count  != rv.seq ) {
          throw new AssertionError("entries are not adjacent.")
        }

        lts.count += rts.count
        rts.count = 0

        if( rv.browsing!=Nil || rv.competing!=Nil ){
          lv.addBrowsing(rv.browsing)
          lv.addCompeting(rv.competing)
          rv.browsing = Nil
          rv.competing = Nil
        }
        rv.unlink
        return lv
      }

      state = new Tombstone()
      merge(entry, getNext)
      merge(getPrevious, entry)
    }

  }

  /**
   * This state is used on the last entry of the queue.  It still has not been initialized
   * with a message, but it may be holding subscriptions.  This state transitions to Loaded
   * once a message is received.
   */
  class Tail extends EntryState {

    override def asTail:Tail = this
    def size = 0
    def messageKey = -1
    def dispatch():QueueEntry = null

    override  def toString = { "tail" }

    override def load = throw new AssertionError("Tail entry cannot be loaded")
    override def flush = throw new AssertionError("Tail entry cannot be flushed")

  }

  /**
   * This state is used while a message is loaded in memory.  A message must be in this state
   * before it can be dispatched to a consumer.  It can transition to Flushed or Tombstone.
   */
  class Loaded(val delivery: Delivery) extends EntryState {

    var aquired = false
    def messageKey = delivery.storeKey
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
        tx.enqueue(toQueueEntryRecord)
        tx.release
      }
    }

    override def flush() = {
      if( queue.host.store!=null && !flushing ) {
        flushing=true
        queue.flushingSize+=size
        if( delivery.storeBatch!=null ) {
          delivery.storeBatch.eagerFlush(^{
            queue.store_flush_source.merge(this)
          })
        } else {
          store
          queue.host.store.flushMessage(messageKey) {
            queue.store_flush_source.merge(this)
          }
        }
      }
      entry
    }

    def flushed() = {
      if( flushing ) {
        queue.flushingSize-=size
        queue.size -= size
        state = new Flushed(delivery.storeKey, size)
      }
    }

    override def load() = {
      if( flushing ) {
        flushing = false
        queue.flushingSize-=size
      }
      entry
    }

    override def tombstone = {
      if( flushing ) {
        flushing = false
        queue.flushingSize-=size
      }
      queue.size -= size
      super.tombstone
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
                  val node = sub.add(entry)
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
          def haveQuickConsumer = queue.fastSubs.find( sub=> sub.pos.seq <= seq ).isDefined
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


  /**
   * Entries which have been deleted get put into the Tombstone state.  Adjacent entries in the
   * Tombstone state get merged into a single entry.
   */
  class Tombstone extends EntryState {

    /** The number of adjacent entries this Tombstone represents. */
    var count = 1L

    def size = 0
    def messageKey = -1

    override def asTombstone = this

    /**
     * Nothing ot dispatch in a Tombstone, move the subscriptions to the next entry.
     */
    def dispatch():QueueEntry = {
      val p = nextOrTail
      p.addBrowsing(browsing)
      p.addCompeting(competing)
      browsing = Nil
      competing = Nil
      p
    }

    override def tombstone = throw new AssertionError("Tombstone entry cannot be tombstoned")
    override  def toString = { "tombstone:{ count: "+count+"}" }

  }

  /**
   * Entries in the Flushed state are not holding the referenced messages in memory anymore.
   * This state can transition to Loaded or Tombstone.
   *
   * TODO: Add a new FlushedList state which can be used to merge multiple
   * adjacent Flushed entries into a single FlushedList state.  This would allow us
   * to support queues with millions of flushed entries without much memory impact.
   */
  class Flushed(val messageKey:Long, val size:Int) extends EntryState {

    var loading = false

    override def asFlushed = this

    override def isFlushedOrFlushing = true

    override def toString = { "flushed:{ loading: "+loading+", size:"+size+"}" }

    // Flushed entries can't be dispatched until
    // they get loaded.
    def dispatch():QueueEntry = {
      if( !loading && hasSubs) {

        // I don't think this should ever happen as we should be prefetching the
        // entry before we dispatch it.
        warn("dispatch called on a flushed entry that is not loading.")

        // ask the subs to fill the prefetches.. that should
        // kick off a load
        (browsing ::: competing).foreach { sub =>
          sub.fillPrefetch
        }
      }
      null
    }

    override def load() = {
      if( !loading ) {
//        trace("Start entry load of message seq: %s", seq)
        // start loading it back...
        loading = true
        queue.loadingSize += size
        queue.host.store.loadMessage(messageKey) { delivery =>
          // pass off to a source so it can aggregate multiple
          // loads to reduce cross thread synchronization
          if( delivery.isDefined ) {
//            debug("Store found message seq: %d", seq)
            queue.store_load_source.merge((this, delivery.get))
          } else {

            debug("Detected store drop of message seq: %d", seq)

            // Looks like someone else removed the message from the store.. lets just
            // tombstone this entry now.
            queue.dispatchQueue {
              tombstone
            }
          }
        }
      }
      entry
    }

    def loaded(messageRecord:MessageRecord) = {
      if( loading ) {
//        debug("Loaded message seq: ", seq )
        loading = false
        queue.loadingSize -= size

        val delivery = new Delivery()
        delivery.message = ProtocolFactory.get(messageRecord.protocol).decode(messageRecord.value)
        delivery.size = messageRecord.size
        delivery.storeKey = messageRecord.key

        queue.size += size
        state = new Loaded(delivery)
      } else {
//        debug("Ignoring store load of: ", messageKey)
      }
    }


    override def tombstone = {
      if( loading ) {
//        debug("Tombstoned, will ignore store load of seq: ", seq)
        loading = false
        queue.loadingSize -= size
      }
      super.tombstone
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
        pos.prefetched -= 1
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
