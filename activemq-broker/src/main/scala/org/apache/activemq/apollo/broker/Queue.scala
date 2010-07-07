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
import org.apache.activemq.util.list.{LinkedNodeList, LinkedNode}
import org.apache.activemq.broker.store.{StoreUOW}
import protocol.ProtocolFactory
import java.util.concurrent.TimeUnit
import java.util.{HashSet, Collections, ArrayList, LinkedList}
import org.apache.activemq.apollo.store.{QueueEntryGroup, QueueEntryRecord, MessageRecord}

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
class Queue(val host: VirtualHost, val destination: Destination, val queueKey: Long = -1L) extends BaseRetained with Route with DeliveryConsumer with BaseService with DispatchLogging {
  override protected def log = Queue

  import Queue._

  var all_subscriptions = Map[DeliveryConsumer, Subscription]()
  var fast_subscriptions = List[Subscription]()

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


  val ack_source = createSource(new ListEventAggregator[(Subscription#AcquiredQueueEntry, StoreUOW)](), dispatchQueue)
  ack_source.setEventHandler(^ {drain_acks});
  ack_source.resume

  val session_manager = new SinkMux[Delivery](messages, dispatchQueue, Delivery)

  // sequence numbers.. used to track what's in the store.
  var message_seq_counter = 1L

  val entries = new LinkedNodeList[QueueEntry]()
  val head_entry = new QueueEntry(this, 0L);
  var tail_entry = new QueueEntry(this, next_message_seq)

  entries.addFirst(head_entry)
  head_entry.tombstone

  var loading_size = 0
  var flushing_size = 0


  //
  // Tuning options.
  //

  /**
   *  The amount of memory buffer space for receiving messages.
   */
  var tune_producer_buffer = 1024*32

  /**
   *  The amount of memory buffer space to use per subscription.
   */
  var tune_consumer_buffer = 1024*64

  /**
   * Subscribers that consume slower than this rate per seconds will be considered
   * slow.
   */
  var tune_slow_subscription_rate = 1000*1024

  /**
   * The number of milliseconds between slow consumer checks.
   */
  var tune_slow_check_interval = 200L

  /**
   * Should this queue persistently store it's entries?
   */
  def tune_persistent = host.store !=null

  /**
   * Should messages be flushed or swapped out of memory if
   * no consumers need the message?
   */
  def tune_flush_to_store = tune_persistent

  /**
   * The number max number of flushed queue entries to load
   * for the store at a time.  Not that Flushed entires are just
   * reference pointers to the actual messages.  When not loaded,
   * the batch is referenced as sequence range to conserve memory.
   */
  def tune_entry_group_size = 10000

  /**
   * The number of intervals that a consumer must not meeting the subscription rate before it is
   * flagged as a slow consumer. 
   */
  var tune_max_slow_intervals = 10

  var enqueue_item_counter = 0L
  var dequeue_item_counter = 0L
  var enqueue_size_counter = 0L
  var dequeue_size_counter = 0L
  var nack_item_counter = 0L
  var nack_size_counter = 0L

  var flushed_items = 0L

  def queue_size = enqueue_size_counter - dequeue_size_counter
  def queue_items = enqueue_item_counter - dequeue_item_counter

  private var capacity = tune_producer_buffer
  var size = 0

  protected def _start(onCompleted: Runnable) = {

    def completed: Unit = {
      // by the time this is run, consumers and producers may have already joined.
      onCompleted.run
      display_stats
      schedual_slow_consumer_check
      // wake up the producers to fill us up...
      if (messages.refiller != null) {
        messages.refiller.run
      }

      // kick of dispatching to the consumers.
      all_subscriptions.valuesIterator.foreach( _.refill_prefetch )
      dispatchQueue << head_entry
    }

    if( tune_persistent ) {
      host.store.listQueueEntryGroups(queueKey, tune_entry_group_size) { groups=>
        dispatchQueue {
          if( !groups.isEmpty ) {

            // adjust the head tombstone.
            head_entry.as_tombstone.count = groups.head.firstSeq

            var last:QueueEntryGroup = null
            groups.foreach { group =>

              // if groups were not adjacent.. create tombstone entry.
              if( last!=null && (last.lastSeq+1 != group.firstSeq) ) {
                val entry = new QueueEntry(Queue.this, last.lastSeq+1)
                entry.tombstone.as_tombstone.count = group.firstSeq - (last.lastSeq+1)
                entries.addLast(entry)
              }

              val entry = new QueueEntry(Queue.this, group.firstSeq).init(group)
              entries.addLast(entry)

              message_seq_counter = group.lastSeq
              enqueue_item_counter += group.count
              enqueue_size_counter += group.size

              last = group
            }

            debug("restored: "+enqueue_item_counter)
          }
          completed
        }
      }
    } else {
      completed
    }
  }

  protected def _stop(onCompleted: Runnable) = {
    throw new AssertionError("Not implemented.");
  }

  def addCapacity(amount:Int) = {
    capacity += amount
  }

  object messages extends Sink[Delivery] {

    var refiller: Runnable = null

    def full = (size >= capacity) || !serviceState.isStarted

    def offer(delivery: Delivery): Boolean = {
      if (full) {
        false
      } else {

        val entry = tail_entry
        tail_entry = new QueueEntry(Queue.this, next_message_seq)
        val queueDelivery = delivery.copy
        entry.init(queueDelivery)
        
        if( tune_persistent ) {
          queueDelivery.uow = delivery.uow
        }

        entries.addLast(entry)
        enqueue_item_counter += 1
        enqueue_size_counter += entry.size

        // Do we need to do a persistent enqueue???
        if (queueDelivery.uow != null) {
          entry.as_loaded.store
        }

        def haveQuickConsumer = fast_subscriptions.find( sub=> sub.pos.seq <= entry.seq ).isDefined

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
        if (queueDelivery.uow != null) {
          queueDelivery.uow.release
          queueDelivery.uow = null
        }

        true
      }
    }
  }


  var check_counter = 0
  def display_stats: Unit = {
    info("contains: %d messages worth %,.2f MB of data, producers are %s, %d/%d buffer space used.", queue_items, (queue_size.toFloat / (1024 * 1024)), {if (messages.full) "being throttled" else "not being throttled"}, size, capacity)
    info("total messages enqueued %d, dequeues %d ", enqueue_item_counter, dequeue_item_counter)
  }

  def display_active_entries: Unit = {
    var cur = entries.getHead
    var total_items = 0L
    var total_size = 0L
    while (cur != null) {
      if (cur.is_loaded || cur.hasSubs || cur.is_prefetched || cur.is_flushed_group ) {
        info("  => " + cur)
      }

      total_size += cur.size
      if (cur.is_flushed || cur.is_loaded) {
        total_items += 1
      } else if (cur.is_flushed_group ) {
        total_items += cur.as_flushed_group.items
      }
      
      cur = cur.getNext
    }
    info("tail: " + tail_entry)

    // sanitiy checks..
    if(total_items != queue_items) {
      warn("queue_items mismatch, found %d, expected %d", total_size, queue_items)
    }
    if(total_size != queue_size) {
      warn("queue_size mismatch, found %d, expected %d", total_size, queue_size)

    }
  }

  def schedual_slow_consumer_check:Unit = {

    def slowConsumerCheck = {
      if( retained > 0 ) {

        // Handy for periodically looking at the dispatch state...
        check_counter += 1

        if( (check_counter%10)==0  ) {
          display_stats
        }

        if( (check_counter%25)==0 ) {
//          if (!all_subscriptions.isEmpty) {
//            display_active_entries
//          }
        }

        // target tune_min_subscription_rate / sec
        val slow_cursor_delta = (((tune_slow_subscription_rate) * tune_slow_check_interval) / 1000).toInt
        var idleConsumerCount = 0


        var startedWithFastSubs = !fast_subscriptions.isEmpty
        fast_subscriptions = Nil

        all_subscriptions.foreach{ case (consumer, sub)=>

          // Skip over new consumers...
          if( sub.advanced_size != 0 ) {

            val cursor_delta = sub.advanced_size - sub.last_cursored_size
            sub.last_cursored_size = sub.advanced_size

            // If the subscription is NOT slow if it's been tail parked or
            // it's been parking and cursoring through the data at the tune_slow_subscription_rate 
            if( (sub.tail_parked && sub.tail_parkings==0) || ( sub.tail_parkings > 0 && cursor_delta >= slow_cursor_delta ) ) {
              if( sub.slow ) {
                debug("subscription is now fast: %s", sub)
                sub.slow_intervals = 0
              }
            } else {
              if( !sub.slow ) {
                trace("slow interval: %d, %d, %d", sub.slow_intervals, sub.tail_parkings, cursor_delta)
                sub.slow_intervals += 1
                if( sub.slow ) {
                  debug("subscription is now slow: %s", sub)
                }
              }
            }

            // has the consumer been stuck at the tail?
            if( sub.tail_parked && sub.tail_parkings==0 ) {
              idleConsumerCount += 1;
            }

            sub.tail_parkings = 0
          }

          if( !sub.slow ) {
            fast_subscriptions ::= sub
          }
        }


        // If we no longer have fast subs...
        if( startedWithFastSubs && fast_subscriptions.isEmpty ) {
          // Flush out the tail entries..
          var cur = entries.getTail
          while( cur!=null ) {
            if( !cur.hasSubs && !cur.is_prefetched ) {
              cur
            }
            cur = cur.getPrevious
          }


        }

        // flush tail entries that are still loaded but which have no fast subs that can process them.
        var cur = entries.getTail
        while( cur!=null ) {
          def haveQuickConsumer = fast_subscriptions.find( sub=> sub.pos.seq <= cur.seq ).isDefined
          if( cur.is_loaded && !cur.hasSubs && !cur.is_prefetched && !cur.as_loaded.acquired && !haveQuickConsumer ) {
            // then flush out to make space...
            cur.flush
            cur = cur.getPrevious
          } else {
            cur = null
          }
        }


        // Trigger a swap if we have consumers waiting for messages and we are full..
        if( idleConsumerCount > 0 && messages.full && flushing_size==0 ) {
          swap
        }
        schedual_slow_consumer_check
      }
    }

    dispatchQueue.dispatchAfter(tune_slow_check_interval, TimeUnit.MILLISECONDS, ^{
      slowConsumerCheck
    })
  }


  def drain_acks = {
    ack_source.getData.foreach {
      case (entry, tx) =>
        entry.ack(tx)
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

    dispatchQueue {
      addCapacity( tune_producer_buffer )
    }

    override def consumer = Queue.this

    override def producer = p

    val session = session_manager.open(producer.dispatchQueue)

    def close = {
      session_manager.close(session)
      dispatchQueue {
        addCapacity( -tune_producer_buffer )
      }
      release
    }

    // Delegate all the flow control stuff to the session
    def full = session.full

    def offer(delivery: Delivery) = {
      if (session.full) {
        false
      } else {

        if( tune_persistent && delivery.uow!=null ) {
          delivery.uow.retain
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

  def connected(values: List[DeliveryConsumer]) = bind(values)

  def bind(values: List[DeliveryConsumer]) = retaining(values) {
    for (consumer <- values) {
      val subscription = new Subscription(this)
      subscription.open(consumer)
      all_subscriptions += consumer -> subscription
      fast_subscriptions ::= subscription
      addCapacity( tune_consumer_buffer )
    }
  } >>: dispatchQueue

  def unbind(values: List[DeliveryConsumer]) = releasing(values) {
    for (consumer <- values) {
      all_subscriptions.get(consumer) match {
        case Some(subscription) =>
          all_subscriptions -= consumer
          fast_subscriptions = fast_subscriptions.filterNot(_ eq subscription)
          subscription.close
          addCapacity( -tune_consumer_buffer )
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
      if( entry.as_tombstone == null ) {

        val loaded = entry.as_loaded

        // Keep around prefetched and loaded entries.
        if( entry.is_prefetched || (loaded!=null && loaded.acquired)) {
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

  def collocate(value:DispatchQueue):Unit = {
    if( value.getTargetQueue ne dispatchQueue.getTargetQueue ) {
      println(dispatchQueue.getLabel+" co-locating with: "+value.getLabel);
      this.dispatchQueue.setTargetQueue(value.getTargetQueue)
    }
  }
  
}

object QueueEntry extends Sizer[QueueEntry] {
  def size(value: QueueEntry): Int = value.size
}

class QueueEntry(val queue:Queue, val seq:Long) extends LinkedNode[QueueEntry] with Comparable[QueueEntry] with Runnable with DispatchLogging {
  override protected def log = Queue
  import QueueEntry._

  // Subscriptions waiting to dispatch this entry.
  var subscriptions:List[Subscription] = Nil
  // The number of subscriptions which have requested this entry to be prefeteched (held in memory) so that it's
  // ready for them to get dispatched.
  var prefetched = 0

  // The current state of the entry: Tail | Loaded | Flushed | Tombstone
  var state:EntryState = new Tail

  def is_prefetched = prefetched>0

  def init(delivery:Delivery):QueueEntry = {
    this.state = new Loaded(delivery, false)
    queue.size += size
    this
  }

  def init(qer:QueueEntryRecord):QueueEntry = {
    this.state = new Flushed(qer.messageKey, qer.size)
    this
  }

  def init(group:QueueEntryGroup):QueueEntry = {
    val count = (((group.lastSeq+1)-group.firstSeq)).toInt
    val tombstones = count-group.count
    this.state = new FlushedGroup(count, group.size, tombstones)
    this
  }

  def hasSubs = !(subscriptions == Nil )

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

  def addSubscriptions(l:List[Subscription]) = {
    subscriptions :::= l
  }


  def removeSubscriptions(s:Subscription) = {
    subscriptions = subscriptions.filterNot(_ == s)
  }

  def nextOrTail():QueueEntry = {
    var entry = getNext
    if (entry == null) {
      entry = queue.tail_entry
    }
    entry
  }


  def compareTo(o: QueueEntry) = {
    (seq - o.seq).toInt
  }

  def toQueueEntryRecord = {
    val qer = new QueueEntryRecord
    qer.queueKey = queue.queueKey
    qer.queueSeq = seq
    qer.messageKey = state.messageKey
    qer.size = state.size
    qer
  }

  override def toString = {
    "{seq: "+seq+", prefetched: "+prefetched+", value: "+state+", subscriptions: "+subscriptions+"}"
  }

  /////////////////////////////////////////////////////
  //
  // State delegates..
  //
  /////////////////////////////////////////////////////

  // What state is it in?
  def as_tombstone = this.state.as_tombstone
  def as_flushed = this.state.as_flushed
  def as_flushed_group = this.state.as_flushed_group
  def as_loaded = this.state.as_loaded
  def as_tail = this.state.as_tail

  def is_tail = this == queue.tail_entry
  def is_head = this == queue.head_entry

  def is_loaded = as_loaded!=null
  def is_flushed = as_flushed!=null
  def is_flushed_group = as_flushed_group!=null
  def is_tombstone = as_tombstone!=null

  // These should not change the current state.
  def size = this.state.size
  def messageKey = this.state.messageKey
  def is_flushed_or_flushing = state.is_flushed_or_flushing
  def dispatch():QueueEntry = state.dispatch

  // These methods may cause a change in the current state.
  def flush:QueueEntry = this.state.flush
  def load:QueueEntry = this.state.load
  def tombstone:QueueEntry = this.state.tombstone

  trait EntryState {

    final def entry:QueueEntry = QueueEntry.this

    def as_tail:Tail = null
    def as_loaded:Loaded = null
    def as_flushed:Flushed = null
    def as_flushed_group:FlushedGroup = null
    def as_tombstone:Tombstone = null

    def size:Int
    def dispatch():QueueEntry
    def messageKey:Long
    def is_flushed_or_flushing = false

    def load = entry

    def flush = entry

    def prefetch_remove = {
      var rc = List[Subscription]()
      if( queue.tune_flush_to_store ) {
        // Update the prefetch counter to reflect that this entry is no longer being prefetched.
        var cur = entry
        while( cur!=null && is_prefetched ) {
          if( cur.hasSubs ) {
            (cur.subscriptions).foreach { sub =>
              if( sub.is_prefetched(entry) ) {
                sub.remove_from_prefetch(entry)
                rc ::= sub
              }
            }
          }
          cur = cur.getPrevious
        }
        assert(!is_prefetched, "entry should not be prefetched.")
      }
      rc
    }

    def tombstone = {

      var refill_preftch_list = prefetch_remove

      // if rv and lv are both adjacent tombstones, then this merges the rv
      // tombstone into lv, unlinks rv, and returns lv, otherwise it returns
      // rv.
      def merge(lv:QueueEntry, rv:QueueEntry):QueueEntry = {
        if( lv==null || rv==null) {
          return rv
        }

        val lts = lv.state.as_tombstone
        val rts = rv.state.as_tombstone

        if( lts==null ||  rts==null ) {
          return rv
        }

        // Sanity check: the the entries are adjacent.. this should
        // always be the case.
        assert( lv.seq + lts.count  == rv.seq , "entries are not adjacent.")

        lts.count += rts.count
        rv.dispatch // moves the subs to the next entry.
        rv.unlink
        return lv
      }

      state = new Tombstone()
      merge(entry, getNext)
      val rc = merge(getPrevious, entry)

      refill_preftch_list.foreach( _.refill_prefetch )

      rc.run // dispatch to move the subs to the next entry..
      rc
    }

  }

  /**
   * This state is used on the last entry of the queue.  It still has not been initialized
   * with a message, but it may be holding subscriptions.  This state transitions to Loaded
   * once a message is received.
   */
  class Tail extends EntryState {

    override def as_tail:Tail = this
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
  class Loaded(val delivery: Delivery, var store_completed:Boolean) extends EntryState {

    var acquired = false
    def messageKey = delivery.storeKey
    def size = delivery.size
    var flushing = false

    override def toString = { "loaded:{ flushing: "+flushing+", acquired: "+acquired+", size:"+size+"}" }

    override def is_flushed_or_flushing = {
      flushing
    }

    override  def as_loaded = this

    def store = {
      delivery.uow.enqueue(toQueueEntryRecord)
      delivery.uow.onComplete(^{
        queue.store_flush_source.merge(this)
      })
    }

    override def flush() = {
      if( queue.tune_flush_to_store ) {
        if( store_completed ) {
          flushing=true
          flushed
        } else {
          if( !flushing ) {
            flushing=true
            queue.flushing_size+=size

            // The storeBatch is only set when called from the messages.offer method
            if( delivery.uow!=null ) {
              delivery.uow.completeASAP
            } else {

              // Are swapping out a non-persistent message?
              if( delivery.storeKey == -1 ) {
                
                delivery.uow = queue.host.store.createStoreUOW
                val uow = delivery.uow
                delivery.storeKey = uow.store(delivery.createMessageRecord)
                store
                uow.completeASAP
                uow.release
                delivery.uow = null

              } else {

                queue.host.store.flushMessage(messageKey) {
                  queue.store_flush_source.merge(this)
                }

              }

            }
          }
        }
      }

      entry
    }

    def flushed() = {
      store_completed = true
      delivery.uow = null
      if( flushing ) {
        queue.flushing_size-=size
        queue.size -= size
        state = new Flushed(delivery.storeKey, size)
      }
    }

    override def load() = {
      if( flushing ) {
        flushing = false
        queue.flushing_size-=size
      }
      entry
    }

    override def tombstone = {
      if( flushing ) {
        flushing = false
        queue.flushing_size-=size
      }
      queue.size -= size
      super.tombstone
    }

    def dispatch():QueueEntry = {

      // Nothing to dispatch if we don't have subs..
      if( subscriptions==Nil ) {
        // This usualy happens when a new consumer connects, it's not marked as slow but
        // is not at the tail.  And this entry is an entry just sent by a producer.
        return nextOrTail
      }

      // can't dispatch until the delivery is set.
      if( delivery==null ) {
        // TODO: check to see if this ever happens
        return null
      }

      var heldBack:List[Subscription] = Nil
      var advancing:List[Subscription] = Nil


      var acquiringSub: Subscription = null
      subscriptions.foreach{ sub=>

        if( sub.browser ) {
          if (!sub.matches(delivery)) {
            // advance: not interested.
            advancing ::= sub
          } else {
            if (sub.offer(delivery)) {
              // advance: accepted...
              advancing ::= sub
            } else {
              // hold back: flow controlled
              heldBack ::= sub
            }
          }

        } else {
          if( acquired ) {
            // advance: another sub already acquired this entry..
            advancing = advancing ::: sub :: Nil
          } else {
            if (!sub.matches(delivery)) {
              // advance: not interested.
              advancing = advancing ::: sub :: Nil
            } else {
              if( sub.full ) {
                // hold back: flow controlled
                heldBack = heldBack ::: sub :: Nil
              } else {
                // advance: accepted...
                acquiringSub = sub
                acquired = true

                val acquiredQueueEntry = sub.acquire(entry)
                val acquiredDelivery = delivery.copy
                acquiredDelivery.ack = (tx)=> {
                  queue.ack_source.merge((acquiredQueueEntry, tx))
                }

                assert(sub.offer(acquiredDelivery), "sub should have accepted, it had reported not full earlier.")
              }
            }
          }
        }
      }

      // The acquiring sub is added last to the list so that
      // the other competing subs get first dibs at the next entry.
      if( acquiringSub != null ) {

        // Advancing may need to be held back because the sub's prefetch is full.
        if( acquiringSub.prefetchFull ) {
          advancing = advancing ::: acquiringSub :: Nil
        } else {
          heldBack = heldBack ::: acquiringSub :: Nil
        }
      }

      // The held back subs stay on this entry..
      subscriptions = heldBack

      // the advancing subs move on to the next entry...
      if ( advancing!=Nil ) {

        val next = nextOrTail
        next.addSubscriptions(advancing)
        advancing.foreach(_.advance(next))

        // flush this entry out if it's not going to be needed soon.
        def haveQuickConsumer = queue.fast_subscriptions.find( sub=> sub.pos.seq <= seq ).isDefined
        if( !hasSubs && !is_prefetched && !acquired && !haveQuickConsumer ) {
          // then flush out to make space...
          flush
        }

        return next
      } else {
        return null
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

    override def as_tombstone = this

    /**
     * Nothing ot dispatch in a Tombstone, move the subscriptions to the next entry.
     */
    def dispatch():QueueEntry = {
      assert(prefetched==0, "tombstones should never be prefetched.")

      val next = nextOrTail
      next.addSubscriptions(subscriptions)
      subscriptions.foreach(_.advance(next))
      subscriptions = Nil
      next
    }

    override def tombstone = throw new AssertionError("Tombstone entry cannot be tombstoned")
    override  def toString = { "tombstone:{ count: "+count+"}" }

  }


  class FlushedGroup(
   /** The number of adjacent entries this FlushedGroup represents. */
   var count:Long,
   /** size in bytes of the group */
   var size:Int,
   /** The number of tombstone entries in the groups */
   var tombstones:Int ) extends EntryState {


    def items = count - tombstones

    def messageKey = -1

    var loading = false

    override def as_flushed_group = this

    override def is_flushed_or_flushing = true

    override def toString = { "flushed_group:{ loading: "+loading+", items: "+items+", size: "+size+"}" }

    // Flushed entries can't be dispatched until
    // they get loaded.
    def dispatch():QueueEntry = {
      null
    }

    override def load() = {
      if( !loading ) {
        loading = true
        queue.host.store.listQueueEntries(queue.queueKey, seq, seq+count-1) { records =>
          queue.dispatchQueue {

            var item_count=0
            var size_count=0
            var last:QueueEntryRecord = null

            val tmpList = new LinkedNodeList[QueueEntry]()
            records.foreach { record =>

              // if entries were not adjacent.. create tombstone entry.
              if( last!=null && (last.queueSeq+1 != record.queueSeq) ) {
                val entry = new QueueEntry(queue, last.queueSeq+1)
                entry.tombstone.as_tombstone.count = record.queueSeq - (last.queueSeq+1)
                tmpList.addLast(entry)
              }

              val entry = new QueueEntry(queue, record.queueSeq).init(record)
              tmpList.addLast(entry)

              item_count += 1
              size_count += record.size
              last = record
            }

            // we may need to adjust the enqueue count if entries
            // were dropped at the store level
            var item_delta = (items - item_count)
            val size_delta: Int = size - size_count

            if ( item_delta!=0 || size_delta!=0 ) {
              assert(item_delta <= 0)
              assert(size_delta <= 0)
              info("Detected store dropped %d message(s) in seq range %d to %d using %d bytes", item_delta, seq, seq+count-1, size_delta)
              queue.enqueue_item_counter += item_delta
              queue.enqueue_size_counter += size_delta
            }

            var refill_preftch_list = prefetch_remove

            linkAfter(tmpList)
            val next = getNext

            // move the subs to the first entry that we just loaded.
            subscriptions.foreach(_.advance(next))
            next.addSubscriptions(subscriptions)

            unlink
            refill_preftch_list.foreach( _.refill_prefetch )
          }
        }
      }
      entry
    }

    override def tombstone = {
      throw new AssertionError("Flush group cannbot be tombstone.");
    }
  }

  /**
   * Entries in the Flushed state are not holding the referenced messages in memory anymore.
   * This state can transition to Loaded or Tombstone.
   *
   */
  class Flushed(val messageKey:Long, val size:Int) extends EntryState {

    var loading = false

    override def as_flushed = this

    override def is_flushed_or_flushing = true

    override def toString = { "flushed:{ loading: "+loading+", size:"+size+"}" }

    // Flushed entries can't be dispatched until
    // they get loaded.
    def dispatch():QueueEntry = {
      // This dispatch can happen when a subscription is holding onto lots of acquired entries
      // it can't prefetch anymore as it's waiting for ack on those messages to avoid
      // blowing it's memory limits.
      null
    }

    override def load() = {
      if( !loading ) {
//        trace("Start entry load of message seq: %s", seq)
        // start loading it back...
        loading = true
        queue.loading_size += size
        queue.host.store.loadMessage(messageKey) { delivery =>
          // pass off to a source so it can aggregate multiple
          // loads to reduce cross thread synchronization
          if( delivery.isDefined ) {
//            debug("Store found message seq: %d", seq)
            queue.store_load_source.merge((this, delivery.get))
          } else {

            info("Detected store dropped message at seq: %d", seq)

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
        queue.loading_size -= size

        val delivery = new Delivery()
        delivery.message = ProtocolFactory.get(messageRecord.protocol).decode(messageRecord.value)
        delivery.size = messageRecord.size
        delivery.storeKey = messageRecord.key

        queue.size += size
        state = new Loaded(delivery, true)
      } else {
//        debug("Ignoring store load of: ", messageKey)
      }
    }


    override def tombstone = {
      if( loading ) {
//        debug("Tombstoned, will ignore store load of seq: ", seq)
        loading = false
        queue.loading_size -= size
      }
      super.tombstone
    }
  }



}


class Subscription(queue:Queue) extends DeliveryProducer with DispatchLogging {
  override protected def log = Queue

  def dispatchQueue = queue.dispatchQueue

  var acquired = new LinkedNodeList[AcquiredQueueEntry]
  var session: DeliverySession = null
  var pos:QueueEntry = null

  var advanced_size = 0L

  // Vars used to detect slow consumers.
  var last_cursored_size = 0L
  var tail_parkings = 0
  var slow_intervals = 0

  def slow = slow_intervals > queue.tune_max_slow_intervals

  var prefetch_tail:QueueEntry = null
  var prefetched_size = 0
  var acquired_size = 0L

  override def toString = {
    def seq(entry:QueueEntry) = if(entry==null) null else entry.seq
    "{ acquired_size: "+acquired_size+", prefetch_size: "+prefetched_size+", pos: "+seq(pos)+" prefetch_tail: "+seq(prefetch_tail)+" }"
  }

  def browser = session.consumer.browser

  def open(consumer: DeliveryConsumer) = {
    pos = queue.head_entry;
    session = consumer.connect(this)
    session.refiller = pos
    queue.head_entry.addSubscriptions(this :: Nil)

    if( queue.serviceState.isStarted ) {
      // kick off the initial dispatch.
      refill_prefetch
      queue.dispatchQueue << queue.head_entry
    }
  }

  def close() = {
    pos.removeSubscriptions(this)

    invalidate_prefetch

    pos = null
    session.refiller = null
    session.close
    session = null

    // nack all the acquired entries.
    var next = acquired.getHead
    while( next !=null ) {
      val cur = next;
      next = next.getNext
      cur.nack // this unlinks the entry.
    }

    // show the queue entries... after we disconnect.
    queue.dispatchQueue.dispatchAfter(1, TimeUnit.SECONDS, ^{
      queue.display_active_entries
    })
  }

  /**
   * Advances the subscriptions position to the specified
   * queue entry.
   */
  def advance(value:QueueEntry):Unit = {
    assert(value!=null)

    // Remove the previous pos from the prefetch counters.
    if( prefetch_tail!=null && !pos.is_tombstone) {
      remove_from_prefetch(pos)
    }
    advanced_size += pos.size

    pos = value
    session.refiller = pos

    refill_prefetch()
    if( tail_parked ) {
      tail_parkings += 1
    }
  }

  /**
   * Rewinds to a previously seen location.. Happens when
   * a nack occurs from another consumer.
   */
  def rewind(value:QueueEntry):Unit = {
    assert(value!=null)
    invalidate_prefetch
    pos = value
    session.refiller = pos
    queue.dispatchQueue << value // queue up the entry to get dispatched..
  }

  def invalidate_prefetch: Unit = {
    if (prefetch_tail != null) {
      // release the prefetch counters...
      var cur = pos
      while (cur.seq <= prefetch_tail.seq) {
        if (!cur.is_tombstone) {
          prefetched_size -= cur.size
          cur.prefetched -= 1
        }
        cur = cur.nextOrTail
      }
      assert(prefetched_size == 0, "inconsistent prefetch size.")
    }
  }


  /**
   * Is the specified queue entry prefeteched by this subscription?
   */
  def is_prefetched(value:QueueEntry) = {
    prefetch_tail!=null && pos.seq <= value.seq && value.seq <= prefetch_tail.seq
  }


  def add_to_prefetch(entry:QueueEntry):Unit = {
    assert( !entry.is_tombstone, "tombstones should not be prefetched..")
    prefetched_size += entry.size
    entry.prefetched += 1
    entry.load
    prefetch_tail = entry
  }

  def remove_from_prefetch(entry:QueueEntry):Unit = {
    prefetched_size -= entry.size
    entry.prefetched -= 1

    if( entry == prefetch_tail ) {
      prefetch_tail = prefetch_tail.getPrevious;
      if( prefetch_tail==null || prefetch_tail.seq < pos.seq ) {
        prefetch_tail = null
        assert( prefetched_size == 0 , "inconsistent prefetch size.")
      }
    } else {
      assert( prefetched_size >= 0 , "inconsistent prefetch size.")
    }
  }

  def refill_prefetch() = {
    if( queue.tune_flush_to_store ) {
      def next_prefetch_pos = if(prefetch_tail==null) {
        if( !pos.is_tail ) {
          pos
        } else {
          null
        }
      } else  {
        prefetch_tail.getNext
      }

      // attempts to fill the prefetch...
      var next = next_prefetch_pos
      while( !prefetchFull && next!=null ) {
        if( !next.is_tombstone ) {
          add_to_prefetch(next)
        }
        next = next.getNext
      }
    }
  }

  def prefetchFull() = acquired_size + prefetched_size >= queue.tune_consumer_buffer

  def tail_parked = pos eq queue.tail_entry

  def matches(entry:Delivery) = session.consumer.matches(entry)
  def full = session.full
  def offer(delivery:Delivery) = session.offer(delivery)


  class AcquiredQueueEntry(val entry:QueueEntry) extends LinkedNode[AcquiredQueueEntry] {

    acquired.addLast(this)
    acquired_size += entry.size

    def ack(sb:StoreUOW) = {

      if (entry.messageKey != -1) {
        val storeBatch = if( sb == null ) {
          queue.host.store.createStoreUOW
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

      queue.dequeue_item_counter += 1
      queue.dequeue_size_counter += entry.size

      // removes this entry from the acquired list.
      unlink()

      // we may now be able to prefetch some messages..
      acquired_size -= entry.size
      entry.tombstone // entry size changes to 0
      refill_prefetch
    }

    def nack = {

      entry.as_loaded.acquired = false
      acquired_size -= entry.size

      // track for stats
      queue.nack_item_counter += 1
      queue.nack_size_counter += entry.size

      // rewind all the matching competing subs past the entry.. back to the entry
      queue.all_subscriptions.valuesIterator.foreach{ sub=>
        if( !sub.browser && entry.seq < sub.pos.seq && sub.matches(entry.as_loaded.delivery)) {
          sub.rewind(entry)
        }
      }
      unlink()
    }
  }


  def acquire(entry:QueueEntry) = new AcquiredQueueEntry(entry)

}
