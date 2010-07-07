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
import org.apache.activemq.apollo.store.{QueueEntryRange, QueueEntryRecord, MessageRecord}
import collection.mutable.ListBuffer

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
  val head_entry = new QueueEntry(this, 0L).head
  var tail_entry = new QueueEntry(this, next_message_seq).tail
  entries.addFirst(head_entry)

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
      host.store.listQueueEntryRanges(queueKey, tune_entry_group_size) { ranges=>
        dispatchQueue {
          if( !ranges.isEmpty ) {

            ranges.foreach { range =>
              val entry = new QueueEntry(Queue.this, range.firstQueueSeq).init(range)
              entries.addLast(entry)

              message_seq_counter = range.lastQueueSeq + 1
              enqueue_item_counter += range.count
              enqueue_size_counter += range.size
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
        total_items += cur.as_flushed_group.count
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
//          display_stats
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

    var entry = head_entry.getNext
    while( entry!=null ) {
      val loaded = entry.as_loaded

      // Keep around prefetched and loaded entries.
      if( entry.is_prefetched || (loaded!=null && loaded.acquired)) {
        entry.load
      } else {
        // flush the the others out of memory.
        entry.flush
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
      debug(dispatchQueue.getLabel+" co-locating with: "+value.getLabel);
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
  var parked:List[Subscription] = Nil

  // The number of subscriptions which have requested this entry to be prefeteched (held in memory) so that it's
  // ready for them to get dispatched.
  var prefetched = 0

  // The current state of the entry: Tail | Loaded | Flushed | Tombstone
  var state:EntryState = new Tail

  def is_prefetched = prefetched>0

  def head():QueueEntry = {
    state = new Head
    this
  }

  def tail():QueueEntry = {
    state = new Tail
    this
  }

  def init(delivery:Delivery):QueueEntry = {
    state = new Loaded(delivery, false)
    queue.size += size
    this
  }

  def init(qer:QueueEntryRecord):QueueEntry = {
    state = new Flushed(qer.messageKey, qer.size)
    this
  }

  def init(range:QueueEntryRange):QueueEntry = {
    state = new FlushedRange(range.lastQueueSeq, range.count, range.size)
    this
  }

  def hasSubs = !parked.isEmpty

  /**
   * Dispatches this entry to the consumers and continues dispatching subsequent
   * entries as long as the dispatch results in advancing in their dispatch position.
   */
  def run() = {
    var next = this;
    while( next!=null && next.dispatch) {
      next = next.getNext
    }
  }

  def ::=(sub:Subscription) = {
    parked ::= sub
  }

  def :::=(l:List[Subscription]) = {
    parked :::= l
  }


  def -=(s:Subscription) = {
    parked = parked.filterNot(_ == s)
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
    "{seq: "+seq+", prefetched: "+prefetched+", value: "+state+", subscriptions: "+parked+"}"
  }

  /////////////////////////////////////////////////////
  //
  // State delegates..
  //
  /////////////////////////////////////////////////////

  // What state is it in?
  def as_head = state.as_head
  def as_tail = state.as_tail

  def as_flushed = state.as_flushed
  def as_flushed_group = state.as_flushed_group
  def as_loaded = state.as_loaded

  def is_tail = this == queue.tail_entry
  def is_head = this == queue.head_entry

  def is_loaded = as_loaded!=null
  def is_flushed = as_flushed!=null
  def is_flushed_group = as_flushed_group!=null

  // These should not change the current state.
  def size = state.size
  def messageKey = state.messageKey
  def is_flushed_or_flushing = state.is_flushed_or_flushing
  def dispatch() = state.dispatch

  // These methods may cause a change in the current state.
  def flush = state.flush
  def load = state.load
  def remove = state.remove

  trait EntryState {

    final def entry:QueueEntry = QueueEntry.this

    def as_tail:Tail = null
    def as_loaded:Loaded = null
    def as_flushed:Flushed = null
    def as_flushed_group:FlushedRange = null
    def as_head:Head = null

    /**
     * Gets the size of this entry in bytes.  The head and tail entries allways return 0.
     */
    def size = 0

    /**
     * Gets the message key for the entry.
     * @returns -1 if it is not known.
     */
    def messageKey = -1L

    /**
     * Attempts to dispatch the current entry to the subscriptions position at the entry.
     * @returns true if at least one subscription advanced to the next entry as a result of dispatching.
     */
    def dispatch() = false

    /**
     * @returns true if the entry is either flushed or flushing.
     */
    def is_flushed_or_flushing = false

    /**
     * Triggers the entry to get loaded if it's not already loaded.
     */
    def load = {}

    /**
     * Triggers the entry to get flushed if it's not already flushed.
     */
    def flush = {}

    /**
     * Takes the current entry out of the prefetch of all subscriptions
     * which have prefetched the entry.  Returns the list of subscriptions which
     * had prefetched it.
     */
    def prefetch_remove = {
      var rc = List[Subscription]()
      if( queue.tune_flush_to_store ) {
        // Update the prefetch counter to reflect that this entry is no longer being prefetched.
        var cur = entry
        while( cur!=null && is_prefetched ) {
          if( cur.hasSubs ) {
            (cur.parked).foreach { sub =>
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

    /**
     * Removes the entry from the queue's linked list of entries.  This gets called
     * as a result of an aquired ack.
     */
    def remove = {
      // take us out of subscription prefetches..
      var refill_preftch_list = prefetch_remove
      // advance subscriptions that were on this entry..
      parked.foreach(_.advance(next))
      nextOrTail :::= parked
      parked = Nil
      // take the entry of the entries list..
      unlink
      // refill the subscription prefetches..
      refill_preftch_list.foreach( _.refill_prefetch )
    }

    /**
     * Advances the specified subscriptions to the next entry in
     * the linked list
     */
    def advance(advancing: Seq[Subscription]): Unit = {
      val nextPos = nextOrTail
      nextPos :::= advancing.toList
      advancing.foreach(_.advance(nextPos))
    }

  }

  /**
   *  Used for the head entry.  This is the starting point for all new subscriptions.
   */
  class Head extends EntryState {

    override  def toString = "head"
    override def as_head = this

    /**
     * New subs get parked here at the Head.  There is nothing to actually dispatch
     * in this entry.. just advance the parked subs onto the next entry.
     */
    override def dispatch() = {
      if( parked != Nil ) {

        advance(parked)
        parked = Nil
        true

      } else {
        false
      }
    }

    override def remove = throw new AssertionError("Head entry cannot be removed")
    override def load = throw new AssertionError("Head entry cannot be loaded")
    override def flush = throw new AssertionError("Head entry cannot be flushed")
  }

  /**
   * This state is used on the last entry of the queue.  It still has not been initialized
   * with a message, but it may be holding subscriptions.  This state transitions to Loaded
   * once a message is received.
   */
  class Tail extends EntryState {

    override  def toString = "tail"
    override def as_tail:Tail = this

    override def remove = throw new AssertionError("Tail entry cannot be removed")
    override def load = throw new AssertionError("Tail entry cannot be loaded")
    override def flush = throw new AssertionError("Tail entry cannot be flushed")

  }

  /**
   * The entry is in this state while a message is loaded in memory.  A message must be in this state
   * before it can be dispatched to a subscription.
   */
  class Loaded(val delivery: Delivery, var stored:Boolean) extends EntryState {

    assert( delivery!=null, "delivery cannot be null")

    var acquired = false
    var flushing = false

    override def toString = { "loaded:{ stored: "+stored+", flushing: "+flushing+", acquired: "+acquired+", size:"+size+"}" }

    override def size = delivery.size
    override def messageKey = delivery.storeKey

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
        if( stored ) {
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
    }

    def flushed() = {
      stored = true
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
    }

    override def remove = {
      if( flushing ) {
        flushing = false
        queue.flushing_size-=size
      }
      queue.size -= size
      super.remove
    }

    override def dispatch():Boolean = {

      // Nothing to dispatch if we don't have subs..
      if( parked.isEmpty ) {
        return false
      }

      var heldBack = ListBuffer[Subscription]()
      var advancing = ListBuffer[Subscription]()

      var acquiringSub: Subscription = null
      parked.foreach{ sub=>

        if( sub.browser ) {
          if (!sub.matches(delivery)) {
            // advance: not interested.
            advancing += sub
          } else {
            if (sub.offer(delivery)) {
              // advance: accepted...
              advancing == sub
            } else {
              // hold back: flow controlled
              heldBack += sub
            }
          }

        } else {
          if( acquired ) {
            // advance: another sub already acquired this entry..
            advancing += sub
          } else {
            if (!sub.matches(delivery)) {
              // advance: not interested.
              advancing += sub
            } else {

              // Is the sub flow controlled?
              if( sub.full || (sub.prefetchFull && !sub.is_prefetched(entry) ) ) {
                // hold back: flow controlled
                heldBack += sub
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
        advancing += acquiringSub
      }

      if ( advancing.isEmpty ) {
        return false
      } else {

        // The held back subs stay on this entry..
        parked = heldBack.toList

        // the advancing subs move on to the next entry...
        advance(advancing)

        // flush this entry out if it's not going to be needed soon.
        def haveQuickConsumer = queue.fast_subscriptions.find( sub=> sub.pos.seq <= seq ).isDefined
        if( !hasSubs && !is_prefetched && !acquired && !haveQuickConsumer ) {
          // then flush out to make space...
          flush
        }
        return true
      }
    }
  }

  /**
   * Loaded entries are moved into the Flushed state reduce memory usage.  Once a Loaded
   * entry is persisted, it can move into this state.  This state only holds onto the
   * the massage key so that it can reload the message from the store quickly when needed.
   */
  class Flushed(override val messageKey:Long, override val size:Int) extends EntryState {

    var loading = false

    override def as_flushed = this

    override def is_flushed_or_flushing = true

    override def toString = { "flushed:{ loading: "+loading+", size:"+size+"}" }

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
            queue.dispatchQueue {
              queue.store_load_source.merge((this, delivery.get))
            }
          } else {

            info("Detected store dropped message at seq: %d", seq)

            // Looks like someone else removed the message from the store.. lets just
            // tombstone this entry now.
            queue.dispatchQueue {
              remove
            }
          }
        }
      }
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


    override def remove = {
      if( loading ) {
        loading = false
        queue.loading_size -= size
      }
      super.remove
    }
  }

  /**
   * A FlushedRange stat is assigned entry is used to represent a rage of flushed entries.
   *
   * Even when entries that are Flushed can us a significant amount of memory if the queue is holding
   * thousands of them.  Multiple entries in the Flushed state can be combined into a single entry in
   * the FlushedRange state thereby conserving even more memory.  A FlushedRange entry only tracks
   * the first, and last sequnce ids of the range.  When the entry needs to be loaded from the range
   * it replaces the FlushedRange entry with all the Flushed entries by querying the store of all the
   * message keys for the entries in the range.
   */
  class FlushedRange(
    /** the last seq id in the range */
    var last:Long,
    /** the number of items in the range */
    var count:Int,
    /** size in bytes of the range */
    override val size:Int) extends EntryState {

    var loading = false

    override def as_flushed_group = this

    override def is_flushed_or_flushing = true

    override def toString = { "flushed_group:{ loading: "+loading+", count: "+count+", size: "+size+"}" }

    override def load() = {
      if( !loading ) {
        loading = true
        queue.host.store.listQueueEntries(queue.queueKey, seq, last) { records =>
          queue.dispatchQueue {

            var item_count=0
            var size_count=0

            val tmpList = new LinkedNodeList[QueueEntry]()
            records.foreach { record =>
              val entry = new QueueEntry(queue, record.queueSeq).init(record)
              tmpList.addLast(entry)
              item_count += 1
              size_count += record.size
            }

            // we may need to adjust the enqueue count if entries
            // were dropped at the store level
            var item_delta = (count - item_count)
            val size_delta: Int = size - size_count

            if ( item_delta!=0 || size_delta!=0 ) {
              assert(item_delta <= 0)
              assert(size_delta <= 0)
              info("Detected store dropped %d message(s) in seq range %d to %d using %d bytes", item_delta, seq, last, size_delta)
              queue.enqueue_item_counter += item_delta
              queue.enqueue_size_counter += size_delta
            }

            var refill_preftch_list = prefetch_remove

            linkAfter(tmpList)
            val next = getNext

            // move the subs to the first entry that we just loaded.
            parked.foreach(_.advance(next))
            next :::= parked

            unlink
            refill_preftch_list.foreach( _.refill_prefetch )
          }
        }
      }
    }

    override def remove = {
      throw new AssertionError("Flushed range cannbot be removed.");
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
    "{ acquired_size: "+acquired_size+", prefetch_size: "+prefetched_size+", pos: "+seq(pos)+" prefetch_tail: "+seq(prefetch_tail)+", tail_parkings: "+tail_parkings+"}"
  }

  def browser = session.consumer.browser

  def open(consumer: DeliveryConsumer) = {
    pos = queue.head_entry;
    session = consumer.connect(this)
    session.refiller = pos
    queue.head_entry ::= this

    if( queue.serviceState.isStarted ) {
      // kick off the initial dispatch.
      refill_prefetch
      queue.dispatchQueue << queue.head_entry
    }
  }

  def close() = {
    pos -= this

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
  }

  /**
   * Advances the subscriptions position to the specified
   * queue entry.
   */
  def advance(value:QueueEntry):Unit = {
    assert(value!=null)
    assert(pos!=null) 

    // Remove the previous pos from the prefetch counters.
    if( prefetch_tail!=null && !pos.is_head) {
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
        if (!cur.is_head) {
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
    prefetch_tail!=null && value!=null && pos.seq <= value.seq && value.seq <= prefetch_tail.seq
  }


  def add_to_prefetch(entry:QueueEntry):Unit = {
    assert( !entry.is_head, "tombstones should not be prefetched..")
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
        if( !next.is_head ) {
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

      val next = entry.nextOrTail
      entry.remove // entry size changes to 0

      refill_prefetch
      next.run
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
