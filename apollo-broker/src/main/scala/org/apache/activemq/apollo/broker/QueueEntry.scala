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

import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.broker.protocol.{MessageCodecFactory, ProtocolFactory, Protocol}
import org.apache.activemq.apollo.broker.store._
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.util.list._
import java.util.concurrent.atomic.AtomicReference
import java.lang.UnsupportedOperationException
import org.fusesource.hawtbuf._
import collection.mutable.ListBuffer
import Queue._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

object QueueEntry extends Sizer[QueueEntry] with Log {
  def size(value: QueueEntry): Int = value.size
}

class QueueEntry(val queue:Queue, val seq:Long) extends LinkedNode[QueueEntry] with Comparable[QueueEntry] {
  import QueueEntry._



  // Subscriptions waiting to dispatch this entry.
  var parked:List[Subscription] = Nil

  // subscriptions will set this to true if they are interested
  // in the entry.
  var prefetched = false

  // The current state of the entry: Head | Tail | Loaded | Swapped | SwappedRange
  var state:EntryState = new Tail

  def <(value:QueueEntry) = this.seq < value.seq
  def <=(value:QueueEntry) = this.seq <= value.seq

  def head():QueueEntry = {
    state = new Head
    this
  }

  def tail():QueueEntry = {
    state = new Tail
    this
  }

  def init(delivery:Delivery):QueueEntry = {
    if( delivery.message == null ) {
      // This must be a swapped out message which has been previously persisted in
      // another queue.  We need to enqueue it to this queue..
      queue.swap_out_size_counter += delivery.size
      queue.swap_out_item_counter += 1
      state = new Swapped(delivery.storeKey, delivery.storeLocator, delivery.size, delivery.expiration, 0, null, delivery.sender)
    } else {
      queue.producer_swapped_in += delivery
      state = new Loaded(delivery, false, queue.producer_swapped_in)
    }
    this
  }

  def init(qer:QueueEntryRecord):QueueEntry = {
    val sender = qer.sender.map(x=> SimpleAddress(x.utf8().toString))
    state = new Swapped(qer.message_key, qer.message_locator, qer.size, qer.expiration, qer.redeliveries, null, sender)
    this
  }

  def init(range:QueueEntryRange):QueueEntry = {
    state = new SwappedRange(range.last_entry_seq, range.count, range.size, range.expiration)
    this
  }

  def hasSubs = !parked.isEmpty

  /**
   * Dispatches this entry to the consumers and continues dispatching subsequent
   * entries as long as the dispatch results in advancing in their dispatch position.
   */
  final val task = new Task {
    def run() {
      queue.assert_executing
      var cur = QueueEntry.this;
      while( cur!=null && cur.isLinked ) {
        val next = cur.getNext
        cur = if( cur.dispatch ) {
          next
        } else {
          null
        }
      }
    }
  }

  def ::=(sub:Subscription) = {
    parked = parked ::: sub :: Nil
  }

  def :::=(l:List[Subscription]) = {
    parked = parked ::: l
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
    qer.queue_key = queue.store_id
    qer.entry_seq = seq
    qer.message_key = state.message_key
    qer.message_locator = state.message_locator
    qer.message_locator = state.message_locator
    qer.size = state.size
    qer.expiration = expiration
    qer.sender = state.sender.map(x=> new UTF8Buffer(x.toString))
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

  def as_swapped = state.as_swapped
  def as_swapped_range = state.as_swapped_range
  def as_loaded = state.as_loaded

  def label = state.label

  def is_tail = as_tail!=null
  def is_head = as_head!=null
  def is_loaded = as_loaded!=null
  def is_swapped = as_swapped!=null
  def is_swapped_range = as_swapped_range!=null
  def is_swapped_or_swapped_range = is_swapped || is_swapped_range
  def is_loading = state match {
    case state:SwappedRange => state.loading
    case state:Swapped => state.loading
    case _ => false
  }

  // These should not change the current state.
  def count = state.count
  def size = state.size
  def expiration = state.expiration
  def redelivery_count = state.redelivery_count
  def redelivered = state.redelivered
  def messageKey = state.message_key
  def is_swapped_or_swapping_out = state.is_swapped_or_swapping_out
  def is_acquired = state.is_acquired
  def acquiring_subscription = state.acquiring_subscription
  def dispatch() = state.dispatch
  def memory_space = state.memory_space

  var expiring = false

  // These methods may cause a change in the current state.
  def swap(asap:Boolean) = state.swap_out(asap)
  def load(space:MemorySpace) = state.swap_in(space)
  def remove = state.remove

  var queued = true

  def dequeue(uow: StoreUOW) = {
    if ( queued ) {
      if (messageKey != -1) {
        val actual_uow = queue.create_uow(uow)
        actual_uow.dequeue(toQueueEntryRecord)
        actual_uow.release
      }
      queue.dequeue_item_counter += 1
      queue.dequeue_size_counter += size
      queue.dequeue_ts = queue.now
      queued = false
    }
  }


  def swapped_range = state.swap_range

  def can_combine_with_prev = {
    var prev = getPrevious
    if ( prev == null ) {
      false
    } else {
      val prev_range = prev.as_swapped_range
      if ( prev_range == null ) {
        false
      } else {
        (!prev_range.loading) &&
        (!is_loading) &&
        ( (is_swapped && !is_acquired) || is_swapped_range ) &&
        (prev.count + count  < queue.tune_swap_range_size)
      }
    }
  }

  trait EntryState {

    final def entry:QueueEntry = QueueEntry.this

    def as_tail:Tail = null
    def as_loaded:Loaded = null
    def as_swapped:Swapped = null
    def as_swapped_range:SwappedRange = null
    def as_head:Head = null

    /**
     * Gets the size of this entry in bytes.  The head and tail entries always return 0.
     */
    def size = 0

    /**
     * When the entry expires or 0 if it does not expire.
     */
    def expiration = 0L

    /**
     * When the entry expires or 0 if it does not expire.
     */
    def redelivery_count:Short = throw new UnsupportedOperationException()

    /**
     * Called to increment the redelivery counter
     */
    def redelivered:Unit = {}

    /**
     * Gets number of messages that this entry represents
     */
    def count = 0

    /**
     * Retuns a string label used to describe this state.
     */
    def label:String

    /**
     * Gets the message key for the entry.
     * @returns -1 if it is not known.
     */
    def message_key = -1L

    def message_locator: AtomicReference[Object] = null

    def sender = List[DestinationAddress]()

    /**
     * Attempts to dispatch the current entry to the subscriptions position at the entry.
     * @return true if at least one subscription advanced to the next entry as a result of dispatching.
     */
    def dispatch() = false

    /**
     * Is the entry acquired by a subscription.
     */
    def is_acquired = acquiring_subscription!=null
    def acquiring_subscription:Acquirer = null

    /**
     * @returns true if the entry is either swapped or swapping.
     */
    def is_swapped_or_swapping_out = false

    /**
     * Triggers the entry to get swapped in if it's not already swapped in.
     */
    def swap_in(space:MemorySpace) = {}

    def memory_space:MemorySpace = null

    /**
     * Triggers the entry to get swapped out if it's not already swapped.
     */
    def swap_out(asap:Boolean) = {}

    def swap_range:Unit = throw new AssertionError("should only be called on swapped entries");

    /**
     * Removes the entry from the queue's linked list of entries.  This gets called
     * as a result of an acquired ack.
     */
    def remove:Unit = {
      // advance subscriptions that were on this entry..
      advance(parked)
      parked = Nil

      // take the entry of the entries list..
      unlink
      //TODO: perhaps refill subscriptions.
    }

    /**
     * Advances the specified subscriptions to the next entry in
     * the linked list
     */
    def advance(advancing: Seq[Subscription]): Unit = {
      val nextPos = nextOrTail
      nextPos :::= advancing.toList
      advancing.foreach(_.advance(nextPos))
      queue.trigger_swap
    }

  }

  /**
   *  Used for the head entry.  This is the starting point for all new subscriptions.
   */
  class Head extends EntryState {

    def label = "head"
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
    override def swap_in(space:MemorySpace) = throw new AssertionError("Head entry cannot be loaded")
    override def swap_out(asap:Boolean) = throw new AssertionError("Head entry cannot be swapped")
  }

  /**
   * This state is used on the last entry of the queue.  It still has not been initialized
   * with a message, but it may be holding subscriptions.  This state transitions to Loaded
   * once a message is received.
   */
  class Tail extends EntryState {

    def label = "tail"
    override  def toString = "tail"
    override def as_tail:Tail = this

    override def remove = throw new AssertionError("Tail entry cannot be removed")
    override def swap_in(space:MemorySpace) = throw new AssertionError("Tail entry cannot be loaded")
    override def swap_out(asap:Boolean) = throw new AssertionError("Tail entry cannot be swapped")

  }

  /**
   * The entry is in this state while a message is loaded in memory.  A message must be in this state
   * before it can be dispatched to a subscription.
   */
  class Loaded(val delivery: Delivery, var enqueue_stored:Boolean, var space:MemorySpace) extends EntryState {

    assert( delivery!=null, "delivery cannot be null")

    var acquirer:Acquirer = _
    override def acquiring_subscription = acquirer

    override def memory_space = space

    var swapping_out = false
    var storing_enqueue = false

    queue.loaded_items += 1
    queue.loaded_size += size

    def label = {
      var rc = "loaded"
      if( is_acquired ) {
        rc += "|acquired"
      }
      if( swapping_out ) {
        rc += "|swapping out"
      }
      rc
    }

    override def toString = { "loaded:{ enqueue_stored: "+enqueue_stored+", swapping_out: "+swapping_out+", acquired: "+acquirer+", size:"+size+"}" }

    override def count = 1
    override def size = delivery.size
    override def expiration = delivery.expiration
    override def message_key = delivery.storeKey
    override def message_locator = delivery.storeLocator
    override def redelivery_count = delivery.redeliveries
    override def sender = delivery.sender

    override def redelivered = delivery.redeliveries = ((delivery.redeliveries+1).min(Short.MaxValue)).toShort

    var remove_pending = false

    override def is_swapped_or_swapping_out = {
      swapping_out
    }

    override  def as_loaded = this

    def store_enqueue(uow:StoreUOW) = {
      assert(queue.service_state.is_starting_or_started)
      if(!enqueue_stored && !storing_enqueue) {
        storing_enqueue = true
        assert( uow!=null )
        uow.enqueue(toQueueEntryRecord)
        queue.swapping_out_size+=size
        uow.on_flush { canceled =>
          queue.defer {
            this.swapped_out(!canceled)
            queue.swapping_out_size-=size
            if( queue.swapping_out_size==0 ) {
              queue.on_queue_flushed
            }
          }
        }
      }
    }

    override def swap_out(asap:Boolean) = {
      if( queue.tune_swap && !swapping_out ) {
        if( enqueue_stored ) {
          switch_to_swapped
        } else {
          swapping_out=true
          val uow = queue.create_uow

          // Are we swapping out a non-persistent message?
          val flush = if( delivery.storeKey == -1 ) {
            delivery.storeLocator = new AtomicReference[Object]()
            delivery.storeKey = uow.store(delivery.createMessageRecord )
            false
          } else {
            true
          }
          store_enqueue(uow)
          if( asap ) {
            if ( flush ) {
              queue.virtual_host.store.flush_message(delivery.storeKey) {}
            } else {
              uow.complete_asap
            }
          }
          uow.release
        }
      }
    }

    var on_swap_out = List[()=>Unit]()

    def fire_swap_out_watchers = if(!on_swap_out.isEmpty) {
      val t = on_swap_out
      on_swap_out = Nil
      for ( task <- t ) {
        task()
      }
    }

    def swapped_out(not_canceled:Boolean) = {
      assert( state == this )
      assert( delivery.storeKey != -1 )
      storing_enqueue = false

      if( not_canceled ) {
        enqueue_stored = true
      } else {
        delivery.storeKey = -1
      }

      if( swapping_out ) {
        swapping_out = false
        if( not_canceled ) {
          queue.swap_out_size_counter += size
          queue.swap_out_item_counter += 1
          switch_to_swapped
        }
        fire_swap_out_watchers
      }
      if( remove_pending ) {
        remove_pending = false
        delivery.message.release
        space -= delivery
        super.remove
      }
    }

    def switch_to_swapped = {
      space -= delivery
      state = new Swapped(delivery.storeKey, delivery.storeLocator, size, expiration, redelivery_count, acquirer, sender)
      if( remove_pending ) {
        remove_pending = false
        state.remove
      } else {
        if( can_combine_with_prev ) {
          getPrevious.as_swapped_range.combineNext
        }
        queue.loaded_items -= 1
        queue.loaded_size -= size
      }
    }

    override def swap_in(space:MemorySpace) = {
      if(space ne this.space) {
        this.space -= delivery
        this.space = space
        this.space += delivery
      }
      swapping_out = false
      fire_swap_out_watchers
    }

    override def remove = {
      if( !remove_pending ) {
        queue.loaded_items -= 1
        queue.loaded_size -= size
      }
      if( storing_enqueue | remove_pending ) {
        remove_pending = true
      } else {
        delivery.message.release
        space -= delivery
        super.remove
      }
    }

    override def dispatch():Boolean = {

      queue.assert_executing

      if( !is_acquired && expiration != 0 && expiration <= queue.now ) {
        val uow = queue.create_uow
        entry.dequeue(uow)
        queue.expired(uow, entry) {
          if( isLinked ) {
            remove
          }
        }
        uow.release
        return true
      }

      // Nothing to dispatch if we don't have subs..
      if( parked.isEmpty ) {
        return false
      }

      var heldBack = ListBuffer[Subscription]()
      var advancing = ListBuffer[Subscription]()

      // avoid doing the copy if its' not needed.
      var _browser_copy:Delivery = null
      def browser_copy = {
        if( _browser_copy==null ) {
          _browser_copy = delivery.copy
          // TODO: perhaps only avoid adding the address in the durable sub case..
          if( _browser_copy.sender == Nil ) {
            _browser_copy.sender ::= queue.address
          }
        }
        _browser_copy
      }

      var acquiringSub: Subscription = null

      // Find the the first exclusive target of the message
      var exclusive_target = queue.exclusive_subscriptions.find( _.matches(delivery) )

      // Should we looks for the message group bucket?
      if ( exclusive_target.isEmpty && delivery.message.message_group != null ) {
        var iterator = queue.message_group_buckets.iterator(delivery.message.message_group)
        while (exclusive_target==None && iterator.hasNext) {
          val bucket = iterator.next();
          if( bucket.sub.matches(delivery) ) {
            exclusive_target = Some(bucket.sub)
          }
        }
      }

      parked.foreach{ sub=>

        if( sub.browser ) {
          if (!sub.matches(delivery)) {
            // advance: not interested.
            advancing += sub
          } else {
            if (sub.offer(browser_copy)) {
              // advance: accepted...
              advancing += sub
            } else {
              // hold back: flow controlled
              heldBack += sub
            }
          }

        } else {
          if( is_acquired ) {
            // advance: another sub already acquired this entry..
            advancing += sub
          } else {

            // Is the current sub not the exclusive target?
            if( (exclusive_target.isDefined && (exclusive_target.get != sub))
                || !sub.matches(delivery)
                || (exclusive_target.isEmpty && delivery.message.message_group!=null) ) {
              // advance: not interested.
              advancing += sub
            } else {

              // Is the sub flow controlled?
              if( sub.full ) {
                // hold back: flow controlled
                heldBack += sub
              } else {
                // advance: accepted...
                if( queue.tune_round_robin ) {
                  acquiringSub = sub
                } else {
                  advancing += sub
                }
                acquirer = sub

                val acquiredQueueEntry = sub.acquire(entry)
                val acquiredDelivery = delivery.copy
                if( acquiredDelivery.sender == Nil) {
                  acquiredDelivery.sender ::= queue.address
                }

                acquiredDelivery.ack = (consumed, uow)=> {
                  if( uow!=null ) {
                    uow.retain
                  }
                  queue.process_ack(acquiredQueueEntry, consumed, uow)
                }

                val accepted = sub.offer(acquiredDelivery)
                assert(accepted, "sub should have accepted, it had reported not full earlier.")
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

        // We can drop after dispatch in some cases.
        if( queue.is_topic_queue  && parked.isEmpty && getPrevious.is_head ) {
          dequeue(null)
          remove
        }

        queue.trigger_swap
        return true
      }
    }
  }

  /**
   * Loaded entries are moved into the Swapped state reduce memory usage.  Once a Loaded
   * entry is persisted, it can move into this state.  This state only holds onto the
   * the massage key so that it can reload the message from the store quickly when needed.
   */
  class Swapped(override val message_key:Long, override val message_locator:AtomicReference[Object], override val size:Int, override val expiration:Long, var _redeliveries:Short, var acquirer:Acquirer, override  val sender:List[DestinationAddress]) extends EntryState {

    assert( message_key!= -1 )

    queue.individual_swapped_items += 1

    var space:MemorySpace = _

    override def redelivery_count = _redeliveries
    override def redelivered = _redeliveries = ((_redeliveries+1).min(Short.MaxValue)).toShort

    override def acquiring_subscription = acquirer
    override def count = 1

    override def as_swapped = this

    override def is_swapped_or_swapping_out = true

    override def memory_space = space

    def label = {
      var rc = "swapped"
      if( is_acquired ) {
        rc += "|acquired"
      }
      if( space!=null ) {
        rc += "|swapping in"
      }
      rc
    }

    override def toString = { "swapped:{ swapping_in: "+space+", acquired:"+acquirer+", size:"+size+"}" }

    def loading = this.space!=null

    override def swap_in(mem_space:MemorySpace) = {
      if( this.space==null ) {
//        trace("Start entry load of message seq: %s", seq)
        // start swapping in...
        space = mem_space
        queue.swapping_in_size += size
        queue.virtual_host.store.load_message(message_key, message_locator) { delivery =>
          // pass off to a source so it can aggregate multiple
          // loads to reduce cross thread synchronization
          if( delivery.isDefined ) {
            queue.defer {
              swapped_in(delivery.get)
              if( entry.hasSubs ) {
                entry.task.run
              }
            }
          } else {

            warn("Queue '%s' detected store dropped message at seq: %d", queue.id, seq)

            // Looks like someone else removed the message from the store.. lets just
            // tombstone this entry now.
            queue.dispatch_queue {
              remove
            }
          }
        }
      }
    }

    def to_delivery = {
      val delivery = new Delivery()
      delivery.seq = seq
      delivery.size = size
      delivery.persistent = true
      delivery.expiration = expiration
      delivery.storeKey = message_key
      delivery.storeLocator = message_locator
      delivery.redeliveries = redelivery_count
      delivery.sender = sender
      delivery
    }

    var swapped_in_watchers = List[()=>Unit]()
    def fire_swapped_in_watchers = {
      val watchers = swapped_in_watchers
      swapped_in_watchers = Nil
      watchers.foreach(_())
    }

    def swapped_in(messageRecord:MessageRecord) = {
      if( space!=null ) {
//        debug("Loaded message seq: ", seq )
        queue.swapping_in_size -= size

        val delivery = to_delivery
        delivery.message = MessageCodecFactory(messageRecord.codec.toString).get.decode(messageRecord)

        space += delivery

        queue.swap_in_size_counter += size
        queue.swap_in_item_counter += 1

        queue.individual_swapped_items -= 1
        state = new Loaded(delivery, true, space)
        space = null
      }
      fire_swapped_in_watchers
    }


    override def remove = {
      if( space!=null ) {
        space = null
        queue.swapping_in_size -= size
      }
      queue.individual_swapped_items -= 1
      super.remove
    }

    override def swap_range = {
      // You can't swap range an acquired entry.
      assert(!is_acquired)
      if( space!=null ) {
        space = null
        queue.swapping_in_size -= size
      }
      queue.individual_swapped_items -= 1
      state = new SwappedRange(seq, 1, size, expiration)
    }

    override def dispatch():Boolean = {
      queue.assert_executing

      if( !is_acquired && expiration != 0 && expiration <= queue.now ) {
        val uow = queue.create_uow
        entry.dequeue(uow)
        queue.expired(uow, entry) {
          if( isLinked ) {
            remove
          }
        }
        uow.release
        return true
      }

      // Nothing to dispatch if we don't have subs..
      if( parked.isEmpty ) {
        return false
      }

      var heldBack = ListBuffer[Subscription]()
      var advancing = ListBuffer[Subscription]()

      parked.foreach{ sub=>
        if( sub.browser ) {
          heldBack += sub
        } else {
          if( is_acquired ) {
            // advance: another sub already acquired this entry.. we don't need to load.. yay!
            advancing += sub
          } else {
            heldBack += sub
          }
        }
      }

      if ( advancing.isEmpty ) {
        if (space==null && !parked.isEmpty) {
          // If we are not swapping in try to get a sub to prefetch us.
          parked.foreach(_.refill_prefetch)
        }
        return false
      } else {

        // The held back subs stay on this entry..
        parked = heldBack.toList

        if (space==null && !parked.isEmpty) {
          // If we are not swapping in try to get a sub to prefetch us.
          parked.foreach(_.refill_prefetch)
        }

        // the advancing subs move on to the next entry...
        advance(advancing)
        return true
      }
    }
  }

  /**
   * A SwappedRange state is assigned entry is used to represent a rage of swapped entries.
   *
   * Even entries that are Swapped can us a significant amount of memory if the queue is holding
   * thousands of them.  Multiple entries in the swapped state can be combined into a single entry in
   * the SwappedRange state thereby conserving even more memory.  A SwappedRange entry only tracks
   * the first, and last sequnce ids of the range.  When the entry needs to be loaded from the range
   * it replaces the swapped range entry with all the swapped entries by querying the store of all the
   * message keys for the entries in the range.
   */
  class SwappedRange(
    /** the last seq id in the range */
    var last:Long,
    /** the number of items in the range */
    var _count:Int,
    /** size in bytes of the range */
    var _size:Int,
    var _expiration:Long) extends EntryState {


    override def count = _count
    override def size = _size
    override def expiration = _expiration

    var loading = false

    override def as_swapped_range = this

    override def is_swapped_or_swapping_out = true


    def label = {
      var rc = "swapped_range"
      if( loading ) {
        rc = "swapped_range|swapping in"
      }
      rc
    }
    override def toString = { "swapped_range:{ swapping_in: "+loading+", count: "+count+", size: "+size+"}" }

    var swapped_in_watchers = List[()=>Unit]()
    def fire_swapped_in_watchers = {
      val watchers = swapped_in_watchers
      swapped_in_watchers = Nil
      watchers.foreach(_())
    }

    override def swap_in(space:MemorySpace):Unit = {
      if( !loading ) {
        loading = true

        def complete_load(attempt_counter:Int, records:Seq[QueueEntryRecord]):Unit = {
          assert(isLinked)

          var item_count=0
          var size_count=0

          val tmpList = new LinkedNodeList[QueueEntry]()
          records.foreach { record =>
            val entry = new QueueEntry(queue, record.entry_seq).init(record)
            tmpList.addLast(entry)
            item_count += 1
            size_count += record.size
          }

          // we may need to adjust the enqueue count if entries
          // were dropped at the store level
          var item_delta = (count - item_count)
          val size_delta: Int = size - size_count

          if ( item_delta!=0 || size_delta!=0 ) {
            if ( attempt_counter < 10) {
              warn("Retrying "+attempt_counter+" load do to Queue '%s' detected store change in range [%d:%d]. %d message(s) and %d bytes", queue.id, seq, last, item_delta, size_delta)
              attempt_load(attempt_counter+1)
              return
            } else {
              warn("Queue '%s' detected store change in range [%d:%d]. %d message(s) and %d bytes", queue.id, seq, last, item_delta, size_delta)
              queue.enqueue_item_counter += item_delta
              queue.enqueue_size_counter += size_delta
            }
          } else if( attempt_counter > 1 ) {
            warn("Recoved!!!! @ "+attempt_counter)
          }

          loading  = false
          linkAfter(tmpList)
          val next = getNext

          // move the subs to the first entry that we just loaded.
          parked.foreach(_.advance(next))
          next :::= parked
          queue.trigger_swap
          unlink
          fire_swapped_in_watchers
        }

        def attempt_load(attempt_counter:Int):Unit = {
          queue.virtual_host.store.list_queue_entries(queue.store_id, seq, last) { records =>
            queue.dispatch_queue {
              complete_load(attempt_counter, records)
            }
          }
        }
        attempt_load(1)
      }
    }

    /**
     * Combines this queue entry with the next queue entry.
     */
    def combineNext():Unit = {
      val value = getNext
      assert(value!=null)
      assert(value.is_swapped || value.is_swapped_range)
      assert(!value.is_acquired)
      assert(!value.is_loading)
      if( value.is_swapped ) {
        assert(last < value.seq )
        last = value.seq
        _count += 1
      } else if( value.is_swapped_range ) {
        assert(last < value.seq )
        last = value.as_swapped_range.last
        _count += value.as_swapped_range.count
      }
      if(_expiration == 0){
        _expiration = value.expiration
      } else {
        if( value.expiration != 0 ) {
          _expiration = value.expiration.min(_expiration)
        }
      }
      _size += value.size
      value.remove
    }

  }

}
