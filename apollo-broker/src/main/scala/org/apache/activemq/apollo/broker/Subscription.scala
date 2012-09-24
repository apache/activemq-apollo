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
import org.apache.activemq.apollo.broker.store._
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.util.list._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Subscription extends Log

/**
 * Interfaces a DispatchConsumer with a Queue.  Tracks current position of the consumer
 * on the queue, and the delivery rate so that slow consumers can be detected.  It also
 * tracks the entries which the consumer has acquired.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Subscription(val queue:Queue, val consumer:DeliveryConsumer) extends DeliveryProducer with Dispatched {
  import Subscription._

  def dispatch_queue = queue.dispatch_queue

  val id = Queue.subcsription_counter.incrementAndGet
  var acquired = new LinkedNodeList[AcquiredQueueEntry]
  var session: DeliverySession = null
  var pos:QueueEntry = null

  var acquired_size = 0L
  def acquired_count = acquired.size()

  var enqueue_size_per_interval = new CircularBuffer[Int](15)

  def avg_enqueue_size_per_interval = {
    var rc = 0
    if( enqueue_size_per_interval.size > 0 ) {
      for( x <- enqueue_size_per_interval ) {
        rc += x
      }
      rc = rc/ enqueue_size_per_interval.size
    }
    rc
  }


  var enqueue_size_at_last_interval = 0L

  var consumer_stall_ms = 0L
  var load_stall_ms = 0L

  var consumer_stall_start = 0L
  var load_stall_start = 0L

  var started_at = Broker.now
  var total_ack_count = 0L
  var total_ack_size = 0L
  var total_nack_count = 0L

  var idle_start = System.nanoTime()
  var idle_total = 0L

  def ack_rates = {
    var duration = ((Broker.now - started_at)*1000000)
    duration -= idle_total
    if( idle_start!=0 ) {
      duration -= System.nanoTime() - idle_start
    }

    if( duration != 0 && total_ack_count > 0 ) {
      val ack_rate = 1000000000d * total_ack_count / duration
      val ack_size_rate = 1000000000d * total_ack_size / duration
      Some((ack_rate, ack_size_rate))
    } else {
      None
    }
  }

  override def toString = {
    def seq(entry:QueueEntry) = if(entry==null) null else entry.seq
    "{ id: "+id+", acquired_size: "+acquired_size+", pos: "+seq(pos)+"}"
  }

  def browser = consumer.browser
  def exclusive = consumer.exclusive

  val consumer_buffer = consumer.receive_buffer_size

  // This opens up the consumer
  def open() = {
    consumer.retain
    if(consumer.start_from_tail) {
      pos = queue.tail_entry;
    } else {
      pos = queue.head_entry;
    }
    assert(pos!=null)
    consumer.set_starting_seq(pos.seq)

    session = consumer.connect(this)
    session.refiller = dispatch_queue.runnable {
      if(session!=null) {
        check_consumer_stall
      }
      if( pos!=null ) {
        pos.task.run
      }
    }
    pos ::= this

    queue.all_subscriptions += consumer -> this
    queue.consumer_counter += 1
    queue.change_consumer_capacity( consumer_buffer )

    if( exclusive ) {
      queue.exclusive_subscriptions.append(this)
    }

    if( queue.service_state.is_started ) {
      // kick off the initial dispatch.
      refill_prefetch
      queue.dispatch_queue << pos.task
    }
    queue.check_idle
  }

  var pending_close_action: ()=>Unit = _

  def check_finish_close = {
    // We can complete the closing of the sub
    // once the outstanding acks are settled.
    if (pending_close_action!=null && acquired.isEmpty) {
      pending_close_action()
      pending_close_action = null
    }
  }

  def close() = {
    if(pos!=null) {
      pos -= this
      pos = null

      queue.exclusive_subscriptions = queue.exclusive_subscriptions.filterNot( _ == this )
      queue.all_subscriptions -= consumer

      session.refiller = NOOP
      session.close
      session = null

      // The following action gets executed once all acquired messages
      // ared acked or nacked.
      pending_close_action = ()=> {
        queue.change_consumer_capacity( - consumer_buffer )

        if( exclusive ) {
          // rewind all the subs to the start of the queue.
          queue.all_subscriptions.values.foreach(_.rewind(queue.head_entry))
        }

        queue.check_idle
        queue.trigger_swap
      }

      consumer.release
      check_finish_close
    } else {}
  }

  /**
   * Advances the subscriptions position to the specified
   * queue entry.
   */
  def advance(value:QueueEntry):Unit = {
    assert(value!=null)
    pos = value
    check_load_stall
    if( tail_parked ) {
        if(consumer.close_on_drain) {
          close
        }
    }
  }

  /**
   * Rewinds to a previously seen location.. Happens when
   * a nack occurs from another consumer.
   */
  def rewind(value:QueueEntry):Unit = {
    assert(value!=null)
    pos -= this
    value ::= this
    pos = value
    check_load_stall
    queue.dispatch_queue << value.task // queue up the entry to get dispatched..
  }

  def tail_parked = pos eq queue.tail_entry

  def matches(entry:Delivery) = consumer.matches(entry)
  def full = session.full

  def offer(delivery:Delivery) = try {
    assert(delivery.seq > 0 )
    session.offer(delivery)
  } finally {
    check_consumer_stall
  }

  def acquire(entry:QueueEntry) = new AcquiredQueueEntry(entry)

  def check_load_stall = {
    if ( pos.is_swapped_or_swapped_range ) {
      if(load_stall_start==0) {
        load_stall_start = queue.virtual_host.broker.now
      }
    } else {
      if(load_stall_start!=0) {
        load_stall_ms += queue.virtual_host.broker.now - load_stall_start
        load_stall_start = 0
      }
    }
  }

  def check_consumer_stall = {
    if ( full ) {
      if(consumer_stall_start==0) {
        consumer_stall_start = queue.virtual_host.broker.now
      }
    } else {
      if( consumer_stall_start!=0 ) {
        consumer_stall_ms += queue.virtual_host.broker.now - consumer_stall_start
        consumer_stall_start = 0
      }
    }
  }

  def adjust_prefetch_size = {
    enqueue_size_per_interval += (session.enqueue_size_counter - enqueue_size_at_last_interval).toInt
    enqueue_size_at_last_interval = session.enqueue_size_counter

    if(consumer_stall_start !=0) {
      val now = queue.virtual_host.broker.now
      consumer_stall_ms += now - consumer_stall_start
      consumer_stall_start = now
    }

    if(load_stall_start !=0) {
      val now = queue.virtual_host.broker.now
      load_stall_ms += now - load_stall_start
      load_stall_start = now
    }

    val rc = (consumer_stall_ms, load_stall_ms)
    consumer_stall_ms = 0
    load_stall_ms = 0
    rc
  }

  def refill_prefetch = {

    var cursor = if( pos.is_tail ) {
      null // can't prefetch the tail..
    } else if( pos.is_head ) {
      pos.getNext // can't prefetch the head.
    } else {
      pos // start prefetching from the current position.
    }

    var remaining = consumer_buffer;
    while( remaining>0 && cursor!=null ) {
      val next = cursor.getNext
      // Browsers prefetch all messages..
      // Non-Browsers prefetch non-acquired messages.
      if( !cursor.prefetched && (browser || !cursor.is_acquired) ) {
        remaining -= cursor.size
        cursor.prefetched = true
        cursor.load(queue.consumer_swapped_in)
      }
      cursor = next
    }

    // If we hit the tail or the producer swap in area.. let the queue know we are keeping up.
    if( !queue.consumers_keeping_up && (cursor == null || (cursor.as_loaded!=null && (cursor.as_loaded.space eq queue.producer_swapped_in))) ) {
      queue.consumers_keeping_up = true
    }

  }

  class AcquiredQueueEntry(val entry:QueueEntry) extends LinkedNode[AcquiredQueueEntry] {

    if(acquired.isEmpty) {
      idle_total = System.nanoTime() - idle_start
      idle_start = 0
    }

    acquired.addLast(this)
    acquired_size += entry.size

    def ack(uow:StoreUOW):Unit = {
      assert_executing
      if(!isLinked) {
        debug("Unexpected ack: message seq allready acked: "+entry.seq)
        return
      }

      total_ack_count += 1
      total_ack_size += entry.size
      entry.dequeue(uow)

      // removes this entry from the acquired list.
      unlink()
      if( acquired.isEmpty ) {
        idle_start = System.nanoTime()
      }

      // we may now be able to prefetch some messages..
      acquired_size -= entry.size

      val next = entry.nextOrTail
      entry.remove // entry size changes to 0

      queue.trigger_swap
      next.task.run
      check_finish_close

    }

    def increment_nack = total_nack_count += 1

    def nack:Unit = {
      assert_executing
      if(!isLinked) {
        debug("Unexpected nack: message seq allready acked: "+entry.seq)
        return
      }

      entry.state match {
        case x:entry.Loaded=> x.acquirer = null
        case x:entry.Swapped=> x.acquirer = null
      }
      acquired_size -= entry.size

      // track for stats
      queue.nack_item_counter += 1
      queue.nack_size_counter += entry.size
      queue.nack_ts = queue.now

      // The following does not need to get done for exclusive subs because
      // they end up rewinding all the sub of the head of the queue.
      if( !exclusive ) {
        // rewind all the matching competing subs past the entry.. back to the entry
        val loaded = entry.as_loaded
        queue.all_subscriptions.valuesIterator.foreach{ sub=>
          val matches = if( loaded!=null ) {
            // small perf optimization.. no need to rewind if the
            // consumer is not interested in the message. (not the typical case).
            sub.matches(loaded.delivery)
          } else {
            true // if message was not loaded lets just assume it was.
          }
          if( !sub.browser && entry.seq < sub.pos.seq && matches) {
            sub.rewind(entry)
          }

        }

      }
      unlink()
      if( acquired.isEmpty ) {
        idle_start = System.nanoTime()
      }
      check_finish_close
    }
  }

}
