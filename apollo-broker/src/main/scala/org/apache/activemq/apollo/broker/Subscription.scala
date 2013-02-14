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
import org.apache.activemq.apollo.dto.QueueConsumerLinkDTO

trait Acquirer
object DeadLetterHandler extends Acquirer

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
class Subscription(val queue:Queue, val consumer:DeliveryConsumer) extends Acquirer with DeliveryProducer with Dispatched with StallCheckSupport {
  import Subscription._

  def dispatch_queue = queue.dispatch_queue

  val id = Queue.subscription_counter.incrementAndGet
  var acquired = new LinkedNodeList[AcquiredQueueEntry]
  var session: DeliverySession = null
  var pos:QueueEntry = null

  var acquired_size = 0L
  def acquired_count = acquired.size()

  var enqueue_size_per_interval = new CircularBuffer[Int](15)

  def create_link_dto(include_metrics:Boolean=true) = {
    val link = new QueueConsumerLinkDTO
    consumer.connection match {
      case Some(connection) =>
        link.kind = "connection"
        link.id = connection.id.toString
        link.label = connection.transport.getRemoteAddress.toString
      case _ =>
        link.kind = "unknown"
        link.label = "unknown"
    }
    if ( include_metrics ) {
      link.position = pos.seq
      link.enqueue_item_counter = session.enqueue_item_counter
      link.enqueue_size_counter = session.enqueue_size_counter
      link.enqueue_ts = session.enqueue_ts
      link.total_ack_count = total_ack_count
      link.total_nack_count = total_nack_count
      link.acquired_size = acquired_size
      link.acquired_count = acquired_count
      ack_rates match {
        case Some((items_per_sec, size_per_sec) ) =>
          link.ack_item_rate = items_per_sec
          link.ack_size_rate = size_per_sec
        case _ =>
      }
      link.waiting_on = if( full ) {
        "consumer"
      } else if( pos.is_tail ) {
        "producer"
      } else if( !pos.is_loaded ) {
        "load"
      } else {
        "dispatch"
      }
    }
    link
  }

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
        stall_check
      }
      if( pos!=null ) {
        pos.task.run
      }
    }
    pos ::= this

    queue.all_subscriptions += consumer -> this
    if( !consumer.browser && queue._message_group_buckets != null ) {

      var iterators = queue._message_group_buckets.add(GroupBucket(this), 10)

      // If we are doing graceful handoffs of message groups...
      if( queue.message_group_graceful_handoff ) {
        import collection.JavaConversions._
        for ( iterator <- iterators ) {

          // When we add the new bucket, it's going to get assigned
          // message groups that were previously being serviced by the next
          // bucket.  We need to suspend dispatch to both these groups
          // until all dispatched messages get ack/drained, so that
          // messages groups are not being concurrently being processed
          // by two subscriptions.

          var taking_over:Subscription = null
          while ( iterator.hasNext && taking_over==null) {
            val next = iterator.next()
            if( next.sub != this ) {
              taking_over = next.sub
            }
          }

          // If we are taking over
          if( taking_over!=null ) {
            this.suspend
            taking_over.suspend
            taking_over.on_drain {
              resume
              taking_over.resume
            }
          }
        }
      }
    }

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

  var suspend_count = 0;

  def suspend = suspend_count+=1
  def resume = {
    suspend_count-=1
    if( suspend_count <= 0) {
      queue.dispatch_queue << pos.task
    }
  }

  def on_drain(func: =>Unit) {
    drain_watchers ::= func _
    check_drained
  }

  var drain_watchers: List[()=>Unit] = Nil

  def check_drained = {
    // We can complete the closing of the sub
    // once the outstanding acks are settled.
    if (acquired.isEmpty && drain_watchers!=Nil) {
      val t = drain_watchers
      drain_watchers = Nil
      for ( action <- t) {
        action()
      }
    }
  }

  def close() = {
    if(pos!=null) {
      pos -= this
      pos = null

      queue.exclusive_subscriptions = queue.exclusive_subscriptions.filterNot( _ == this )
      queue.all_subscriptions -= consumer
      if( !consumer.browser && queue._message_group_buckets != null ) {
        queue._message_group_buckets.remove(GroupBucket(this))
        if( queue._message_group_buckets.getNodes.isEmpty ) {
          queue._message_group_buckets = null
        }
      }

      session.refiller = NOOP
      session.close
      session = null

      // The following action gets executed once all acquired messages
      // ared acked or nacked.

      consumer.release
      on_drain {
        queue.change_consumer_capacity( - consumer_buffer )

        if( exclusive ) {
          // rewind all the subs to the start of the queue.
          queue.all_subscriptions.values.foreach(_.rewind(queue.head_entry))
        }

        queue.check_idle
        queue.trigger_swap
      }
    } else {}
  }

  /**
   * Advances the subscriptions position to the specified
   * queue entry.
   */
  def advance(value:QueueEntry):Unit = {
    assert(value!=null)
    pos = value
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
    queue.dispatch_queue << value.task // queue up the entry to get dispatched..
  }

  def tail_parked = pos eq queue.tail_entry

  def matches(entry:Delivery) = consumer.matches(entry)
  def full = suspend_count > 0 || session.full

  def offer(delivery:Delivery) = try {
    assert(delivery.seq > 0 )
    if( full ) {
      false
    } else {
      val accepted = session.offer(delivery)
      assert(accepted)
      true
    }
  } finally {
    stall_check
  }

  def acquire(entry:QueueEntry) = new AcquiredQueueEntry(entry)


  def adjust_prefetch_size = {
    enqueue_size_per_interval += (session.enqueue_size_counter - enqueue_size_at_last_interval).toInt
    enqueue_size_at_last_interval = session.enqueue_size_counter
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
        debug("Unexpected ack: message seq already acked: "+entry.seq)
        return
      }

      total_ack_count += 1
      total_ack_size += entry.size
      remove(uow)
    }

    def remove(uow:StoreUOW):Unit = {
      assert_executing
      val next = entry.getNext
      entry.dequeue(uow)

      // removes this entry from the acquired list.
      unlink()
      if( acquired.isEmpty ) {
        idle_start = System.nanoTime()
      }

      // we may now be able to prefetch some messages..
      acquired_size -= entry.size

      entry.remove // entry size changes to 0
      queue.trigger_swap
      if( next!=null ) {
        next.task.run
      }
      check_drained

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
      check_drained
    }
  }

}

trait StallCheckSupport {
  def full:Boolean

  var stall_start = Broker.now
  var stall_ms = 0L

  def reset_stall_timer = {
    if( stall_start!=0 ) {
      val now = Broker.now
      stall_ms += now - stall_start
      stall_start = now
    }
    val rc = stall_ms
    stall_ms = 0
    rc
  }

  def stall_check = {
    if ( full ) {
      if(stall_start==0) {
        stall_start = Broker.now
      }
    } else {
      if( stall_start!=0 ) {
        stall_ms += Broker.now - stall_start
        stall_start = 0
      }
    }
  }

}
