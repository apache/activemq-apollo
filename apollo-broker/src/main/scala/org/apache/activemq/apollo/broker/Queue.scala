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

import java.util.concurrent.TimeUnit

import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.broker.store._
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.util.list._
import org.fusesource.hawtdispatch.{ListEventAggregator, DispatchQueue, BaseRetained}
import OptionSupport._
import java.util.concurrent.atomic.AtomicInteger
import security.SecuredResource._
import security.{SecuredResource, SecurityContext}
import org.apache.activemq.apollo.dto._
import java.util.regex.Pattern
import collection.mutable.ListBuffer
import org.fusesource.hawtbuf.Buffer
import org.apache.activemq.apollo.broker.{DeliveryResult, Subscription}

object Queue extends Log {
  val subscription_counter = new AtomicInteger(0)

  class MemorySpace {
    var items = 0
    var size = 0
    var size_max = 0

    def +=(delivery:Delivery) = {
      items += 1
      size += delivery.size
    }

    def -=(delivery:Delivery) = {
      items -= 1
      size -= delivery.size
    }
  }

}

import Queue._

case class GroupBucket(sub:Subscription) {
  override def toString: String = sub.id.toString
}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Queue(val router: LocalRouter, val store_id:Long, var binding:Binding) extends BaseRetained with BindableDeliveryProducer with DeliveryConsumer with BaseService with DomainDestination with DeferringDispatched with SecuredResource {
  override def toString = binding.toString

  def virtual_host = router.virtual_host

  val resource_kind = binding match {
    case x:DurableSubscriptionQueueBinding=> DurableSubKind
    case x:QueueDomainQueueBinding=> QueueKind
    case x:TempQueueBinding => TopicQueueKind
    case _ => OtherKind
  }

  var producers = ListBuffer[BindableDeliveryProducer]()
  var inbound_sessions = Set[DeliverySession]()
  var all_subscriptions = Map[DeliveryConsumer, Subscription]()
  var exclusive_subscriptions = ListBuffer[Subscription]()

  var _message_group_buckets: HashRing[GroupBucket, String] = _

  def message_group_buckets = {
    // If the queue is not using message groups, lets avoid
    // creating the bucket hash ring.
    if( _message_group_buckets == null )  {
      _message_group_buckets = new HashRing[GroupBucket, String]()
      // Create a bucket for each subscription
      for( sub <- all_subscriptions.values if !sub.browser) {
        _message_group_buckets.add(GroupBucket(sub), 10)
      }
    }
    _message_group_buckets
  }

  def filter = binding.message_filter

  override val dispatch_queue: DispatchQueue = createQueue(id);

  def address = binding.address

  debug("created queue: " + id)

  val session_manager = new SessionSinkMux[Delivery](messages, dispatch_queue, Delivery, Integer.MAX_VALUE, 1024*640) {
    override def time_stamp = now
  }

  // sequence numbers.. used to track what's in the store.
  var message_seq_counter = 1L

  val entries = new LinkedNodeList[QueueEntry]()
  val head_entry = new QueueEntry(this, 0L).head
  var tail_entry = new QueueEntry(this, next_message_seq)
  entries.addFirst(head_entry)

  //
  // Frequently accessed tuning configuration.
  //

  /**
   * Should this queue persistently store it's entries?
   */
  var tune_persistent = true

  /**
   * Should use a round robin dispatching of messages?
   */
  var tune_round_robin = true

  /**
   * Should messages be swapped out of memory if
   * no consumers need the message?
   */
  var tune_swap = true

  /**
   * The number max number of swapped queue entries to load
   * for the store at a time.  Note that swapped entries are just
   * reference pointers to the actual messages.  When not loaded,
   * the batch is referenced as sequence range to conserve memory.
   */
  var tune_swap_range_size = 0

  /**
   *  The max memory to allow this queue to grow to.
   */
  var tune_quota = -1L
  var tune_quota_messages = -1L

  /**
   *  The message delivery rate (in bytes/sec) at which
   *  the queue enables a enqueue rate throttle
   *  to allow consumers to catchup with producers.
   */
  var tune_fast_delivery_rate = 0
  
  /**
   *  The rate at which to throttle producers when
   *  consumers are catching up.  
   */
  var tune_catchup_enqueue_rate = 0

  /**
   *  The rate at which producers are throttled at.
   */
  var tune_max_enqueue_rate = 0

  var now = System.currentTimeMillis

  var enqueue_item_counter = 0L
  var enqueue_size_counter = 0L
  var enqueue_ts = now;

  var dequeue_item_counter = 0L
  var dequeue_size_counter = 0L
  var dequeue_ts = now;

  var nack_item_counter = 0L
  var nack_size_counter = 0L
  var nack_ts = now;

  var expired_item_counter = 0L
  var expired_size_counter = 0L
  var expired_ts = now;

  def queue_size = enqueue_size_counter - dequeue_size_counter
  def queue_items = enqueue_item_counter - dequeue_item_counter

  var swapping_in_size = 0
  var swapping_out_size = 0

  val producer_swapped_in = new MemorySpace { override def toString = "producer_swapped_in" }
  val consumer_swapped_in = new MemorySpace { override def toString = "consumer_swapped_in" }

  var swap_out_item_counter = 0L
  var swap_out_size_counter = 0L

  var swap_in_item_counter = 0L
  var swap_in_size_counter = 0L

  var producer_counter = 0L
  var consumer_counter = 0L

  // This set to true if any consumer kept up within the
  // last second.
  var consumers_keeping_up_historically = false

  var individual_swapped_items = 0

  var swap_triggered = false
  def trigger_swap = {
    dispatch_queue.assertExecuting()
    if( tune_swap && !swap_triggered ) {
      swap_triggered = true
      defer {
        swap_triggered = false
        swap_messages
      }
    }
  }

  var restored_from_store = false

  var auto_delete_after = 0
  var idled_at = 0L

  var loaded_items = 0
  var loaded_size = 0
  def swapped_in_size_max = this.producer_swapped_in.size_max + this.consumer_swapped_in.size_max

  var config:QueueSettingsDTO = _
  var full_policy:FullDropPolicy = Block

  def dlq_nak_limit = OptionSupport(config.nak_limit).getOrElse(0)
  def dlq_expired = OptionSupport(config.dlq_expired).getOrElse(false)

  def message_group_graceful_handoff = OptionSupport(config.message_group_graceful_handoff).getOrElse(true)

  def configure(update:QueueSettingsDTO) = {
    def mem_size(value:String, default:String) = MemoryPropertyEditor.parse(Option(value).getOrElse(default)).toInt

    var new_tail_buffer = mem_size(update.tail_buffer, "640k")
    var old_tail_buffer = Option(config).map { config =>
      mem_size(config.tail_buffer, "640k")
    }.getOrElse(0)

    producer_swapped_in.size_max += new_tail_buffer - old_tail_buffer
    session_manager.resize(Int.MaxValue, new_tail_buffer)

    tune_persistent = virtual_host.store !=null && update.persistent.getOrElse(true)
    tune_round_robin = update.round_robin.getOrElse(true)
    tune_swap = tune_persistent && update.swap.getOrElse(true)
    tune_swap_range_size = update.swap_range_size.getOrElse(10000)
    tune_fast_delivery_rate = mem_size(update.fast_delivery_rate,"512k")
    tune_catchup_enqueue_rate = mem_size(update.catchup_enqueue_rate,"-1")
    tune_max_enqueue_rate = mem_size(update.max_enqueue_rate,"-1")
    tune_quota = mem_size(update.quota,"-1")
    tune_quota_messages = update.quota_messages.getOrElse(-1L)

    full_policy = Option(update.full_policy).getOrElse("block").toLowerCase match {
      case "drop head" => DropHead
      case "drop tail" => DropTail
      case "block" => Block
      case _ =>
        warn("Invalid 'full_policy' configured for queue '%s': '%s'", id, update.full_policy)
        Block
    }

    update match {
      case update:QueueDTO =>
        auto_delete_after = update.auto_delete_after.getOrElse(30)
        if( auto_delete_after!= 0 ) {
          // we don't auto delete explicitly configured queues,
          // non destination queues, or mirrored queues.
          if( update.mirrored.getOrElse(false) || !binding.isInstanceOf[QueueDomainQueueBinding] || !LocalRouter.is_wildcard_destination(update.id) ) {
            auto_delete_after = 0
          }
        }
      case _ =>
    }
    config = update
    this
  }

  def mirrored = config match {
    case config:QueueDTO =>
      config.mirrored.getOrElse(false)
    case _ => false
  }

  def get_queue_metrics:DestMetricsDTO = {
    dispatch_queue.assertExecuting()
    val rc = new DestMetricsDTO

    rc.enqueue_item_counter = this.enqueue_item_counter
    rc.enqueue_size_counter = this.enqueue_size_counter
    rc.enqueue_ts = this.enqueue_ts

    rc.dequeue_item_counter = this.dequeue_item_counter
    rc.dequeue_size_counter = this.dequeue_size_counter
    rc.dequeue_ts = this.dequeue_ts

    rc.nack_item_counter = this.nack_item_counter
    rc.nack_size_counter = this.nack_size_counter
    rc.nack_ts = this.nack_ts

    rc.expired_item_counter = this.expired_item_counter
    rc.expired_size_counter = this.expired_size_counter
    rc.expired_ts = this.expired_ts

    rc.queue_size = this.queue_size
    rc.queue_items = this.queue_items

    rc.swap_out_item_counter = this.swap_out_item_counter
    rc.swap_out_size_counter = this.swap_out_size_counter
    rc.swap_in_item_counter = this.swap_in_item_counter
    rc.swap_in_size_counter = this.swap_in_size_counter

    rc.swapping_in_size = this.swapping_in_size
    rc.swapping_out_size = this.swapping_out_size

    rc.swapped_in_items = this.loaded_items
    rc.swapped_in_size = this.loaded_size
    rc.swapped_in_size_max = swapped_in_size_max

    rc.producer_counter = this.producer_counter
    rc.consumer_counter = this.consumer_counter

    rc.producer_count = this.inbound_sessions.size
    rc.consumer_count = this.all_subscriptions.size
    rc
  }

  def browse(from_seq:Long, to:Option[Long], max:Long)(func: (BrowseResult)=>Unit):Unit = {
    var result = ListBuffer[(EntryStatusDTO, Delivery)]()
    def load_from(start:Long):Unit = {
      assert_executing
      var cur = head_entry.getNext
      while(true) {
        if( cur == null || result.size >= max || ( to.isDefined && cur.seq > to.get) ) {
          func(BrowseResult(head_entry.seq, head_entry.getPreviousCircular.seq, enqueue_item_counter, result.toArray))
          return
        }
        val next = cur.getNext
        if ( cur.seq >= start ) {
          cur.state match {
            case state:QueueEntry#Loaded =>
              result.append((create_entry_status(cur), state.delivery))
            case state:QueueEntry#Swapped =>
              state.swapped_in_watchers ::=(()=>{
                load_from(cur.seq) // resume loading
              })
              cur.load(consumer_swapped_in)
              return
            case state:QueueEntry#SwappedRange =>
              state.swapped_in_watchers ::=(()=>{
                load_from(cur.seq)
              })
              cur.load(consumer_swapped_in)
              return
          }
        }
        cur = next
      }
    }
    load_from(from_seq)
  }

  def status(entries:Boolean=false, include_producers:Boolean=false, include_consumers:Boolean=false) = {
    val rc = new QueueStatusDTO
    rc.id = this.id
    binding match {
      case binding:TempQueueBinding =>
        rc.id = store_id.toString
      case _ =>
    }
    rc.state = this.service_state.toString
    rc.state_since = this.service_state.since
    rc.binding = this.binding.dto
    rc.config = this.config
    if( max_enqueue_rate < Int.MaxValue ) {
      rc.max_enqueue_rate = new java.lang.Integer(max_enqueue_rate)
    }
    rc.metrics = this.get_queue_metrics
    rc.metrics.current_time = now

    if( entries ) {
      var cur = this.head_entry
      while( cur!=null ) {
        rc.entries.add(create_entry_status(cur))
        cur = if( cur == this.tail_entry ) {
          null
        } else {
          cur.nextOrTail
        }
      }
    } else {
      rc.entries = null
    }

    if( include_producers ) {
      for ( session <- this.inbound_sessions ) {
        val link = new LinkDTO()
        session.producer.connection match {
          case Some(connection) =>
            link.kind = "connection"
            link.id = connection.id.toString
            link.label = connection.transport.getRemoteAddress.toString
          case _ =>
            link.kind = "unknown"
            link.label = "unknown"
        }
        link.enqueue_item_counter = session.enqueue_item_counter
        link.enqueue_size_counter = session.enqueue_size_counter
        link.enqueue_ts = session.enqueue_ts
        rc.producers.add(link)
      }
    } else {
      rc.producers = null
    }

    if( include_consumers ) {
      for( sub <- this.all_subscriptions.values ) {
        rc.consumers.add(sub.create_link_dto())
      }
    } else {
      rc.consumers = null
    }
    rc
  }


  def create_entry_status(cur: QueueEntry): EntryStatusDTO = {
    val rc = new EntryStatusDTO
    rc.seq = cur.seq
    rc.count = cur.count
    rc.size = cur.size
    rc.consumer_count = cur.parked.size
    rc.is_prefetched = cur.prefetched
    rc.state = cur.label
    rc.acquirer = cur.acquiring_subscription match {
      case sub:Subscription => sub.create_link_dto(false)
      case _ => null
    }
    rc
  }

  def update(on_completed:Task) = dispatch_queue {

    val prev_persistent = tune_persistent

    configure(binding.config(virtual_host))

    restore_from_store {
      check_idle
      trigger_swap
      on_completed.run
    }
  }

  def check_idle {
    if (inbound_sessions.isEmpty && all_subscriptions.isEmpty && queue_items==0 ) {
      if (idled_at==0 && auto_delete_after!=0) {
        idled_at = now
        val idled_at_start = idled_at
        dispatch_queue.after(auto_delete_after, TimeUnit.SECONDS) {
          // Have we been idle that whole time?
          if( idled_at == idled_at_start ) {
            virtual_host.dispatch_queue {
              if( virtual_host.service_state.is_started ) {
                router._destroy_queue(this)
              }
            }
          }
        }
      }
    } else {
      idled_at = 0
    }
  }

  def restore_from_store(on_completed: => Unit) {
    if (!restored_from_store && tune_persistent) {
      restored_from_store = true
      virtual_host.store.list_queue_entry_ranges(store_id, tune_swap_range_size) { ranges =>
        dispatch_queue {
          if (ranges != null && !ranges.isEmpty) {

            ranges.foreach {
              range =>
                val entry = new QueueEntry(Queue.this, range.first_entry_seq).init(range)
                entries.addLast(entry)

                message_seq_counter = range.last_entry_seq + 1
                enqueue_item_counter += range.count
                enqueue_size_counter += range.size
                tail_entry = new QueueEntry(Queue.this, next_message_seq)
            }

            all_subscriptions.valuesIterator.foreach( _.rewind(head_entry) )
            debug("restored: " + enqueue_item_counter)
          }
          on_completed
        }
      }
    } else {
      on_completed
    }
  }

  protected def _start(on_completed: Task) = {
    restore_from_store {


      // by the time this is run, consumers and producers may have already joined.
      on_completed.run
      schedule_reoccurring(1, TimeUnit.SECONDS) {
        queue_maintenance
      }

      // wake up the producers to fill us up...
      if (messages.refiller != null) {
        messages.refiller.run
      }

      // kick off dispatching to the consumers.
      check_idle
      trigger_swap
      dispatch_queue << head_entry.task

    }
  }

  var stop_listener_waiting_for_flush:Task = _

  protected def _stop(on_completed: Task) = {

    // Now that we are stopping the queue will no longer be 'full'
    // draining will nack all enqueue attempts.
    messages.refiller.run

    // Disconnect the producers..
    producers.foreach { producer =>
      disconnect(producer)
    }
    // Close all the subscriptions..
    all_subscriptions.values.toArray.foreach { sub:Subscription =>
      sub.close()
    }

    if( dlq_route!=null ) {
      val route = dlq_route
      dlq_route = null
      virtual_host.dispatch_queue {
        router.disconnect(route.addresses, route)
      }
    }

    trigger_swap

    stop_listener_waiting_for_flush = on_completed
    if( swapping_out_size==0 ) {
      on_queue_flushed
    }
  }

  def on_queue_flushed = {
    if(stop_listener_waiting_for_flush!=null) {
      stop_listener_waiting_for_flush.run()
      stop_listener_waiting_for_flush = null
    }
  }

  def might_unfill[T](func: =>T):T = {
    val was_full = messages.full
    try {
      func
    } finally {
      if( was_full && !messages.full ) {
        messages.stall_check
        messages.refiller.run
      }
    }
  }

  def change_consumer_capacity(amount:Int) = might_unfill {
    consumer_swapped_in.size_max += amount
  }


  def is_topic_queue = resource_kind eq TopicQueueKind
  def create_uow:StoreUOW = if(virtual_host.store==null) null else virtual_host.store.create_uow
  def create_uow(uow:StoreUOW):StoreUOW = if(uow==null) create_uow else {uow.retain; uow}

  object messages extends Sink[(Session[Delivery], Delivery)] {
    def stall_check = {}
    var refiller: Task = null

    def is_quota_exceeded = (tune_quota >= 0 && queue_size > tune_quota) || (tune_quota_messages >= 0 && queue_items > tune_quota_messages)
    def is_enqueue_throttled = (enqueues_remaining!=null && enqueues_remaining.get() <= 0)
    def is_enqueue_buffer_maxed = (producer_swapped_in.size >= producer_swapped_in.size_max)

    def full = if( service_state.is_started ) {
      if ( full_policy eq Block ) {
        is_enqueue_buffer_maxed || is_enqueue_throttled || is_quota_exceeded
      } else {
        // we are never full since we can just drop messages at will.
        false
      }
    } else if( service_state.is_starting) {
      true
    } else {
      false
    }

    def offer(event: (Session[Delivery], Delivery)): Boolean = {
      if (full) {
        false
      } else {
        val (session, delivery) = event
        session_manager.delivered(session, delivery.size)
        // We may need to drop this enqueue or head entries due
        // to the drop policy.
        var drop = false

        if( is_topic_queue && all_subscriptions.isEmpty ) {
          // no need to queue it..
          drop = true
        } else if( full_policy ne Block ) {

          def eval_drop(entry:QueueEntry) = entry.state match {
            case state: entry.Loaded =>
              var next = entry.getNext
              if (!entry.is_acquired) {
                entry.dequeue(null)
                entry.remove
              }
              next
            case state: entry.Swapped =>
              var next = entry.getNext
              if (!entry.is_acquired) {
                entry.dequeue(null)
                entry.remove
              }
              next
            case state: entry.SwappedRange =>
              // we need to load in the range before we can drop entries..
              entry.load(null)
              null
          }

          if( tune_persistent ) {
            var exceeded = is_quota_exceeded
            if( exceeded) {
              full_policy match {
                case Block =>
                case DropTail =>
                  drop = true // we can drop this enqueue attempt.
                case DropHead =>
                  var entry = head_entry.getNext
                  while(entry!=null && is_quota_exceeded) {
                    entry = eval_drop(entry)
                  }
              }
            }
          } else {
            if( is_enqueue_buffer_maxed) {
              full_policy match {
                case DropTail =>
                  drop = true // we can drop this enqueue attempt.
                case DropHead =>
                  var entry = head_entry.getNext
                  while(entry!=null && is_enqueue_buffer_maxed) {
                    entry = eval_drop(entry)
                  }
                case _ =>
              }
            }
          }
        }
        
        val expiration = delivery.expiration
        val expired = expiration != 0 && expiration <= now

        // Don't even enqueue if the message has expired or
        // the queue has stopped or message needs to get dropped.
        if( !service_state.is_started || expired || drop) {
          if( delivery.ack!=null ) {
            delivery.ack(if ( expired ) Expired else Undelivered, delivery.uow)
          }
          if( delivery.persistent && tune_persistent ) {
            assert(delivery.uow!=null)
            delivery.uow.release()
          }
          return true
        }

        val entry = tail_entry
        tail_entry = new QueueEntry(Queue.this, next_message_seq)
        val queue_delivery = delivery.copy
        queue_delivery.seq = entry.seq
        entry.init(queue_delivery)
        
        entries.addLast(entry)
        enqueue_item_counter += 1
        enqueue_size_counter += entry.size
        enqueue_ts = now;

        // To decrease the enqueue throttle.
        enqueue_remaining_take(entry.size)

        // Do we need to do a persistent enqueue???
        val uow = if( queue_delivery.persistent && tune_persistent ) {
          assert(delivery.uow !=null)
          val uow = delivery.uow
          entry.state match {
            case state:entry.Loaded =>
              // Little hack to expand the producer memory window for persistent
              // messages until the uow completes.  Sender might be sending a very
              // larger UOW which does not fit in the window and then the UOW does
              // not finish.
              producer_swapped_in.size_max += delivery.size
              uow.on_flush { canceled =>
                defer {
                  producer_swapped_in.size_max -= delivery.size
                }
              }

              state.store_enqueue(uow)
            case state:entry.Swapped =>
              uow.enqueue(entry.toQueueEntryRecord)
          }
          uow
        } else {
          null
        }

        if( entry.hasSubs ) {
          // try to dispatch it directly...
          entry.dispatch
        }

        // entry might get dispatched and removed.
        if( entry.isLinked ) {
          if( !consumers_keeping_up_historically  ) {
            entry.swap(true)
          } else if( entry.is_acquired && uow != null) {
            // If the message as dispatched and it's marked to get persisted anyways,
            // then it's ok if it falls out of memory since we won't need to load it again.
            entry.swap(false)
          }
        }

        if( delivery.ack!=null ) {
          delivery.ack(Consumed, uow)
        }

        // release the store batch...
        if (uow != null) {
          uow.release
        }

        
        if( full ) {
          trigger_swap
        }
        stall_check
        true
      }
    }
  }

  def expired(uow:StoreUOW, entry:QueueEntry)(func: =>Unit):Unit = {
    if( entry.expiring ) {
      func
    } else {
      entry.expiring = true
      expired_ts = now
      expired_item_counter += 1
      expired_size_counter += entry.size
      if( dlq_expired ) {
        dead_letter(uow, entry) { uow =>
          func
        }
      } else {
        func
      }
    }
  }

  def display_stats: Unit = {
    info("contains: %d messages worth %,.2f MB of data, producers are %s, %d/%d buffer space used.", queue_items, (queue_size.toFloat / (1024 * 1024)), {if (messages.full) "being throttled" else "not being throttled"}, loaded_size, swapped_in_size_max)
    info("total messages enqueued %d, dequeues %d ", enqueue_item_counter, dequeue_item_counter)
  }

  def display_active_entries: Unit = {
    var cur = entries.getHead
    var total_items = 0L
    var total_size = 0L
    while (cur != null) {
      if (cur.is_loaded || cur.hasSubs || cur.prefetched || cur.is_swapped_range ) {
        info("  => " + cur)
      }

      total_size += cur.size
      if (cur.is_swapped || cur.is_loaded) {
        total_items += 1
      } else if (cur.is_swapped_range ) {
        total_items += cur.as_swapped_range.count
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

  var keep_up_delivery_rate = 0L
  
  def swap_messages:Unit = {
    dispatch_queue.assertExecuting()

    if( !service_state.is_started )
      return

    var cur = entries.getHead
    while( cur!=null ) {

      // reset the prefetch flags and handle expiration...
      cur.prefetched = false
      val next = cur.getNext

      // handle expiration...
      if( !cur.expiring && cur.expiration != 0 && cur.expiration <= now ) {
        val entry = cur
        cur.state match {
          case x:QueueEntry#SwappedRange =>
            // load the range to expire the messages in it.
            cur.load(null)
          case state:QueueEntry#Swapped =>
            // remove the expired message if it has not been
            // acquired.
            if( !state.is_acquired ) {
              val uow = create_uow
              entry.dequeue(uow)
              expired(uow, entry) {
                if( entry.isLinked ) {
                  entry.remove
                }
              }
            }
          case state:QueueEntry#Loaded =>
            // remove the expired message if it has not been
            // acquired.
            if( !state.is_acquired ) {
              val uow = create_uow
              entry.dequeue(uow)
              expired(uow, entry) {
                if( entry.isLinked ) {
                  entry.remove
                }
              }
            }
          case _ =>
        }
      }
      cur = next
    }

    // Set the prefetch flags
    all_subscriptions.valuesIterator.foreach{ x=>
      x.refill_prefetch
    }

    // swap out messages.
    cur = entries.getHead.getNext
    var dropping_head_entries = is_topic_queue
    var distance_from_last_prefetch = 0L
    while( cur!=null ) {
      val next = cur.getNext
      if ( dropping_head_entries ) {
        if( cur.parked.isEmpty ) {
          if( cur.is_swapped_range ) {
            cur.load(producer_swapped_in)
            dropping_head_entries=false
          } else {
            cur.dequeue(null)
            cur.remove
          }
        } else {
          cur.load(consumer_swapped_in)
          dropping_head_entries = false
        }
      } else {
        if( cur.prefetched ) {
          // Prefteched entries need to get loaded..
          cur.load(consumer_swapped_in)
          distance_from_last_prefetch = 0
        } else {

          // This is a non-prefetched entry.. entires ahead and behind the
          // consumer subscriptions.
          val loaded = cur.as_loaded
          if( loaded!=null ) {
            // It's in memory.. perhaps we need to swap it out..
            if(!consumers_keeping_up_historically) {
              // Swap out ASAP if consumers are not keeping up..
              cur.swap(true)
            } else {
              // Consumers seem to be keeping up.. so we have to be more selective
              // about what gets swapped out..

              if (cur.memory_space eq producer_swapped_in ) {
                // If we think we can catch up in seconds.. lets keep it in producer_swapped_in to
                // pause the producer.
                val max_distance = delivery_rate * 2;
                if( distance_from_last_prefetch < max_distance ) {
                  // Looks like the entry will be used soon..
                  cur.load(producer_swapped_in)
                } else {
                  // Does not look to be anywhere close to the consumer.. so get
                  // rid of it asap.
                  cur.swap(true)
                }
              } else if ( cur.is_acquired ) {
                // Entry was just used...
                cur.load(consumer_swapped_in)
  //              cur.swap(false)
              } else {
                // Does not look to be anywhere close to the consumer.. so get
                // rid of it asap.
                cur.swap(true)
              }
            }
          }

          distance_from_last_prefetch += cur.size
        }
      }
      cur = next
    }                               


    // Combine swapped items into swapped ranges
    if( individual_swapped_items > tune_swap_range_size*2 ) {

      var distance_from_sub = tune_swap_range_size;
      var cur = entries.getHead
      var combine_counter = 0;

      while( cur!=null ) {

        // get the next now.. since cur may get combined and unlinked
        // from the entry list.
        val next = cur.getNext

        if( cur.prefetched ) {
          distance_from_sub = 0
        } else {
          distance_from_sub += 1
          if( cur.can_combine_with_prev ) {
            cur.getPrevious.as_swapped_range.combineNext
            combine_counter += 1
          } else {
            if( cur.is_swapped && !cur.is_acquired && distance_from_sub > tune_swap_range_size ) {
              cur.swapped_range
              combine_counter += 1
            }
          }

        }
        cur = next
      }
      trace("combined %d entries", combine_counter)
    }
    
    if(!messages.full) {
      messages.stall_check
      messages.refiller.run
    }

  }

  def swapped_out_size = queue_size - (producer_swapped_in.size + consumer_swapped_in.size)
  var delivery_rate = 0

  def queue_maintenance:Unit = {
    var elapsed = System.currentTimeMillis-now
    now += elapsed

    delivery_rate = 0
    var avg_browser_delivery_rate = 0
    var avg_sub_stall_ms = 0L

    all_subscriptions.values.foreach{ sub=>
      sub.adjust_prefetch_size
      avg_sub_stall_ms += sub.reset_stall_timer
      if(sub.browser) {
        avg_browser_delivery_rate += sub.avg_enqueue_size_per_interval
      } else {
        delivery_rate += sub.avg_enqueue_size_per_interval
      }
    }

    if ( !all_subscriptions.isEmpty ) {
      avg_sub_stall_ms = avg_sub_stall_ms / all_subscriptions.size
      avg_browser_delivery_rate = avg_browser_delivery_rate / all_subscriptions.size
    }

    // add the browser delivery rate in as an average.
    delivery_rate += avg_browser_delivery_rate

    val rate_adjustment = elapsed.toFloat / 1000.toFloat
    delivery_rate  = (delivery_rate / rate_adjustment).toInt

    consumers_keeping_up_historically = (
      // No brainer.. we see consumers are fast..
      ( delivery_rate > tune_fast_delivery_rate )
      ||
      // also if the queue size is small and there's not much
      // much consumer stalling happening.
      ( queue_size < delivery_rate && avg_sub_stall_ms < 200 )
    )

//    println("delivery_rate:%d, tune_fast_delivery_rate: %d, queue_size: %d, avg_consumer_stall_ms: %d, consumers_keeping_up_historically: %s".
//            format(delivery_rate, tune_fast_delivery_rate, queue_size, avg_sub_stall_ms, consumers_keeping_up_historically))

    // Figure out what the max enqueue rate should be.
    max_enqueue_rate = Int.MaxValue
    if( consumers_keeping_up_historically && swapped_out_size > 0 ) {
      if( tune_catchup_enqueue_rate >= 0 ) {
        max_enqueue_rate = tune_catchup_enqueue_rate
      } else {
        max_enqueue_rate = delivery_rate / 2;
      }
    }
    if(tune_max_enqueue_rate >=0 ) {
      max_enqueue_rate = max_enqueue_rate.min(tune_max_enqueue_rate)
    }

    if( max_enqueue_rate < Int.MaxValue ) {
      if(enqueues_remaining==null) {
        enqueues_remaining = new LongCounter()
        enqueue_throttle_release(enqueues_remaining)
      }
    } else {
      if(enqueues_remaining!=null) {
        enqueues_remaining = null
      }
    }

    swap_messages
    check_idle
  }
    
  var max_enqueue_rate = Int.MaxValue
  var enqueues_remaining:LongCounter = _
  

  def enqueue_remaining_take(amount:Int) = {
    if(enqueues_remaining!=null) {
      enqueues_remaining.addAndGet(-amount)
    }
  }
  
  def enqueue_throttle_release(throttle:LongCounter):Unit = {
    if( enqueues_remaining==throttle ) {
      might_unfill {
        val amount = max_enqueue_rate / 10
        val remaining = throttle.get
//        if(remaining < 0) {
//          throttle.addAndGet(amount)
//        } else {
          throttle.set(amount)
//        }
      }
      dispatch_queue.after(100, TimeUnit.MILLISECONDS) {
        enqueue_throttle_release(throttle)
      }
    }
  }

  class DlqProducerRoute(val addresses:Array[ConnectAddress]) extends DeliveryProducerRoute(router) {
    override def connection = None
    override def dispatch_queue = Queue.this.dispatch_queue
  }

  var dlq_route:DlqProducerRoute = _
  var dlq_overflow:OverflowSink[(Delivery, (StoreUOW)=>Unit)] = _

  def dead_letter(original_uow:StoreUOW, entry:QueueEntry)(removeFunc: (StoreUOW)=>Unit) = {
    assert_executing
    if( config.dlq==null ) {
      removeFunc(null)
    } else {

      def complete(original_delivery:Delivery) = {
        assert_executing
        val delivery = original_delivery.copy()
        delivery.uow = if(delivery.storeKey == -1) {
          null
        } else {
          create_uow(original_uow)
        }
        delivery.expiration=0

        if( dlq_route==null ) {
          val dlq = config.dlq.replaceAll(Pattern.quote("*"), id)
          dlq_route = new DlqProducerRoute(Array(SimpleAddress("queue:"+dlq)))
          router.virtual_host.dispatch_queue {
            val rc = router.connect(dlq_route.addresses, dlq_route, null)
            assert( rc == None ) // Not expecting this to ever fail.
          }

          dlq_overflow = new OverflowSink[(Delivery, (StoreUOW)=>Unit)](dlq_route.flatMap{ x =>
            Some(x._1)
          }) {
            override protected def onDelivered(value: (Delivery, (StoreUOW) => Unit)) {
              val (delivery, callback) = value;
              callback(delivery.uow)
              if( delivery.uow!=null ) {
                delivery.uow.release
                delivery.uow = null
              }
            }
          }
        }
        dlq_overflow.offer((delivery, removeFunc))
      }

      entry.state match {
        case x:entry.Loaded=>
          if( x.swapping_out ) {
            x.acquirer = DeadLetterHandler
            x.on_swap_out ::=( ()=> {
              complete(entry.state match {
                case state:entry.Swapped=>
                  state.to_delivery
                case state:entry.Loaded =>
                  state.delivery
                case state => sys.error("Unexpected type: "+state)
              })
            })
          } else {
            complete(x.delivery)
          }
        case x:entry.Swapped=>
          complete(x.to_delivery)
        case _ =>
          throw new Exception("Invalid queue entry state, it cannot be DQLed.")
      }
    }
  }

  def process_ack(entry:Subscription#AcquiredQueueEntry, consumed:DeliveryResult, uow:StoreUOW) = defer {
    might_unfill {
      consumed match {
        case Consumed =>
          entry.ack(uow)
        case Expired=>
          val actual = create_uow(uow)
          expired(actual, entry.entry) {
            entry.ack(actual)
          }
          if( actual!=null ){
            actual.release
          }
        case Delivered =>
          entry.increment_nack
          entry.entry.redelivered
          entry.nack
        case Undelivered =>
          entry.nack
        case Poisoned =>
          entry.increment_nack
          entry.entry.redelivered
          var limit = dlq_nak_limit
          if( limit>0 && entry.entry.redelivery_count >= limit ) {
            dead_letter(uow, entry.entry) { uow =>
              entry.remove(uow)
            }
          } else {
            entry.nack
          }
      }
      if( uow!=null ) {
        uow.release
      }
    }
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the DeliveryConsumer trait.  Allows this queue
  // to receive messages from producers.
  //
  /////////////////////////////////////////////////////////////////////

  def matches(delivery: Delivery) = filter.matches(delivery.message)

  def is_persistent = tune_persistent

  class QueueDeliverySession(val producer: DeliveryProducer) extends DeliverySession with SessionSinkFilter[Delivery]{
    retain

    def odlToString = Queue.this.toString
    override def toString = {
      "QueueDeliverySession("+
        "queue: "+Queue.this.id +
        ", full:"+full+
        ", "+downstream+
      ")"
    }

    override def consumer = Queue.this

    val downstream = session_manager.open(producer.dispatch_queue)

    dispatch_queue {
      inbound_sessions += this
      producer_counter += 1
    }

    def close = dispatch_queue {
      session_manager.close(downstream, (delivery)=>{
        // We have been closed so we have to nak any deliveries.
        if( delivery.ack!=null ) {
          delivery.ack(Undelivered, delivery.uow)
        }
      })
      inbound_sessions -= this
      release
    }

    def offer(delivery: Delivery) = {
      if (downstream.full) {
        false
      } else {
        if( delivery.message!=null ) {
          delivery.message.retain
        }
        if( delivery.persistent && tune_persistent ) {
          delivery.uow = create_uow(delivery.uow)
        }
        val rc = downstream.offer(delivery)
        assert(rc, "session should accept since it was not full")
        true
      }
    }
  }

  def connect(p: DeliveryProducer) = new QueueDeliverySession(p)

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the Route trait.  Allows consumers to bind/unbind
  // from this queue so that it can send messages to them.
  //
  /////////////////////////////////////////////////////////////////////

  def connected() = {}

  def bind(value: DeliveryConsumer, ctx:SecurityContext, cb: (Result[Zilch, String])=>Unit):Unit = {
    if( ctx!=null ) {
      if( value.browser ) {
        if( !virtual_host.authorizer.can(ctx, "receive", this) ) {
          cb(new Failure("Not authorized to browse the queue"))
          return
        }
      } else {
        if( !virtual_host.authorizer.can(ctx, "consume", this) ) {
          cb(new Failure("Not authorized to consume from the queue"))
          return
        }
      }
    }
    bind(value::Nil, ()=>{ cb(Success(Zilch)) })
  }

  def bind(values: List[DeliveryConsumer], on_bind:()=>Unit) = {
    values.foreach(_.retain)
    dispatch_queue {
      for (consumer <- values) {
        val sub = new Subscription(this, consumer)
        sub.open
        consumer.release()
      }
      on_bind()
    }
  }

  def unbind(values: List[DeliveryConsumer]):Unit = dispatch_queue {
    for (consumer <- values) {
      all_subscriptions.get(consumer) match {
        case Some(subscription) =>
          subscription.close
        case None =>
      }
    }
  }

  def disconnected() = throw new RuntimeException("unsupported")

  def bind(bind_address:BindAddress, consumer: DeliveryConsumer, on_bind:()=>Unit) = {
    bind(consumer::Nil, on_bind)
  }
  def unbind(consumer: DeliveryConsumer, persistent:Boolean):Unit = {
    unbind(consumer::Nil)
  }

  def connect (connect_address:ConnectAddress, producer:BindableDeliveryProducer) = {
    if( mirrored ) {
      // this is a mirrored queue.. actually have the produce bind to the topic, instead of the
      val topic_address = new SimpleAddress("topic", binding.address.path)
      val topic = router.local_topic_domain.get_or_create_destination(topic_address, null).success
      topic.connect(topic_address, producer)
    } else {
      dispatch_queue {
        producers += producer
        check_idle
      }
      producer.bind(this::Nil, ()=>{})
    }
  }

  def disconnect (producer:BindableDeliveryProducer) = {
    if( mirrored ) {
      val topic_address = new SimpleAddress("topic", binding.address.path)
      val topic = router.local_topic_domain.get_or_create_destination(topic_address, null).success
      topic.disconnect(producer)
    } else {
      dispatch_queue {
        producers -= producer
        check_idle
      }
      producer.unbind(this::Nil)
    }
  }

  override def connection:Option[BrokerConnection] = None

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

}


