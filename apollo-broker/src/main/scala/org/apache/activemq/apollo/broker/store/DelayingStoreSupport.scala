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
package org.apache.activemq.apollo.broker.store

import collection.mutable.ListBuffer
import java.util.HashMap
import collection.Seq
import org.fusesource.hawtdispatch._
import java.util.concurrent._
import atomic.AtomicInteger
import org.apache.activemq.apollo.util._
import org.fusesource.hawtdispatch.{BaseRetained, ListEventAggregator}
import org.apache.activemq.apollo.dto.{StoreStatusDTO, TimeMetricDTO, IntMetricDTO}
import org.fusesource.hawtbuf.Buffer

/**
 * <p>
 * Support class for implementing Stores which delay doing updates
 * so that it can support potentially be canceling the update due
 * to subsequent operation.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DelayingStoreSupport extends Store with BaseService {

  protected def flush_delay:Long

  protected def get_next_msg_key:Long

  protected def store(uows: Seq[DelayableUOW])(callback: =>Unit):Unit

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the BaseService interface
  //
  /////////////////////////////////////////////////////////////////////
  val dispatch_queue:DispatchQueue = createQueue(toString)
  val aggregator = new AggregatingExecutor(dispatch_queue)

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the StoreBatch interface
  //
  /////////////////////////////////////////////////////////////////////
  def create_uow() = new DelayableUOW

  class DelayableUOW extends BaseRetained with StoreUOW {

    class MessageAction {

      var msg= 0L
      var message_record: MessageRecord = null
      var enqueues = ListBuffer[QueueEntryRecord]()
      var dequeues = ListBuffer[QueueEntryRecord]()

      def uow = DelayableUOW.this
      def isEmpty() = message_record==null && enqueues==Nil && dequeues==Nil

      def cancel() = {
        uow.rm(msg)
      }
    }

    val uow_id:Int = next_batch_id.getAndIncrement
    var commit_ts:Long = 0

    // User might request the UOW to flush asap
    var flush_asap = false
    // Or to get canceled..
    var canceled = false

    // Perhaps track the 4 states below with a single enum?

    // UOW is delayed until we send it to get flushed.
    var delayed = true
    // Once completed it will be marked flushed. Flushed just
    // means the message has been written to disk
    // and out of memory
    var flushed = false
    // You have to wait for it to be completed
    // to know the write has been synced to disk.
    var completed = false

    var flush_listeners = ListBuffer[(Boolean)=>Unit]()
    var complete_listeners = ListBuffer[() => Unit]()

    var actions = Map[Long, MessageAction]()
    var map_actions = Map[Buffer, Buffer]()

    def put(key: Buffer, value: Buffer) = {
      map_actions += (key -> value)
    }

    def on_flush(callback: (Boolean)=>Unit) = {
      (this.synchronized {
        if( flushed ) {
          Some(canceled)
        } else {
          flush_listeners += callback
          None
        }
      }).foreach(callback(_))
    }

    def on_complete(callback: =>Unit) = {
      if( this.synchronized {
        if( completed ) {
          true
        } else {
          complete_listeners += ( ()=> callback  )
          false
        }
      }) {
        callback
      }
    }

    def complete_asap() = this.synchronized { flush_asap=true }

    var delayable_actions = 0

    def delayable = !flush_asap && delayable_actions>0 && flush_delay>=0

    def rm(msg:Long) = {
      actions -= msg
      if( actions.isEmpty && map_actions.isEmpty ) {
        cancel
      }
    }

    def cancel = {
      dispatch_queue.assertExecuting()
      canceled = true
      delayed_uows.remove(uow_id)
      on_completed
    }

    def store(record: MessageRecord):Long = {
      record.key = get_next_msg_key
      val action = new MessageAction
      action.msg = record.key
      action.message_record = record
      this.synchronized {
        actions += record.key -> action
      }
      aggregator {
        pending_stores.put(record.key, action)
      }
      delayable_actions += 1
      record.key
    }

    def action(msg:Long) = {
      actions.get(msg) match {
        case Some(x) => x
        case None =>
          val x = new MessageAction
          x.msg = msg
          actions += msg->x
          x
      }
    }

    def enqueue(entry: QueueEntryRecord) = {
      val a = this.synchronized {
        val a = action(entry.message_key)
        a.enqueues += entry
        delayable_actions += 1
        a
      }
      aggregator {
        cancelable_enqueue_actions.put(key(entry), a)
      }

    }

    def dequeue(entry: QueueEntryRecord) = {
      this.synchronized {
        action(entry.message_key).dequeues += entry
      }
    }

    override def dispose = {
      commit_ts = System.nanoTime
      uow_source.merge(this)
    }

    def on_flushed() = this.synchronized {
      if( !flushed ) {
        flushed = true
        flush_listeners.foreach(_(canceled))
      }
    }

    def on_completed() = this.synchronized {
      if ( !completed ) {
        on_flushed
        completed = true
        commit_latency_counter += System.nanoTime-commit_ts
        complete_listeners.foreach(_())
        super.dispose
      }
    }
  }

  def flush_message(message_key: Long)(cb: => Unit) = flush_message_source.merge((message_key, cb _))

  val flush_message_source = createSource(new ListEventAggregator[(Long, ()=>Unit)](), dispatch_queue)
  flush_message_source.setEventHandler(^{drain_flush_message});
  flush_message_source.resume
  
  def drain_flush_message:Unit = {
    flush_message_source.getData.foreach { case (message_key, cb) =>
      pending_stores.get(message_key) match {
        case null => cb()
        case action =>
          action.uow.on_complete( cb() )
          flush(action.uow)
      }
    }
  }

  implicit def toTimeMetricDTO( m: TimeMetric) = {
    val rc = new TimeMetricDTO()
    rc.count = m.count
    rc.max = m.max
    rc.min = m.min
    rc.total = m.total
    rc
  }

  implicit def toIntMetricDTO( m: IntMetric) = {
    val rc = new IntMetricDTO()
    rc.count = m.count
    rc.max = m.max
    rc.min = m.min
    rc.total = m.total
    rc
  }

  var metric_canceled_message_counter:Long = 0
  var metric_canceled_enqueue_counter:Long = 0
  var metric_flushed_message_counter:Long = 0
  var metric_flushed_enqueue_counter:Long = 0

  val commit_latency_counter = new TimeCounter
  var commit_latency = commit_latency_counter(false)

  val message_load_latency_counter = new TimeCounter
  var message_load_latency = message_load_latency_counter(false)

  val range_load_latency_counter = new TimeCounter
  var range_load_latency = message_load_latency_counter(false)

  val message_load_batch_size_counter = new IntMetricCounter
  var message_load_batch_size = message_load_batch_size_counter(false)

  var canceled_add_message:Long = 0
  var canceled_enqueue:Long = 0

  protected def fill_store_status(rc: StoreStatusDTO) {
    rc.id = this.toString
    rc.state = service_state.toString
    rc.state_since = service_state.since

    rc.flush_latency = flush_latency
    rc.message_load_latency = message_load_latency

    rc.canceled_message_counter = metric_canceled_message_counter
    rc.canceled_enqueue_counter = metric_canceled_enqueue_counter
    rc.flushed_message_counter = metric_flushed_message_counter
    rc.flushed_enqueue_counter = metric_flushed_enqueue_counter
    rc.pending_stores = pending_stores.size

//    import collection.JavaConversions._
//    var last = ""
//    var count = 0
//    pending_stores.valuesIterator.map(_.uow.status).foreach{ line =>
//      if( last!= "" && last!=line) {
//        println(last+" occured "+count+" times")
//        count = 0
//      }
//      count += 1
//      last = line
//    }
//    println(last+" occured "+count+" times")
//    println("--------------")
  }

  def key(x:QueueEntryRecord) = (x.queue_key, x.entry_seq)

  val uow_source = createSource(new ListEventAggregator[DelayableUOW](), dispatch_queue)
  uow_source.setEventHandler(^{drain_uows});
  uow_source.resume

  var pending_stores = new HashMap[Long, DelayableUOW#MessageAction]()
  var cancelable_enqueue_actions = new HashMap[(Long,Long), DelayableUOW#MessageAction]()
  var delayed_uows = new HashMap[Int, DelayableUOW]()

  val next_batch_id = new AtomicInteger(1)

  def drain_uows = {
    dispatch_queue.assertExecuting()
    uow_source.getData.foreach { uow =>

      delayed_uows.put(uow.uow_id, uow)

      uow.actions.foreach { case (msg, action) =>

        // dequeues can cancel out previous enqueues
        action.dequeues.foreach { currentDequeue=>
          val currentKey = key(currentDequeue)
          val prev_action:DelayableUOW#MessageAction = cancelable_enqueue_actions.remove(currentKey)

          def prev_uow = prev_action.uow

          if( prev_action!=null && !prev_uow.canceled ) {


            prev_uow.delayable_actions -= 1
            metric_canceled_enqueue_counter += 1

            // yay we can cancel out a previous enqueue
            prev_action.enqueues = prev_action.enqueues.filterNot( x=> key(x) == currentKey )

            // if the message is not in any queues.. we can gc it..
            if( prev_action.enqueues == Nil && prev_action.message_record !=null ) {
              pending_stores.remove(msg)
              prev_action.message_record = null
              prev_uow.delayable_actions -= 1
              metric_canceled_message_counter += 1
            }

            // Cancel the action if it's now empty
            if( prev_action.isEmpty ) {
              prev_action.cancel()
            } else if( !prev_uow.delayable ) {
              // flush it if there is no point in delyaing anymore
              flush(prev_uow)
            }

            // since we canceled out the previous enqueue.. now cancel out the action
            action.dequeues = action.dequeues.filterNot( _ == currentDequeue)
            if( action.isEmpty ) {
              action.cancel()
            }
          }
        }
      }

      if( !uow.canceled ) {
        if( uow.delayable ) {
          val uow_id = uow.uow_id
          dispatch_queue.executeAfter(flush_delay, TimeUnit.MILLISECONDS, ^{
            flush(delayed_uows.get(uow_id))
          })
        } else {
          flush(uow)
        }
      }
    }
  }

  private def flush(uow:DelayableUOW) = {
    if( uow!=null && uow.delayed && !uow.canceled ) {
      uow.delayed = false
      delayed_uows.remove(uow.uow_id)
      flush_source.merge(uow)
    }
  }

  val flush_source = createSource(new ListEventAggregator[DelayableUOW](), dispatch_queue)
  flush_source.setEventHandler(^{drain_flushes});
  flush_source.resume

  val flush_latency_counter = new TimeCounter
  var flush_latency = flush_latency_counter(false)

  def drain_flushes:Unit = {
    dispatch_queue.assertExecuting()

    if( !service_state.is_started ) {
      return
    }
    
    val uows = flush_source.getData

    var fasap = 0
    var fdelayed = 0
    
    // Some UOWs may have been canceled.
    uows.flatMap { uow=>
      if( uow.canceled ) {
        None
      } else {
        if( uow.flush_asap ) {
          fasap += 1
        } else {
          fdelayed +=1
        }
        // It will not be possible to cancel the UOW anymore..
        uow.actions.foreach { case (_, action) =>
          action.enqueues.foreach { queue_entry=>
            val action = cancelable_enqueue_actions.remove(key(queue_entry))
            assert(action!=null)
          }
        }
        Some(uow)
      }
    }
    if( !uows.isEmpty ) {
      flush_latency_counter.start { end=>
        flush_source.suspend
        store(uows) {
          store_completed(uows)
          flush_source.resume
          dispatch_queue.assertExecuting()
          uows.foreach { uow=>
            uow.actions.foreach { case (msg, action) =>
              if( action.message_record !=null ) {
                metric_flushed_message_counter += 1
                pending_stores.remove(msg)
              }
              action.enqueues.foreach { queue_entry=>
                metric_flushed_enqueue_counter += 1
              }
            }
          }
          end()
        }
      }
    }
  }

  def store_completed(uows: ListBuffer[DelayingStoreSupport.this.type#DelayableUOW]) = {
    uows.foreach { uow =>
        uow.on_completed
    }
  }


}
