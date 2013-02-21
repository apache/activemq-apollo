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
import collection.Seq
import org.fusesource.hawtdispatch._
import java.util.concurrent._
import atomic.AtomicInteger
import org.apache.activemq.apollo.util._
import org.fusesource.hawtdispatch.{BaseRetained, ListEventAggregator}
import org.apache.activemq.apollo.dto.{StoreStatusDTO, TimeMetricDTO, IntMetricDTO}
import org.fusesource.hawtbuf.Buffer
import java.lang.ref.WeakReference
import language.implicitConversions
import java.io.{PrintWriter, StringWriter}

object DelayingStoreSupport extends Log

sealed trait UowState {
  def stage:Int
}
// UoW is initial open.
object UowOpen extends UowState {
  override def stage = 0
  override def toString = "UowOpen"
}
// UoW is Committed once the broker finished creating it.
object UowClosed extends UowState {
  override def stage = 1
  override def toString = "UowClosed"
}
// UOW is delayed until we send it to get flushed.
object UowDelayed extends UowState {
  override def stage = 2
  override def toString = "UowDelayed"
}
object UowFlushQueued extends UowState {
  override def stage = 3
  override def toString = "UowFlushQueued"
}

object UowFlushing extends UowState {
  override def stage = 4
  override def toString = "UowFlushing"
}
// Then it moves on to be flushed. Flushed just
// means the message has been written to disk
// and out of memory
object UowFlushed extends UowState {
  override def stage = 5
  override def toString = "UowFlushed"
}

// Once completed then you know it has been synced to disk.
object UowCompleted extends UowState {
  override def stage = 6
  override def toString = "UowCompleted"
}

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

  import DelayingStoreSupport._

  protected def flush_delay:Long

  protected def get_next_msg_key:Long

  protected def store(uows: Seq[DelayableUOW])(callback: =>Unit):Unit

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the BaseService interface
  //
  /////////////////////////////////////////////////////////////////////
  val dispatch_queue:DispatchQueue = createQueue(toString)
  
  val event_source = createSource(new ListEventAggregator[Runnable](), dispatch_queue)
  event_source.setEventHandler(^{ event_source.getData.foreach(_.run()) });
  event_source.resume
  

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the StoreBatch interface
  //
  /////////////////////////////////////////////////////////////////////
  def create_uow = new DelayableUOW

  class DelayableUOW extends BaseRetained with StoreUOW {

    override def toString: String = uow_id.toString

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
    var close_ts:Long = 0

    // User might request the UOW to flush asap
    var flush_asap = false
    // Or perhaps the user needs a disk sync too.
    var flush_sync = false
    // Or to get canceled..
    var canceled = false

    private var _state:UowState = UowOpen
    
    def state = this._state
    def state_=(next:UowState) {
      assert(this._state.stage < next.stage)
      this._state = next
    }

    private var complete_listeners = ListBuffer[(Boolean) => Unit]()

    var actions = Map[Long, MessageAction]()
    var map_actions = Map[Buffer, Buffer]()


    def put(key: Buffer, value: Buffer) = {
      map_actions += (key -> value)
    }

    def on_flush(callback: (Boolean)=>Unit) = {
      (this.synchronized {
        if( state.stage >= UowFlushed.stage ) {
          Some(canceled)
        } else {
          complete_listeners += callback
          None
        }
      }).foreach(callback(_))
    }


    def on_complete(callback: (Boolean)=>Unit) = {
      var (completed, value) = this.synchronized {
        if (state eq UowCompleted) {
          (true, canceled)
        } else {
          flush_sync = true
          complete_listeners += callback
          (false, canceled)
        }
      }
      if(completed) {
        callback(value)
      }
    }
    
    def complete_asap = this.synchronized {
      flush_asap=true
      if( state eq UowDelayed ) {
        queue_flush(this)
        false
      } else if( state eq UowCompleted ) {
        true
      } else {
        false
      }
    }

    var delayable_actions = 0

    def delayable = !flush_asap && delayable_actions>0 && flush_delay>=0

    def rm(msg:Long) = {
      actions -= msg
      if( actions.isEmpty && map_actions.isEmpty && state.stage < UowFlushing.stage ) {
        cancel
      }
    }


    private def cancel = {
      dispatch_queue.assertExecuting()
      canceled = true
      on_completed
    }

    def store(record: MessageRecord):Long = {
      record.key = get_next_msg_key
      val action = new MessageAction
      action.msg = record.key
      action.message_record = record
      on_store_requested(record)
      this.synchronized {
        actions += record.key -> action
        pending_stores.put(action.message_record.key, action)
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
      assert( !locator_based || entry.message_locator!=null )
      val a = this.synchronized {
        val a = action(entry.message_key)
        a.enqueues += entry
        delayable_actions += 1
        cancelable_enqueue_actions.put(key(entry), a)
      }
    }

    def dequeue(entry: QueueEntryRecord) = {
      this.synchronized {
        action(entry.message_key).dequeues += entry
      }
    }
    
    def have_locators:Boolean = {
      actions.values.foreach{ a =>
        // There must either be a dequeue or a message record for a enqueue request.
        // if not, then there should be a message locator
        if( a.message_record==null ) {
          if(!a.dequeues.isEmpty ){
            a.dequeues.foreach { d =>
              if ( d.message_locator.get() == null ) {
                return false
              }
            }
          }
          else if (!a.enqueues.isEmpty){
            a.enqueues.foreach { e =>
              if ( e.message_locator.get() == null ) {
                return false
              }
            }
          }
        }
      }
      true
    }

    override def dispose = this.synchronized {
      state = UowClosed
      close_ts = System.nanoTime
      uow_source.merge(this)
    }

    def on_completed = this.synchronized {
      if ( state.stage < UowCompleted.stage ) {
        state = UowCompleted
        close_latency_counter += System.nanoTime-close_ts
        complete_listeners.foreach(_(canceled))
        super.dispose
      }
    }
  }

  protected def locator_based = false
  
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

  def on_store_requested(mr:MessageRecord) = {}

  var metric_canceled_message_counter:Long = 0
  var metric_canceled_enqueue_counter:Long = 0
  var metric_flushed_message_counter:Long = 0
  var metric_flushed_enqueue_counter:Long = 0

  val close_latency_counter = new TimeCounter
  var close_latency = close_latency_counter(false)

  val message_load_latency_counter = new TimeCounter
  var message_load_latency = message_load_latency_counter(false)

  val range_load_latency_counter = new TimeCounter
  var range_load_latency = message_load_latency_counter(false)

  val message_load_batch_size_counter = new IntMetricCounter
  var message_load_batch_size = message_load_batch_size_counter(false)

  var canceled_add_message:Long = 0
  var canceled_enqueue:Long = 0

  protected def fill_store_status(rc: StoreStatusDTO) {
    rc.kind = this.kind
    rc.location = this.location
    rc.state = service_state.toString
    rc.state_since = service_state.since

    rc.flush_latency = flush_latency
    rc.message_load_latency = message_load_latency

    rc.canceled_message_counter = metric_canceled_message_counter
    rc.canceled_enqueue_counter = metric_canceled_enqueue_counter
    rc.flushed_message_counter = metric_flushed_message_counter
    rc.flushed_enqueue_counter = metric_flushed_enqueue_counter
    rc.pending_stores = pending_stores.size
  }

  def detailed_pending_status = {
    import collection.JavaConversions._

    val writer = new StringWriter();
    val out = new PrintWriter(writer);

    out.println("--- Pending Stores Details ---")
    out.println("flush_source suspended: "+flush_source.isSuspended)
    pending_stores.valuesIterator.foreach{ action =>
      out.println("uow: %d, state:%s".format(action.uow.uow_id, action.uow.state))
    }
    writer.toString
  }

  def key(x:QueueEntryRecord) = (x.queue_key, x.entry_seq)

  val uow_source = createSource(new ListEventAggregator[DelayableUOW](), dispatch_queue)
  uow_source.setEventHandler(^{drain_uows});
  uow_source.resume

  val pending_stores = new ConcurrentHashMap[Long, DelayableUOW#MessageAction]()
  var cancelable_enqueue_actions = new ConcurrentHashMap[(Long,Long), DelayableUOW#MessageAction]()

  val next_batch_id = new AtomicInteger(1)

  def drain_uows = {
    dispatch_queue.assertExecuting()
    uow_source.getData.foreach { uow =>

      // Broker could issue a flush_message call before
      // this stage runs.. which make the stage jump over UowDelayed
      if( uow.state.stage < UowDelayed.stage ) {
        uow.state = UowDelayed
      }


      if( uow.state.stage < UowFlushing.stage ) {
        uow.actions.foreach { case (msg, action) =>

          // The UoW may have been canceled.
          if( action.message_record!=null && action.enqueues.isEmpty ) {
            pending_stores.remove(msg)
            action.message_record = null
            uow.delayable_actions -= 1
            metric_canceled_message_counter += 1
          }
          if( action.isEmpty ) {
            action.cancel()
          }

          // dequeues can cancel out previous enqueues
          action.dequeues.foreach { entry=>

            val entry_key = key(entry)
            val prev_action:DelayableUOW#MessageAction = cancelable_enqueue_actions.remove(entry_key)

            if( prev_action!=null ) {
              val prev_uow = prev_action.uow

              prev_uow.synchronized {
                if( !prev_uow.canceled ) {

                  prev_uow.delayable_actions -= 1
                  metric_canceled_enqueue_counter += 1

                  // yay we can cancel out a previous enqueue
                  prev_action.enqueues = prev_action.enqueues.filterNot( x=> key(x) == entry_key )

                  if( prev_uow.state.stage >= UowDelayed.stage ) {
                    // if the message is not in any queues.. we can gc it..
                    if( prev_action.enqueues.isEmpty && prev_action.message_record !=null ) {
                      pending_stores.remove(msg)
                      prev_action.message_record = null
                      prev_uow.delayable_actions -= 1
                      metric_canceled_message_counter += 1
                    }

                    // Cancel the action if it's now empty
                    if( prev_action.isEmpty ) {
                      prev_action.cancel()
                    } else if( !prev_uow.delayable ) {
                      // flush it if there is no point in delaying anymore
                      prev_uow.complete_asap()
                    }
                  }

                }
              }

              // since we canceled out the previous enqueue.. now cancel out the action
              action.dequeues = action.dequeues.filterNot( _ == entry)
              if( action.isEmpty ) {
                action.cancel()
              }

            }
          }
        }
      }

      if( !uow.canceled && uow.state.stage < UowFlushQueued.stage ) {
        if( uow.delayable ) {
          // Let the uow get GCed if its' canceled during the delay window..
          val ref = new WeakReference[DelayableUOW](uow)
          schedule_flush(ref)
        } else {
          queue_flush(uow)
        }
      }
    }
  }

  def flush_message(message_key: Long)(cb: => Unit) = event_source.merge(^{
    pending_stores.get(message_key) match {
      case null => cb
      case action =>
        val uow = action.uow
        uow.on_flush( (canceled)=>{ cb } )
        uow.complete_asap
    }
  })

  private def schedule_flush(ref: WeakReference[DelayableUOW]) {
    dispatch_queue.executeAfter(flush_delay, TimeUnit.MILLISECONDS, ^ {
      val uow = ref.get();
      if (uow != null) {
        queue_flush(uow)
      }
    })
  }

  private def queue_flush(uow:DelayableUOW) = {
    if( uow!=null && !uow.canceled && uow.state.stage < UowFlushQueued.stage ) {
      uow.state = UowFlushQueued
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
    
    // Some UOWs may have been canceled.
    val uows = flush_source.getData.flatMap { uow=>
      if( uow.canceled ) {
        None
      } else {
        uow.state = UowFlushing
        assert( !locator_based || uow.have_locators )
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
          assert_executing
          flush_source.resume
          store_completed(uows)
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
