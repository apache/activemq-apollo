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
package org.apache.activemq.apollo.store

import collection.mutable.ListBuffer
import java.util.HashMap
import collection.Seq
import org.fusesource.hawtdispatch.ScalaDispatch._
import java.util.concurrent._
import org.apache.activemq.apollo.util._
import org.fusesource.hawtdispatch.{BaseRetained, ListEventAggregator}

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
  val dispatchQueue = createQueue(toString)

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the StoreBatch interface
  //
  /////////////////////////////////////////////////////////////////////
  def createStoreUOW() = new DelayableUOW

  class DelayableUOW extends BaseRetained with StoreUOW {

    var dispose_start:Long = 0
    var flushing = false;

    class MessageAction {

      var msg= 0L
      var messageRecord: MessageRecord = null
      var enqueues = ListBuffer[QueueEntryRecord]()
      var dequeues = ListBuffer[QueueEntryRecord]()

      def uow = DelayableUOW.this
      def isEmpty() = messageRecord==null && enqueues==Nil && dequeues==Nil

      def cancel() = {
        uow.rm(msg)
      }
    }

    val uow_id:Int = next_batch_id.getAndIncrement
    var actions = Map[Long, MessageAction]()

    var completeListeners = ListBuffer[Runnable]()
    var disableDelay = false

    def onComplete(callback: Runnable) = if( callback!=null ) { this.synchronized { completeListeners += callback } }

    def completeASAP() = this.synchronized { disableDelay=true }

    var delayable_actions = 0

    def delayable = !disableDelay && delayable_actions>0 && flush_delay>=0

    def rm(msg:Long) = {
      actions -= msg
      if( actions.isEmpty ) {
        cancel
      }
    }

    def cancel = {
      delayedUOWs.remove(uow_id)
      onPerformed
    }

    def store(record: MessageRecord):Long = {
      record.key = get_next_msg_key
      val action = new MessageAction
      action.msg = record.key
      action.messageRecord = record
      this.synchronized {
        actions += record.key -> action
      }
      dispatchQueue {
        pendingStores.put(record.key, action)
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
        val a = action(entry.messageKey)
        a.enqueues += entry
        delayable_actions += 1
        a
      }
      dispatchQueue {
        pending_enqueues.put(key(entry), a)
      }

    }

    def dequeue(entry: QueueEntryRecord) = {
      this.synchronized {
        action(entry.messageKey).dequeues += entry
      }
    }

    override def dispose = {
      dispose_start = System.nanoTime
      uow_source.merge(this)
    }

    def onPerformed() = this.synchronized {
      commit_latency_counter += System.nanoTime-dispose_start
      completeListeners.foreach { x=>
        x.run
      }
      super.dispose
    }
  }


  def flushMessage(messageKey: Long)(cb: => Unit) = dispatchQueue {
    val action: DelayableUOW#MessageAction = pendingStores.get(messageKey)
    if( action == null ) {
      cb
    } else {
      action.uow.onComplete(^{ cb })
      flush(action.uow.uow_id)
    }
  }


  var metric_canceled_message_counter:Long = 0
  var metric_canceled_enqueue_counter:Long = 0
  var metric_flushed_message_counter:Long = 0
  var metric_flushed_enqueue_counter:Long = 0

  val commit_latency_counter = new TimeCounter
  var commit_latency = commit_latency_counter(false)

  val message_load_latency_counter = new TimeCounter
  var message_load_latency = message_load_latency_counter(false)

  val message_load_batch_size_counter = new IntMetricCounter
  var message_load_batch_size = message_load_batch_size_counter(false)

  var canceled_add_message:Long = 0
  var canceled_enqueue:Long = 0


  def key(x:QueueEntryRecord) = (x.queueKey, x.queueSeq)

  val uow_source = createSource(new ListEventAggregator[DelayableUOW](), dispatchQueue)
  uow_source.setEventHandler(^{drain_uows});
  uow_source.resume

  var pendingStores = new HashMap[Long, DelayableUOW#MessageAction]()
  var pending_enqueues = new HashMap[(Long,Long), DelayableUOW#MessageAction]()
  var delayedUOWs = new HashMap[Int, DelayableUOW]()

  var next_batch_id = new IntCounter(1)

  def drain_uows = {
    uow_source.getData.foreach { uow =>

      delayedUOWs.put(uow.uow_id, uow)

      uow.actions.foreach { case (msg, action) =>

        // dequeues can cancel out previous enqueues
        action.dequeues.foreach { currentDequeue=>
          val currentKey = key(currentDequeue)
          val prev_action:DelayableUOW#MessageAction = pending_enqueues.remove(currentKey)

          def prev_batch = prev_action.uow

          if( prev_action!=null && !prev_batch.flushing ) {


            prev_batch.delayable_actions -= 1
            metric_canceled_enqueue_counter += 1

            // yay we can cancel out a previous enqueue
            prev_action.enqueues = prev_action.enqueues.filterNot( x=> key(x) == currentKey )

            // if the message is not in any queues.. we can gc it..
            if( prev_action.enqueues == Nil && prev_action.messageRecord !=null ) {
              pendingStores.remove(msg)
              prev_action.messageRecord = null
              prev_batch.delayable_actions -= 1
              metric_canceled_message_counter += 1
            }

            // Cancel the action if it's now empty
            if( prev_action.isEmpty ) {
              prev_action.cancel()
            } else if( !prev_batch.delayable ) {
              // flush it if there is no point in delyaing anymore
              flush(prev_batch.uow_id)
            }

            // since we canceled out the previous enqueue.. now cancel out the action
            action.dequeues = action.dequeues.filterNot( _ == currentDequeue)
            if( action.isEmpty ) {
              action.cancel()
            }
          }
        }
      }

      val batch_id = uow.uow_id
      if( uow.delayable ) {
        dispatchQueue.dispatchAfter(flush_delay, TimeUnit.MILLISECONDS, ^{flush(batch_id)})
      } else {
        flush(batch_id)
      }

    }
  }

  private def flush(batch_id:Int) = {
    flush_source.merge(batch_id)
  }

  val flush_source = createSource(new ListEventAggregator[Int](), dispatchQueue)
  flush_source.setEventHandler(^{drain_flushes});
  flush_source.resume

  val flush_latency_counter = new TimeCounter
  var flush_latency = flush_latency_counter(false)

  def drain_flushes:Unit = {

    if( !serviceState.isStarted ) {
      return
    }

    val uows = flush_source.getData.flatMap{ uow_id =>

      val uow = delayedUOWs.remove(uow_id)
      // Message may be flushed or canceled before the timeout flush event..
      // uow may be null in those cases
      if (uow!=null) {
        uow.flushing = true
        Some(uow)
      } else {
        None
      }
    }

    if( !uows.isEmpty ) {
      flush_latency_counter.start { end=>
        store(uows) {
          end()
          uows.foreach { uow=>

            uow.actions.foreach { case (msg, action) =>
              if( action.messageRecord !=null ) {
                metric_flushed_message_counter += 1
                pendingStores.remove(msg)
              }
              action.enqueues.foreach { queueEntry=>
                metric_flushed_enqueue_counter += 1
                val k = key(queueEntry)
                pending_enqueues.remove(k)
              }
            }
            uow.onPerformed

          }
        }
      }
    }
  }

}
