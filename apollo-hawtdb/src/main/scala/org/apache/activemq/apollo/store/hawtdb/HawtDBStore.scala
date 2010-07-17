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
package org.apache.activemq.apollo.store.hawtdb

import org.fusesource.hawtdispatch.BaseRetained
import java.util.concurrent.atomic.AtomicLong
import collection.mutable.ListBuffer
import java.util.HashMap
import collection.{Seq}
import org.fusesource.hawtdispatch.ScalaDispatch._
import java.io.File
import java.util.concurrent._
import org.apache.activemq.apollo.dto._
import org.apache.activemq.apollo.store._
import org.apache.activemq.apollo.util._
import ReporterLevel._

object HawtDBStore extends Log {
  val DATABASE_LOCKED_WAIT_DELAY = 10 * 1000;

  /**
   * Creates a default a configuration object.
   */
  def defaultConfig() = {
    val rc = new HawtDBStoreDTO
    rc.directory = new File("activemq-data")
    rc
  }

  /**
   * Validates a configuration object.
   */
  def validate(config: HawtDBStoreDTO, reporter:Reporter):ReporterLevel = {
    new Reporting(reporter) {
      if( config.directory==null ) {
        error("The HawtDB Store directory property must be configured.")
      }
    }.result
  }
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class HawtDBStore extends Store with BaseService with DispatchLogging {

  import HawtDBStore._
  override protected def log = HawtDBStore

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the BaseService interface
  //
  /////////////////////////////////////////////////////////////////////
  val dispatchQueue = createQueue("hawtdb store")

  var next_queue_key = new AtomicLong(1)
  var next_msg_key = new AtomicLong(1)

  var executor_pool:ExecutorService = _
  var config:HawtDBStoreDTO = defaultConfig
  val client = new HawtDBClient(this)

  def configure(config: StoreDTO, reporter: Reporter) = configure(config.asInstanceOf[HawtDBStoreDTO], reporter)

  def configure(config: HawtDBStoreDTO, reporter: Reporter) = {
    if ( HawtDBStore.validate(config, reporter) < ERROR ) {
      if( serviceState.isStarted ) {
        // TODO: apply changes while he broker is running.
        reporter.report(WARN, "Updating cassandra store configuration at runtime is not yet supported.  You must restart the broker for the change to take effect.")
      } else {
        this.config = config
      }
    }
  }

  protected def _start(onCompleted: Runnable) = {
    executor_pool = Executors.newFixedThreadPool(1, new ThreadFactory(){
      def newThread(r: Runnable) = {
        val rc = new Thread(r, "hawtdb store client")
        rc.setDaemon(true)
        rc
      }
    })
    client.config = config
    poll_stats
    executor_pool {
      client.start(^{
        next_msg_key.set( client.rootBuffer.getLastMessageKey.longValue +1 )
        next_queue_key.set( client.rootBuffer.getLastQueueKey.longValue +1 )
        onCompleted.run
      })
    }
  }

  protected def _stop(onCompleted: Runnable) = {
    new Thread() {
      override def run = {
        executor_pool.shutdown
        executor_pool.awaitTermination(1, TimeUnit.DAYS)
        executor_pool = null
        client.stop
        onCompleted.run
      }
    }.start
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the Store interface
  //
  /////////////////////////////////////////////////////////////////////

  /**
   * Deletes all stored data from the store.
   */
  def purge(callback: =>Unit) = {
    executor_pool {
      client.purge(^{
        next_queue_key.set(1)
        next_msg_key.set(1)
        callback
      })
    }
  }


  /**
   * Ges the last queue key identifier stored.
   */
  def getLastQueueKey(callback:(Option[Long])=>Unit):Unit = {
    executor_pool {
      callback(Some(client.rootBuffer.getLastQueueKey.longValue))
    }
  }

  def addQueue(record: QueueRecord)(callback: (Boolean) => Unit) = {
    executor_pool {
     client.addQueue(record, ^{ callback(true) })
    }
  }

  def removeQueue(queueKey: Long)(callback: (Boolean) => Unit) = {
    executor_pool {
      client.removeQueue(queueKey,^{ callback(true) })
    }
  }

  def getQueue(queueKey: Long)(callback: (Option[QueueRecord]) => Unit) = {
    executor_pool {
      callback( client.getQueue(queueKey) )
    }
  }

  def listQueues(callback: (Seq[Long]) => Unit) = {
    executor_pool {
      callback( client.listQueues )
    }
  }

  

  val load_source = createSource(new ListEventAggregator[(Long, (Option[MessageRecord])=>Unit)](), dispatchQueue)
  load_source.setEventHandler(^{drain_loads});
  load_source.resume


  def loadMessage(messageKey: Long)(callback: (Option[MessageRecord]) => Unit) = {
    message_load_latency_counter.start { end=>
      load_source.merge((messageKey, { (result)=>
        end()
        callback(result)
      }))
    }
  }

  def drain_loads = {
    var data = load_source.getData
    message_load_batch_size_counter += data.size
    executor_pool ^{
      client.loadMessages(data)
    }
  }

  def listQueueEntryRanges(queueKey: Long, limit: Int)(callback: (Seq[QueueEntryRange]) => Unit) = {
    executor_pool ^{
      callback( client.listQueueEntryGroups(queueKey, limit) )
    }
  }

  def listQueueEntries(queueKey: Long, firstSeq: Long, lastSeq: Long)(callback: (Seq[QueueEntryRecord]) => Unit) = {
    executor_pool ^{
      callback( client.getQueueEntries(queueKey, firstSeq, lastSeq) )
    }
  }

  def flushMessage(messageKey: Long)(cb: => Unit) = dispatchQueue {
    val action: HawtDBUOW#MessageAction = pendingStores.get(messageKey)
    if( action == null ) {
      cb
    } else {
      action.uow.onComplete(^{ cb })
      flush(action.uow.uow_id)
    }
  }

  def createStoreUOW() = new HawtDBUOW


  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the StoreBatch interface
  //
  /////////////////////////////////////////////////////////////////////
  class HawtDBUOW extends BaseRetained with StoreUOW {

    var dispose_start:Long = 0
    var flushing = false;

    class MessageAction {

      var msg= 0L
      var messageRecord: MessageRecord = null
      var enqueues = ListBuffer[QueueEntryRecord]()
      var dequeues = ListBuffer[QueueEntryRecord]()

      def uow = HawtDBUOW.this
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

    def delayable = !disableDelay && delayable_actions>0 && config.flush_delay>=0

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
      record.key = next_msg_key.getAndIncrement
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

  val uow_source = createSource(new ListEventAggregator[HawtDBUOW](), dispatchQueue)
  uow_source.setEventHandler(^{drain_uows});
  uow_source.resume

  var pendingStores = new HashMap[Long, HawtDBUOW#MessageAction]()
  var pending_enqueues = new HashMap[(Long,Long), HawtDBUOW#MessageAction]()
  var delayedUOWs = new HashMap[Int, HawtDBUOW]()

  var next_batch_id = new IntCounter(1)

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

  def storeStatusDTO(callback:(StoreStatusDTO)=>Unit) = dispatchQueue {
    val rc = new HawtDBStoreStatusDTO

    rc.state = serviceState.toString
    rc.state_since = serviceState.since

    rc.flush_latency = flush_latency
    rc.message_load_latency = message_load_latency
    rc.message_load_batch_size = message_load_batch_size

    rc.journal_append_latency = client.metric_journal_append
    rc.index_update_latency = client.metric_index_update

    rc.canceled_message_counter = metric_canceled_message_counter
    rc.canceled_enqueue_counter = metric_canceled_enqueue_counter
    rc.flushed_message_counter = metric_flushed_message_counter
    rc.flushed_enqueue_counter = metric_flushed_enqueue_counter

    callback(rc)
  }


  def poll_stats:Unit = {
    def displayStats = {
      if( serviceState.isStarted ) {

        flush_latency = flush_latency_counter(true)
        message_load_latency = message_load_latency_counter(true)
        client.metric_journal_append = client.metric_journal_append_counter(true)
        client.metric_index_update = client.metric_index_update_counter(true)
        commit_latency = commit_latency_counter(true)
        message_load_batch_size =  message_load_batch_size_counter(true)

        poll_stats
      }
    }
    
    dispatchQueue.dispatchAfter(1, TimeUnit.SECONDS, ^{ displayStats })
  }

  def drain_uows = {
    uow_source.getData.foreach { uow =>

      delayedUOWs.put(uow.uow_id, uow)

      uow.actions.foreach { case (msg, action) =>

        // dequeues can cancel out previous enqueues
        action.dequeues.foreach { currentDequeue=>
          val currentKey = key(currentDequeue)
          val prev_action:HawtDBUOW#MessageAction = pending_enqueues.remove(currentKey)

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
        dispatchQueue.dispatchAfter(config.flush_delay, TimeUnit.MILLISECONDS, ^{flush(batch_id)})
      } else {
        flush(batch_id)
      }

    }
  }

  def flush(batch_id:Int) = {
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
        executor_pool {
          client.store(uows, ^{
            dispatchQueue {

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
          })
        }
      }
    }
  }

}
