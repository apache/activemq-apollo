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
package org.apache.activemq.broker.store.cassandra

import org.apache.activemq.broker.store.{StoreBatch, Store}
import org.fusesource.hawtdispatch.BaseRetained
import com.shorrockin.cascal.session._
import java.util.concurrent.atomic.AtomicLong
import collection.mutable.ListBuffer
import java.util.HashMap
import org.apache.activemq.apollo.store.{QueueEntryRecord, MessageRecord, QueueStatus, QueueRecord}
import org.apache.activemq.apollo.broker.{Logging, Log, BaseService}
import org.apache.activemq.apollo.dto.{CassandraStoreDTO, StoreDTO}
import collection.{JavaConversions, Seq}
import org.apache.activemq.apollo.broker.{Reporting, ReporterLevel, Reporter}
import com.shorrockin.cascal.utils.Conversions._
import org.fusesource.hawtdispatch.ScalaDispatch._
import ReporterLevel._
import java.util.concurrent._
import org.apache.activemq.apollo.util.{TimeCounter, IntCounter}

object CassandraStore extends Log {

  /**
   * Creates a default a configuration object.
   */
  def defaultConfig() = {
    val rc = new CassandraStoreDTO
    rc.hosts.add("localhost:9160")
    rc
  }

  /**
   * Validates a configuration object.
   */
  def validate(config: CassandraStoreDTO, reporter:Reporter):ReporterLevel = {
    new Reporting(reporter) {
      if( config.hosts.isEmpty ) {
        error("At least one cassandra host must be configured.")
      }
    }.result
  }
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class CassandraStore extends Store with BaseService with Logging {

  import CassandraStore._
  override protected def log = CassandraStore

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the BaseService interface
  //
  /////////////////////////////////////////////////////////////////////
  val dispatchQueue = createQueue("cassandra store")

  var next_queue_key = new AtomicLong(1)
  var next_msg_key = new AtomicLong(1)

  val client = new CassandraClient()
  var config:CassandraStoreDTO = defaultConfig
  var blocking:ThreadPoolExecutor = null

  def configure(config: StoreDTO, reporter: Reporter) = configure(config.asInstanceOf[CassandraStoreDTO], reporter)

  def configure(config: CassandraStoreDTO, reporter: Reporter) = {
    if ( CassandraStore.validate(config, reporter) < ERROR ) {
      if( serviceState.isStarted ) {
        // TODO: apply changes while he broker is running.
        reporter.report(WARN, "Updating cassandra store configuration at runtime is not yet supported.  You must restart the broker for the change to take effect.")
      } else {
        this.config = config
      }
    }
  }


  protected def _start(onCompleted: Runnable) = {

    blocking = new ThreadPoolExecutor(4, 20, 1, TimeUnit.SECONDS, new LinkedBlockingQueue[Runnable](), new ThreadFactory(){
      def newThread(r: Runnable) = {
        val rc = new Thread(r, "cassandra client")
        rc.setDaemon(true)
        rc
      }
    })
    client.schema = Schema(config.keyspace)

    // TODO: move some of this parsing code into validation too.
    val HostPort = """([^:]+)(:(\d+))?""".r
    import JavaConversions._
    client.hosts = config.hosts.flatMap { x=>
      x match {
        case HostPort(host,_,port)=>
          Some(Host(host, port.toInt, 3000))
        case _=> None
      }
    }.toList

    client.start
    schedualDisplayStats
    onCompleted.run
  }

  protected def _stop(onCompleted: Runnable) = {
    blocking.shutdown
  }


  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the BrokerDatabase interface
  //
  /////////////////////////////////////////////////////////////////////
  val storeLatency = new TimeCounter
  def schedualDisplayStats:Unit = {
    def displayStats = {
      if( serviceState.isStarted ) {
        val cl = storeLatency.apply(true)
        info("metrics: store latency: %,.3f ms", cl.avgTime(TimeUnit.MILLISECONDS))
        schedualDisplayStats
      }
    }
    dispatchQueue.dispatchAfter(5, TimeUnit.SECONDS, ^{ displayStats })
  }

  /**
   * Deletes all stored data from the store.
   */
  def purge(callback: =>Unit) = {
    blocking {
      client.purge
      next_queue_key.set(1)
      next_msg_key.set(1)
      callback
    }
  }

  def addQueue(record: QueueRecord)(callback: (Option[Long]) => Unit) = {
    val key = next_queue_key.getAndIncrement
    record.key = key
    blocking {
      client.addQueue(record)
      callback(Some(key))
    }
  }

  def removeQueue(queueKey: Long)(callback: (Boolean) => Unit) = {
    blocking {
      callback(client.removeQueue(queueKey))
    }
  }

  def getQueueStatus(id: Long)(callback: (Option[QueueStatus]) => Unit) = {
    blocking {
      callback( client.getQueueStatus(id) )
    }
  }

  def listQueues(callback: (Seq[Long]) => Unit) = {
    blocking {
      callback( client.listQueues )
    }
  }

  def loadMessage(id: Long)(callback: (Option[MessageRecord]) => Unit) = {
    blocking {
      callback( client.loadMessage(id) )
    }
  }


  def listQueueEntries(id: Long)(callback: (Seq[QueueEntryRecord]) => Unit) = {
    blocking {
      callback( client.getQueueEntries(id) )
    }
  }

  def flushMessage(id: Long)(callback: => Unit) = ^{
    val action: CassandraBatch#MessageAction = pendingStores.get(id)
    if( action == null ) {
      callback
    } else {
      action.tx.eagerFlush(callback _)
      flush(action.tx.txid)
    }

  } >>: dispatchQueue

  def createStoreBatch() = new CassandraBatch


  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the StoreBatch interface
  //
  /////////////////////////////////////////////////////////////////////
  class CassandraBatch extends BaseRetained with StoreBatch {

    class MessageAction {

      var msg= 0L
      var store: MessageRecord = null
      var enqueues = ListBuffer[QueueEntryRecord]()
      var dequeues = ListBuffer[QueueEntryRecord]()

      def tx = CassandraBatch.this
      def isEmpty() = store==null && enqueues==Nil && dequeues==Nil
      def cancel() = {
        tx.rm(msg)
        if( tx.isEmpty ) {
          tx.cancel
        }
      }
    }

    val txid:Int = next_tx_id.getAndIncrement
    var actions = Map[Long, MessageAction]()

    var flushListeners = ListBuffer[Runnable]()
    def eagerFlush(callback: Runnable) = if( callback!=null ) { this.synchronized { flushListeners += callback } }

    def rm(msg:Long) = {
      actions -= msg
    }

    def isEmpty = actions.isEmpty
    def cancel = {
      delayedTransactions.remove(txid)
      onPerformed
    }

    def store(record: MessageRecord):Long = {
      record.key = next_msg_key.getAndIncrement
      val action = new MessageAction
      action.msg = record.key
      action.store = record
      this.synchronized {
        actions += record.key -> action
      }
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
      this.synchronized {
        action(entry.messageKey).enqueues += entry
      }
    }

    def dequeue(entry: QueueEntryRecord) = {
      this.synchronized {
        action(entry.messageKey).dequeues += entry
      }
    }

    override def dispose = {
      transaction_source.merge(this)
    }


    def onPerformed() {
      flushListeners.foreach { x=>
        x.run()
      }
      super.dispose
    }
  }

  def key(x:QueueEntryRecord) = (x.queueKey, x.queueSeq)

  val transaction_source = createSource(new ListEventAggregator[CassandraBatch](), dispatchQueue)
  transaction_source.setEventHandler(^{drain_transactions});
  transaction_source.resume

  var pendingStores = new HashMap[Long, CassandraBatch#MessageAction]()
  var pendingEnqueues = new HashMap[(Long,Long), CassandraBatch#MessageAction]()
  var delayedTransactions = new HashMap[Int, CassandraBatch]()

  var next_tx_id = new IntCounter(1)
  
  def drain_transactions = {
    transaction_source.getData.foreach { tx =>

      val tx_id = tx.txid
      delayedTransactions.put(tx_id, tx)

      tx.actions.foreach { case (msg, action) =>
        if( action.store!=null ) {
          pendingStores.put(msg, action)
        }
        action.enqueues.foreach { queueEntry=>
          pendingEnqueues.put(key(queueEntry), action)
        }


        // dequeues can cancel out previous enqueues
        action.dequeues.foreach { currentDequeue=>
          val currentKey = key(currentDequeue)
          val prevAction:CassandraBatch#MessageAction = pendingEnqueues.remove(currentKey)
          if( prevAction!=null ) {

            // yay we can cancel out a previous enqueue
            prevAction.enqueues = prevAction.enqueues.filterNot( x=> key(x) == currentKey )

            // if the message is not in any queues.. we can gc it..
            if( prevAction.enqueues == Nil && prevAction.store !=null ) {
              pendingStores.remove(msg)
              prevAction.store = null
            }

            // Cancel the action if it's now empty
            if( prevAction.isEmpty ) {
              prevAction.cancel()
            }

            // since we canceled out the previous enqueue.. now cancel out the action
            action.dequeues = action.dequeues.filterNot( _ == currentDequeue)
            if( action.isEmpty ) {
              action.cancel()
            }
          }
        }
      }

      if( !tx.flushListeners.isEmpty || config.flushDelay <= 0 ) {
        flush(tx_id)
      } else {
        dispatchQueue.dispatchAfter(config.flushDelay, TimeUnit.MILLISECONDS, ^{flush(tx_id)})
      }

    }
  }

  def flush(tx_id:Int) = {
    flush_source.merge(tx_id)
  }

  val flush_source = createSource(new ListEventAggregator[Int](), dispatchQueue)
  flush_source.setEventHandler(^{drain_flushes});
  flush_source.resume

  def drain_flushes:Unit = {

    if( !serviceState.isStarted ) {
      return
    }
    
    val txs = flush_source.getData.flatMap{ tx_id =>
      val tx = delayedTransactions.remove(tx_id)
      // Message may be flushed or canceled before the timeout flush event..
      // tx may be null in those cases
      if (tx!=null) {

        tx.actions.foreach { case (msg, action) =>
          if( action.store!=null ) {
            pendingStores.remove(msg)
          }
          action.enqueues.foreach { queueEntry=>
            val k = key(queueEntry)
            pendingEnqueues.remove(k)
          }
        }

        Some(tx)
      } else {
        None
      }
    }

    if( !txs.isEmpty ) {
      storeLatency.start { end =>
        blocking {
          client.store(txs)
          dispatchQueue {
            end()
            txs.foreach { x=>
              x.onPerformed
            }
          }
        }
      }
    }
  }

}
