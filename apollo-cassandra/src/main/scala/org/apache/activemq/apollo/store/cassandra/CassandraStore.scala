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
package org.apache.activemq.apollo.store.cassandra

import org.fusesource.hawtdispatch.BaseRetained
import com.shorrockin.cascal.session._
import java.util.concurrent.atomic.AtomicLong
import collection.mutable.ListBuffer
import java.util.HashMap
import collection.{JavaConversions, Seq}
import com.shorrockin.cascal.utils.Conversions._
import org.fusesource.hawtdispatch.ScalaDispatch._
import org.fusesource.hawtdispatch.ListEventAggregator
import java.util.concurrent._
import org.apache.activemq.apollo.dto._
import org.apache.activemq.apollo.store._
import org.apache.activemq.apollo.util._
import ReporterLevel._

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

  var next_msg_key = new AtomicLong(1)

  val client = new CassandraClient()
  var config:CassandraStoreDTO = defaultConfig
  var blocking:ExecutorService = null

  def configure(config: StoreDTO, reporter: Reporter):Unit = configure(config.asInstanceOf[CassandraStoreDTO], reporter)


  def storeStatusDTO(callback:(StoreStatusDTO)=>Unit) = dispatchQueue {
    val rc = new StoreStatusDTO
    rc.state = serviceState.toString
    rc.state_since = serviceState.since
    callback(rc)
  }

  def configure(config: CassandraStoreDTO, reporter: Reporter):Unit = {
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
    blocking = Executors.newFixedThreadPool(20, new ThreadFactory(){
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
    new Thread("casandra client shutdown") {
      override def run = {
        while( !blocking.awaitTermination(5, TimeUnit.SECONDS) ) {
          warn("cassandra thread pool is taking a long time to shutdown.")
        }
        client.stop
        onCompleted.run
      }
    }.start
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
      next_msg_key.set(1)
      callback
    }
  }

  /**
   * Ges the next queue key identifier.
   */
  def getLastQueueKey(callback:(Option[Long])=>Unit):Unit = {
    // TODO:
    callback( Some(1L) )
  }

  def addQueue(record: QueueRecord)(callback: (Boolean) => Unit) = {
    blocking {
      client.addQueue(record)
      callback(true)
    }
  }

  def removeQueue(queueKey: Long)(callback: (Boolean) => Unit) = {
    blocking {
      callback(client.removeQueue(queueKey))
    }
  }

  def getQueue(id: Long)(callback: (Option[QueueRecord]) => Unit) = {
    blocking {
      callback( client.getQueue(id) )
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


  def listQueueEntryRanges(queueKey: Long, limit: Int)(callback: (Seq[QueueEntryRange]) => Unit) = {
    blocking {
      callback( client.listQueueEntryGroups(queueKey, limit) )
    }
  }


  def listQueueEntries(queueKey: Long, firstSeq: Long, lastSeq: Long)(callback: (Seq[QueueEntryRecord]) => Unit) = {
    blocking {
      callback( client.getQueueEntries(queueKey, firstSeq, lastSeq) )
    }
  }

  def flushMessage(id: Long)(callback: => Unit) = ^{
    val action: CassandraUOW#MessageAction = pendingStores.get(id)
    if( action == null ) {
      callback
    } else {
      action.uow.onComplete(callback _)
      flush(action.uow.uow_id)
    }

  } >>: dispatchQueue

  def createStoreUOW() = new CassandraUOW


  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the StoreBatch interface
  //
  /////////////////////////////////////////////////////////////////////
  class CassandraUOW extends BaseRetained with StoreUOW {

    class MessageAction {

      var msg= 0L
      var store: MessageRecord = null
      var enqueues = ListBuffer[QueueEntryRecord]()
      var dequeues = ListBuffer[QueueEntryRecord]()

      def uow = CassandraUOW.this
      def isEmpty() = store==null && enqueues==Nil && dequeues==Nil
      def cancel() = {
        uow.rm(msg)
        if( uow.isEmpty ) {
          uow.cancel
        }
      }
    }

    val uow_id:Int = next_uow_id.getAndIncrement
    var actions = Map[Long, MessageAction]()
    var flushing= false

    var completeListeners = ListBuffer[Runnable]()

    def onComplete(callback: Runnable) = if( callback!=null ) { this.synchronized { completeListeners += callback } }

    var disableDelay = false
    def completeASAP() = this.synchronized { disableDelay=true }

    def delayable = !disableDelay


    def rm(msg:Long) = {
      actions -= msg
    }

    def isEmpty = actions.isEmpty
    def cancel = {
      delayedUOWs.remove(uow_id)
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
      dispatchQueue {
        pendingStores.put(record.key, action)
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
        val a = action(entry.messageKey)
        a.enqueues += entry
        dispatchQueue {
          pendingEnqueues.put(key(entry), a)
        }
      }
    }

    def dequeue(entry: QueueEntryRecord) = {
      this.synchronized {
        action(entry.messageKey).dequeues += entry
      }
    }

    override def dispose = {
      uow_source.merge(this)
    }


    def onPerformed() {
      completeListeners.foreach { x=>
        x.run()
      }
      super.dispose
    }
  }

  def key(x:QueueEntryRecord) = (x.queueKey, x.queueSeq)

  val uow_source = createSource(new ListEventAggregator[CassandraUOW](), dispatchQueue)
  uow_source.setEventHandler(^{drain_uows});
  uow_source.resume

  var pendingStores = new HashMap[Long, CassandraUOW#MessageAction]()
  var pendingEnqueues = new HashMap[(Long,Long), CassandraUOW#MessageAction]()
  var delayedUOWs = new HashMap[Int, CassandraUOW]()

  var next_uow_id = new IntCounter(1)
  
  def drain_uows = {
    uow_source.getData.foreach { uow =>

      val uow_id = uow.uow_id
      delayedUOWs.put(uow_id, uow)

      uow.actions.foreach { case (msg, action) =>

        // dequeues can cancel out previous enqueues
        action.dequeues.foreach { currentDequeue=>
          val currentKey = key(currentDequeue)
          val prevAction:CassandraUOW#MessageAction = pendingEnqueues.remove(currentKey)
          if( prevAction!=null && !prevAction.uow.flushing ) {

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

      if( !uow.completeListeners.isEmpty || config.flush_delay <= 0 ) {
        flush(uow_id)
      } else {
        dispatchQueue.dispatchAfter(config.flush_delay, TimeUnit.MILLISECONDS, ^{flush(uow_id)})
      }

    }
  }

  def flush(uow_id:Int) = {
    flush_source.merge(uow_id)
  }

  val flush_source = createSource(new ListEventAggregator[Int](), dispatchQueue)
  flush_source.setEventHandler(^{drain_flushes});
  flush_source.resume

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
      storeLatency.start { end =>
        blocking {
          client.store(uows)
          dispatchQueue {
            end()
            uows.foreach { uow=>

              uow.actions.foreach { case (msg, action) =>
                if( action.store!=null ) {
                  pendingStores.remove(msg)
                }
                action.enqueues.foreach { queueEntry=>
                  val k = key(queueEntry)
                  pendingEnqueues.remove(k)
                }
              }

              uow.onPerformed
            }
          }
        }
      }
    }
  }

}
