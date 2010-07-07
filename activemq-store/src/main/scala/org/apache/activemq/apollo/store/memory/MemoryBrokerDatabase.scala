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
package org.apache.activemq.apollo.store.memory


import _root_.java.lang.{String}
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import org.fusesource.hawtbuf._
import org.apache.activemq.util.TreeMap
import java.util.concurrent.atomic.{AtomicLong}
import collection.JavaConversions
import java.util.{ArrayList, HashSet}
import collection.mutable.HashMap
import org.apache.activemq.Service
import org.fusesource.hawtdispatch.{DispatchQueue, BaseRetained, Retained}
import org.apache.activemq.apollo.util.BaseService
import org.apache.activemq.broker.store._

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Counter(private var value:Int = 0) {

  def get() = value

  def incrementAndGet() = addAndGet(1)
  def decrementAndGet() = addAndGet(-1)
  def addAndGet(amount:Int) = {
    value+=amount
    value
  }

  def getAndIncrement() = getAndAdd(1)
  def getAndDecrement() = getAndAdd(-11)
  def getAndAdd(amount:Int) = {
    val rc = value
    value+=amount
    rc
  }

}


/**
 *  @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class MemoryBrokerDatabase() extends BaseService with BrokerDatabase {

  val dispatchQueue = createQueue("MessagesTable")
  def getDispatchQueue = dispatchQueue

  /////////////////////////////////////////////////////////////////////
  //
  // Methods related to Service interface impl
  //
  /////////////////////////////////////////////////////////////////////

  def _stop(onCompleted: Runnable) = {
    onCompleted.run
  }

  def _start(onCompleted: Runnable) = {
    onCompleted.run
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Methods related to queue management
  //
  /////////////////////////////////////////////////////////////////////
  private val queue_id_generator = new AtomicLong
  val queues = new TreeMap[Long, QueueData]

  case class QueueData(val record:StoredQueue) {
    var messges = new TreeMap[Long, Long]()
  }

  def listQueues(cb: (Seq[Long])=>Unit ) = reply(cb) {
    JavaConversions.asSet(queues.keySet).toSeq
  } >>: dispatchQueue

  def getQueueInfo(id:Long)(cb:(Option[StoredQueue])=>Unit ) = reply(cb) {
    val qd = queues.get(id)
    if( qd == null ) {
      None
    } else {
      val rc = qd.record
      if( qd.messges.isEmpty ) {
        rc.count = 0
        rc.first = -1
        rc.last = -1
      } else {
        rc.count = qd.messges.size
        rc.first = qd.messges.firstKey
        rc.last = qd.messges.lastKey
      }
      Some(rc)
    }
  } >>: dispatchQueue

  def addQueue(record:StoredQueue)(cb:(Option[Long])=>Unit):Unit = reply(cb) {
    val id = queue_id_generator.incrementAndGet
    if( queues.containsKey(id) ) {
      None
    } else {
      queues.put(id, QueueData(record))
      Some(id)
    }
  } >>: dispatchQueue

  /////////////////////////////////////////////////////////////////////
  //
  // Methods related to message storage
  //
  /////////////////////////////////////////////////////////////////////
  class MessageData(val delivery:StoredMessage) {
    val queueRefs = new Counter()
    var onFlush = List[()=>Unit]()
  }

  private val msg_id_generator = new AtomicLong
  val messages = new TreeMap[Long, MessageData]

  def flushMessage(msg:Long)(cb: =>Unit) = ^{
    val rc = messages.get(msg)
    if( rc == null ) {
      cb
    } else {
      rc.onFlush ::= cb _
    }
  } >>: dispatchQueue

  def loadMessage(ref:Long)(cb:(Option[StoredMessage])=>Unit ) = reply(cb) {
    val rc = messages.get(ref)
    if( rc == null ) {
      None
    } else {
      Some(rc.delivery)
    }
  } >>: dispatchQueue

  /////////////////////////////////////////////////////////////////////
  //
  // Methods related to store transactions
  //
  /////////////////////////////////////////////////////////////////////
  val transactions = new HashSet[MemoryStoreTransaction]()

  def createStoreTransaction() =  {
    val tx = new MemoryStoreTransaction()
    using(tx) {
      transactions.add(tx)
    } >>: dispatchQueue
    tx
  }

  class MemoryStoreTransaction extends BaseRetained with StoreTransaction  {

    val updated = HashMap[Long, MessageData]()

    def store(sm:StoredMessage) = {
      if( sm.id == -1 ) {
        sm.id = msg_id_generator.incrementAndGet
        using(this) {
          val md = new MessageData(sm)
          updated.put(sm.id, md)
          messages.put(sm.id, md)
        } >>: dispatchQueue
      }
    }

    def enqueue(queue:Long, seq:Long, msg:Long) = {
      using(this) {
        val qd = queues.get(queue)
        if( qd!=null ) {
          val md = updated.getOrElse(msg, messages.get(msg))
          md.queueRefs.incrementAndGet
          qd.messges.put(seq, msg)
        }
      } >>: dispatchQueue
    }

    def dequeue(queue:Long, seq:Long, msg:Long) = {
      using(this) {
        val qd = queues.get(queue)
        if( qd!=null ) {
          val md = updated.getOrElse(msg, messages.get(msg))
          md.queueRefs.decrementAndGet
          qd.messges.remove(seq)
        }
      } >>: dispatchQueue
    }


    override def dispose = {
      dispatchQueue {
        updated.foreach{ x=>
          if( x._2.queueRefs.get == 0 ) {
            messages.remove(x._1)
            x._2.onFlush.foreach( _() )
          }
        }
        transactions.remove(MemoryStoreTransaction.this)
        super.dispose
      }
    }
  }

}
