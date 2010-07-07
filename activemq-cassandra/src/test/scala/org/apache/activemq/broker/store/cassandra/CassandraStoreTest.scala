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

import org.fusesource.hawtbuf.AsciiBuffer._
import org.scalatest.BeforeAndAfterAll
import org.fusesource.hawtdispatch.ScalaDispatch._
import org.fusesource.hawtdispatch.TaskTracker
import org.apache.activemq.apollo.broker.{LoggingTracker, FunSuiteSupport}
import java.util.concurrent.{TimeUnit, CountDownLatch}
import org.apache.activemq.apollo.store.{QueueEntryRecord, QueueStatus, QueueRecord, MessageRecord}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class CassandraStoreTest extends FunSuiteSupport with CassandraServerMixin {

  def CB[T](func: (T=>Unit)=>Unit ) = {
    class X {
      var value:T = _
    }
    val rc = new X
    val cd = new CountDownLatch(1)
    def cb(x:T) = {
      rc.value = x
      cd.countDown
    }
    func(cb)
    cd.await
    rc.value
  }

  var store:CassandraStore=null

  override protected def beforeAll() = {
    store = new CassandraStore()
    val tracker = new LoggingTracker("store startup")
    tracker.start(store)
    tracker.await
  }

  override protected def afterAll() = {
    val tracker = new LoggingTracker("store stop")
    tracker.stop(store)
    tracker.await
  }


  def expectCB[T](expected:T)(func: (T=>Unit)=>Unit ) = {
    expect(expected) {
      CB(func)
    }
  }


  test("add message") {
   addMessage
  }

  def addMessage() {
    var queueA = new QueueRecord
    queueA.key =1
    queueA.name = ascii("queue:1")

    val rc:Option[Long] = CB( cb=> store.addQueue(queueA)(cb) )
    queueA.key = rc.get

    val expected:Seq[Long] = List(queueA.key)
    expectCB(expected) { cb=>
      store.listQueues(cb)
    }

    var tx = store.createStoreBatch
    var message = new MessageRecord
    message.key = 35
    message.protocol = ascii("test-protocol")
    message.value = ascii("test content").buffer
    message.size = message.value.length
    tx.store(message)


    val disposed = new CountDownLatch(1)

    var queueEntry = new QueueEntryRecord
    queueEntry.queueKey = queueA.key
    queueEntry.messageKey = message.key
    queueEntry.queueSeq = 1

    tx.enqueue(queueEntry)
    tx.setDisposer(^{ disposed.countDown })
    tx.dispose

    // It should not finish disposing right away...
    expect(false) {
      disposed.await(5, TimeUnit.SECONDS)
    }

    var flushed = new CountDownLatch(1)
    store.flushMessage(message.key) {
      flushed.countDown
    }

    // Should flush quickly now..
    expect(true) {
      flushed.await(1, TimeUnit.SECONDS)
    }
    // Flushing triggers the tx to finish disposing.
    expect(true) {
      disposed.await(1, TimeUnit.SECONDS)
    }

    // add another message to the queue..
    tx = store.createStoreBatch
    message = new MessageRecord
    message.key = 36
    message.protocol = ascii("test-protocol")
    message.value = ascii("test content").buffer
    message.size = message.value.length
    tx.store(message)

    queueEntry = new QueueEntryRecord
    queueEntry.queueKey = queueA.key
    queueEntry.messageKey = message.key
    queueEntry.queueSeq = 2

    tx.enqueue(queueEntry)

    flushed = new CountDownLatch(1)
    store.flushMessage(message.key) {
      flushed.countDown
    }
    flushed.await

    val qso:Option[QueueStatus] = CB( cb=> store.getQueueStatus(queueA.key)(cb) )
    expect(ascii("queue:1")) {
      qso.get.record.name
    }
    expect(2) {
      qso.get.count
    }

    println("xx")

  }
    
}
