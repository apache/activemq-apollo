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

import org.fusesource.hawtbuf.AsciiBuffer._
import org.fusesource.hawtdispatch.ScalaDispatch._
import org.fusesource.hawtdispatch.TaskTracker
import java.util.concurrent.{TimeUnit, CountDownLatch}
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll}
import collection.mutable.ListBuffer
import org.apache.activemq.apollo.util.{LoggingTracker, FunSuiteSupport, LongCounter}

/**
 * <p>Implements generic testing of Store implementations.</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract class StoreFunSuiteSupport extends FunSuiteSupport with BeforeAndAfterEach {

  var store:Store = null

  def createStore(flushDelay:Long):Store

  /**
   * Handy helper to call an async method on the store and wait for
   * the result of the callback.
   */
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


  override protected def beforeAll() = {
    store = createStore(5*1000)
    val tracker = new LoggingTracker("store startup")
    tracker.start(store)
    tracker.await
  }

  override protected def afterAll() = {
    val tracker = new LoggingTracker("store stop")
    tracker.stop(store)
    tracker.await
  }

  override protected def beforeEach() = {
    val tracker = new LoggingTracker("store startup")
    val task = tracker.task("purge")
    store.purge(task.run)
    tracker.await
  }

  def expectCB[T](expected:T)(func: (T=>Unit)=>Unit ) = {
    expect(expected) {
      CB(func)
    }
  }

  val queue_key_counter = new LongCounter

  def addQueue(name:String):Long = {
    var queueA = new QueueRecord
    queueA.key = queue_key_counter.incrementAndGet
    queueA.binding_kind = ascii("test")
    queueA.binding_data = ascii(name)
    val rc:Boolean = CB( cb=> store.addQueue(queueA)(cb) )
    expect(true)(rc)
    queueA.key
  }

  def addMessage(batch:StoreUOW, content:String):Long = {
    var message = new MessageRecord
    message.protocol = ascii("test-protocol")
    message.buffer = ascii(content).buffer
    message.size = message.buffer.length
    batch.store(message)
  }


  def entry(queueKey:Long, queueSeq:Long, messageKey:Long=0) = {
    var queueEntry = new QueueEntryRecord
    queueEntry.queueKey = queueKey
    queueEntry.queueSeq = queueSeq
    queueEntry.messageKey = messageKey
    queueEntry
  }

  def populate(queueKey:Long, messages:List[String], firstSeq:Long=1) = {
    var batch = store.createStoreUOW
    var msgKeys = ListBuffer[Long]()
    var nextSeq = firstSeq

    messages.foreach { message=>
      val msgKey = addMessage(batch, message)
      msgKeys += msgKey
      batch.enqueue(entry(queueKey, nextSeq, msgKey))
      nextSeq += 1
    }

    val tracker = new TaskTracker()
    tracker.release(batch)
    msgKeys.foreach { msgKey =>
      store.flushMessage(msgKey) {}
    }
    tracker.await
    msgKeys
  }

  test("load stored message") {
    val A = addQueue("A")
    val msgKeys = populate(A, "message 1"::"message 2"::"message 3"::Nil)

    val rc:Option[MessageRecord] = CB( cb=> store.loadMessage(msgKeys.head)(cb) )
    expect(ascii("message 1").buffer) {
      rc.get.buffer
    }
  }

  test("add and list queues") {
    val A = addQueue("A")
    val B = addQueue("B")
    val C = addQueue("C")

    expectCB(List(A,B,C).toSeq) { cb=>
      store.listQueues(cb)
    }
  }

  test("get queue status") {
    val A = addQueue("my queue name")
    populate(A, "message 1"::"message 2"::"message 3"::Nil)

    val rc:Option[QueueRecord] = CB( cb=> store.getQueue(A)(cb) )
    expect(ascii("my queue name")) {
      rc.get.binding_data.ascii
    }
  }

  test("list queue entries") {
    val A = addQueue("A")
    val msgKeys = populate(A, "message 1"::"message 2"::"message 3"::Nil)

    val rc:Seq[QueueEntryRecord] = CB( cb=> store.listQueueEntries(A,msgKeys.head, msgKeys.last)(cb) )
    expect(msgKeys.toSeq) {
      rc.map( _.messageKey )
    }
  }

  test("batch completes after a delay") {x}
  def x = {
    val A = addQueue("A")
    var batch = store.createStoreUOW

    val m1 = addMessage(batch, "message 1")
    batch.enqueue(entry(A, 1, m1))

    val tracker = new TaskTracker()
    tracker.release(batch)
    expect(false) {
      tracker.await(3, TimeUnit.SECONDS)
    }
    expect(true) {
      tracker.await(3, TimeUnit.SECONDS)
    }
  }

  test("flush cancels the delay") {
    val A = addQueue("A")
    var batch = store.createStoreUOW

    val m1 = addMessage(batch, "message 1")
    batch.enqueue(entry(A, 1, m1))

    val tracker = new TaskTracker()
    tracker.release(batch)

    store.flushMessage(m1) {}

    expect(true) {
      tracker.await(1, TimeUnit.SECONDS)
    }
  }


}
