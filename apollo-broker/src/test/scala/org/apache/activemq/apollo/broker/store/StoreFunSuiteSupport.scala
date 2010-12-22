package org.apache.activemq.apollo.broker.store

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
import org.fusesource.hawtbuf.AsciiBuffer._
import org.fusesource.hawtdispatch._
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

  def create_store(flushDelay:Long):Store

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
    store = create_store(5*1000)
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

  def add_queue(name:String):Long = {
    var queue_a = new QueueRecord
    queue_a.key = queue_key_counter.incrementAndGet
    queue_a.binding_kind = ascii("test")
    queue_a.binding_data = ascii(name)
    val rc:Boolean = CB( cb=> store.add_queue(queue_a)(cb) )
    expect(true)(rc)
    queue_a.key
  }

  def add_message(batch:StoreUOW, content:String):Long = {
    var message = new MessageRecord
    message.protocol = ascii("test-protocol")
    message.buffer = ascii(content).buffer
    message.size = message.buffer.length
    batch.store(message)
  }


  def entry(queue_key:Long, entry_seq:Long, message_key:Long=0) = {
    var queueEntry = new QueueEntryRecord
    queueEntry.queue_key = queue_key
    queueEntry.entry_seq = entry_seq
    queueEntry.message_key = message_key
    queueEntry
  }

  def populate(queue_key:Long, messages:List[String], first_seq:Long=1) = {
    var batch = store.create_uow
    var msg_keys = ListBuffer[Long]()
    var next_seq = first_seq

    messages.foreach { message=>
      val msgKey = add_message(batch, message)
      msg_keys += msgKey
      batch.enqueue(entry(queue_key, next_seq, msgKey))
      next_seq += 1
    }

    val tracker = new TaskTracker()
    tracker.release(batch)
    msg_keys.foreach { msgKey =>
      store.flush_message(msgKey) {}
    }
    tracker.await
    msg_keys
  }

  test("load stored message") {
    val A = add_queue("A")
    val msg_keys = populate(A, "message 1"::"message 2"::"message 3"::Nil)

    val rc:Option[MessageRecord] = CB( cb=> store.load_message(msg_keys.head)(cb) )
    expect(ascii("message 1").buffer) {
      rc.get.buffer
    }
  }

  test("add and list queues") {
    val A = add_queue("A")
    val B = add_queue("B")
    val C = add_queue("C")

    expectCB(List(A,B,C).toSeq) { cb=>
      store.list_queues(cb)
    }
  }

  test("get queue status") {
    val A = add_queue("my queue name")
    populate(A, "message 1"::"message 2"::"message 3"::Nil)

    val rc:Option[QueueRecord] = CB( cb=> store.get_queue(A)(cb) )
    expect(ascii("my queue name")) {
      rc.get.binding_data.ascii
    }
  }

  test("list queue entries") {
    val A = add_queue("A")
    val msg_keys = populate(A, "message 1"::"message 2"::"message 3"::Nil)

    val rc:Seq[QueueEntryRecord] = CB( cb=> store.list_queue_entries(A,msg_keys.head, msg_keys.last)(cb) )
    expect(msg_keys.toSeq) {
      rc.map( _.message_key )
    }
  }

  test("batch completes after a delay") {x}
  def x = {
    val A = add_queue("A")
    var batch = store.create_uow

    val m1 = add_message(batch, "message 1")
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
    val A = add_queue("A")
    var batch = store.create_uow

    val m1 = add_message(batch, "message 1")
    batch.enqueue(entry(A, 1, m1))

    val tracker = new TaskTracker()
    tracker.release(batch)

    store.flush_message(m1) {}

    expect(true) {
      tracker.await(1, TimeUnit.SECONDS)
    }
  }


}
