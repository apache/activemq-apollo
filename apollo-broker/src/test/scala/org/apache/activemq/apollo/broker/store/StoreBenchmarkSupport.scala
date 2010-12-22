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
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger, AtomicBoolean}
import org.apache.activemq.apollo.util.{LoggingTracker, FunSuiteSupport, LongCounter}

/**
 * <p>Implements generic testing of Store implementations.</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract class StoreBenchmarkSupport extends FunSuiteSupport with BeforeAndAfterEach {

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
    var queueA = new QueueRecord
    queueA.key = queue_key_counter.incrementAndGet
    queueA.binding_kind = ascii("test")
    queueA.binding_data = ascii(name)
    val rc:Boolean = CB( cb=> store.add_queue(queueA)(cb) )
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


  def entry(queue_key:Long, entry_seq:Long, message_key:Long=0) = {
    var queueEntry = new QueueEntryRecord
    queueEntry.queue_key = queue_key
    queueEntry.entry_seq = entry_seq
    queueEntry.message_key = message_key
    queueEntry
  }


  def payload(prefix:String, messageSize:Int) = {
    val buffer = new StringBuffer(messageSize)
    buffer.append(prefix);
    for( i <- buffer.length to messageSize ) {
      buffer.append(('a'+(i%26)).toChar)
    }
    var rc = buffer.toString
    if( rc.length > messageSize ) {
      rc.substring(0, messageSize)
    } else {
      rc
    }
  }

  def populate(queue_key:Long, messages:List[String], firstSeq:Long=1) = {
    var batch = store.create_uow
    var msgKeys = ListBuffer[Long]()
    var nextSeq = firstSeq

    messages.foreach { message=>
      val msgKey = addMessage(batch, message)
      msgKeys += msgKey
      batch.enqueue(entry(queue_key, nextSeq, msgKey))
      nextSeq += 1
    }

    val tracker = new TaskTracker()
    tracker.release(batch)
    msgKeys.foreach { msg_key =>
      store.flush_message(msg_key) {}
    }
    tracker.await
    msgKeys
  }

  test("store enqueue and load latencey") {
    val A = add_queue("A")
    var message_keys = storeMessages(A)
    loadMessages(A, message_keys)
  }

  def storeMessages(queue:Long) = {

    var seq = 0L
    var message_keys = ListBuffer[Long]()

    val content = payload("message\n", 1024)
    var metric = benchmarkCount(100000) {
      seq += 1

      var batch = store.create_uow
      val message = addMessage(batch, content)
      message_keys += message
      batch.enqueue(entry(queue, seq, message))

      val latch = new CountDownLatch(1)
      batch.setDisposer(^{latch.countDown} )
      batch.release
      store.flush_message(message) {}

      latch.await

    }
    println("enqueue metrics: "+metric)
    println("enqueue latency is: "+metric.latency(TimeUnit.MILLISECONDS)+" ms")
    println("enqueue rate is: "+metric.rate(TimeUnit.SECONDS)+" enqueues/s")
    message_keys.toList
  }

  def loadMessages(queue:Long, message_keys: List[Long]) = {

    var keys = message_keys.toList
    val metric = benchmarkCount(keys.size) {
      val latch = new CountDownLatch(1)
      store.load_message(keys.head) { msg=>
        assert(msg.isDefined, "message key not found: "+keys.head)
        latch.countDown
      }
      latch.await
      keys = keys.drop(1)
    }

    println("load metrics: "+metric)
    println("load latency is: "+metric.latency(TimeUnit.MILLISECONDS)+" ms")
    println("load rate is: "+metric.rate(TimeUnit.SECONDS)+" loads/s")

  }

  case class Metric(count:Long, duration:Long) {
    def latency(unit:TimeUnit) = {
      ((duration).toFloat / unit.toNanos(1)) / count
    }
    def rate(unit:TimeUnit) = {
      (count.toFloat * unit.toNanos(1) ) / duration
    }
  }

  def benchmarkFor(duration:Int)(func: =>Unit ) = {

    val counter = new AtomicLong()
    val done = new AtomicBoolean()
    val warmup = new AtomicBoolean(true)

    var startT = 0L
    var endT = 0L
    val thread = new Thread("benchmarked task") {

      override def run = {
        while(warmup.get) {
          func
        }
        startT = System.nanoTime();
        while(!done.get) {
          func
          counter.incrementAndGet
        }
        endT = System.nanoTime();
      }
    }

    thread.start()

    Thread.sleep(1000*5)
    warmup.set(false)
    Thread.sleep(1000*duration)
    done.set(true)
    thread.join

    Metric(counter.get, endT-startT)
  }

  def benchmarkCount(iterations:Int)(func: =>Unit ) = {
    val startT = System.nanoTime();
    var i = 0
    while( i < iterations) {
      func
      i += 1
    }
    val endT = System.nanoTime();
    Metric(iterations, endT-startT)
  }
}
