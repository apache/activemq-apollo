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
package org.apache.activemq.broker.store

import org.fusesource.hawtbuf.AsciiBuffer._
import org.fusesource.hawtdispatch.ScalaDispatch._
import org.fusesource.hawtdispatch.TaskTracker
import org.apache.activemq.apollo.broker.{LoggingTracker, FunSuiteSupport}
import java.util.concurrent.{TimeUnit, CountDownLatch}
import org.apache.activemq.apollo.store.{QueueEntryRecord, QueueStatus, QueueRecord, MessageRecord}
import org.scalatest.{BeforeAndAfterEach, BeforeAndAfterAll}
import collection.mutable.ListBuffer
import java.util.concurrent.atomic.{AtomicLong, AtomicInteger, AtomicBoolean}

/**
 * <p>Implements generic testing of Store implementations.</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract class StoreBenchmarkSupport extends FunSuiteSupport with BeforeAndAfterEach {

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

  def addQueue(name:String):Long = {
    var queueA = new QueueRecord
    queueA.name = ascii(name)
    val rc:Option[Long] = CB( cb=> store.addQueue(queueA)(cb) )
    expect(true)(rc.isDefined)
    rc.get
  }

  def addMessage(batch:StoreBatch, content:String):Long = {
    var message = new MessageRecord
    message.protocol = ascii("test-protocol")
    message.value = ascii(content).buffer
    message.size = message.value.length
    batch.store(message)
  }


  def entry(queueKey:Long, queueSeq:Long, messageKey:Long=0) = {
    var queueEntry = new QueueEntryRecord
    queueEntry.queueKey = queueKey
    queueEntry.queueSeq = queueSeq
    queueEntry.messageKey = messageKey
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

  def populate(queueKey:Long, messages:List[String], firstSeq:Long=1) = {
    var batch = store.createStoreBatch
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

  test("store enqueue latencey") {
    val A = addQueue("A")
    var seq = 0

    val content = payload("message\n", 1024)
    val metric = benchmark {
      seq += 1

      var batch = store.createStoreBatch
      val message = addMessage(batch, content)
      batch.enqueue(entry(A, seq, message))

      val latch = new CountDownLatch(1)
      batch.setDisposer(^{cd(latch)} )
      batch.release
      store.flushMessage(message) {}

      latch.await

    }
    println("enqueue metrics: "+metric)
    println("enqueue latency is: "+metric.latency(TimeUnit.MILLISECONDS)+" ms")
    println("enqueue rate is: "+metric.rate(TimeUnit.SECONDS)+" enqueues/s")
  }

  def cd(latch:CountDownLatch) = {
    latch.countDown
  }


  case class Metric(count:Long, duration:Long) {
    def latency(unit:TimeUnit) = {
      ((duration).toFloat / unit.toNanos(1)) / count
    }
    def rate(unit:TimeUnit) = {
      (count.toFloat * unit.toNanos(1) ) / duration
    }
  }

  def benchmark(func: =>Unit ) = {

    val counter = new AtomicLong()
    val done = new AtomicBoolean()
    var startT = 0L
    var endT = 0L
    val thread = new Thread("benchmarked task") {
      override def run = {
        startT = System.nanoTime();
        while(!done.get) {
          func
          counter.incrementAndGet
        }
        endT = System.nanoTime();
      }
    }

    thread.start()
    Thread.sleep(1000*30)
    done.set(true)
    thread.join

    Metric(counter.get, endT-startT)
  }


}
