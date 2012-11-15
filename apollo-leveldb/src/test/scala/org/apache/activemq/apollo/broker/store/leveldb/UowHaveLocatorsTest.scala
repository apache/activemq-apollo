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
package org.apache.activemq.apollo.broker.store.leveldb

import dto.LevelDBStoreDTO
import org.apache.activemq.apollo.broker.store.{QueueEntryRecord, Store, StoreFunSuiteSupport}
import org.fusesource.hawtdispatch.TaskTracker
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference


/**
 * <p>Tests specifically for APLO-201</p>
 *
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
class UowHaveLocatorsTest extends StoreFunSuiteSupport {


  override protected def get_flush_delay() = 500


  def create_store(flushDelay: Long): Store = {
    new LevelDBStore({
      val rc = new LevelDBStoreDTO
      rc.directory = data_directory
      rc.flush_delay = flushDelay
      rc
    })
  }

  test("APLO-201: Persistent Store: UOW with message locator and no message (previously flushed)"){
    val queue = add_queue("A")
    val batch = store.create_uow
    val m1 = add_message(batch, "Hello!")
    val queueEntryRecord: QueueEntryRecord =  entry(queue, 1, m1)
    batch.enqueue(queueEntryRecord)

    var tracker = new TaskTracker("uknown", 0)
    var task = tracker.task("uow complete")
    batch.on_complete(task.run)
    batch.release

    assert(queueEntryRecord.message_locator.get() == null)

    expect(true) {
      tracker.await(2, TimeUnit.SECONDS)
    }
    assert(queueEntryRecord.message_locator.get() != null)

    val batch2 = store.create_uow
    batch2.enqueue(queueEntryRecord)

    tracker = new TaskTracker("uknown", 0)
    task = tracker.task("uow complete")
    batch2.on_complete(task.run)
    batch2.release

    expect(true) {
      tracker.await(2, TimeUnit.SECONDS)
    }
  }

  // needed to get access to the DelayableUOW class
  // note, this will be "locator_based" since we're using levelDB
  class LocatorBasedStore(val configDto: LevelDBStoreDTO = new LevelDBStoreDTO) extends LevelDBStore(configDto){
    def create_uow_delayable() = new DelayableUOW
  }

  test("Have message locators for locator-based store"){

    val uow = (new LocatorBasedStore).create_uow_delayable()
    val queueEntry = new QueueEntryRecord
    queueEntry.message_key = 1L
    queueEntry.message_locator = new AtomicReference[Object]

    uow.enqueue(queueEntry)
    assert(uow.have_locators == false)

    uow.dequeue(queueEntry)
    assert(uow.have_locators == false)

    queueEntry.message_locator.set("test")
    assert(uow.have_locators == true)

  }

  test("Have message locators for enqueues"){
    val uow = (new LocatorBasedStore).create_uow_delayable()
    val queueEntry = new QueueEntryRecord
    queueEntry.message_key = 1L
    queueEntry.message_locator = new AtomicReference[Object]
    queueEntry.message_locator.set("test")

    uow.enqueue(queueEntry)
    assert(uow.have_locators == true)

  }

}
