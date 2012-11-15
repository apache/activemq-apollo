/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.broker.store

import org.apache.activemq.apollo.util._
import org.fusesource.hawtbuf.AsciiBuffer._
import org.apache.activemq.apollo.util.FileSupport._
import java.io.{FileInputStream, BufferedInputStream, FileOutputStream, BufferedOutputStream}
import org.fusesource.hawtdispatch.TaskTracker
import java.util.concurrent.TimeUnit


/**
 *
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
abstract class StoreTests extends StoreFunSuiteSupport {

  test("add and list queues") {
    val A = add_queue("A")
    val B = add_queue("B")
    val C = add_queue("C")

    val seq:Seq[Long] = List(A,B,C).toSeq
    expectCB(seq) { cb=>
      store.list_queues(cb)
    }
  }

  test("export and import") {
    val A = add_queue("A")
    val msg_keys = populate(A, "message 1"::"message 2"::"message 3"::Nil)

    val rc:Option[MessageRecord] = sync_cb( cb=> store.load_message(msg_keys.head._1, msg_keys.head._2)(cb) )
    expect(ascii("message 1").buffer) {
      rc.get.buffer
    }

    val file = test_data_dir / "export.tgz"
    file.getParentFile.mkdirs()
    using( new BufferedOutputStream(new FileOutputStream(file))) { os =>
    // Export the data...
      expect(None) {
        sync_cb[Option[String]] { cb =>
          store.export_data(os, cb)
        }
      }
    }

    // purge the data..
    purge

    // There should ne no queues..
    expectCB(Seq[Long]()) { cb=>
      store.list_queues(cb)
    }

    // Import the data..
    using(new BufferedInputStream(new FileInputStream(file))) { is =>
      expect(None) {
        sync_cb[Option[String]] { cb =>
          store.import_data(is, cb)
        }
      }
    }

    // The data should be there now again..
    val queues:Seq[Long] = sync_cb(store.list_queues(_))
    expect(1)(queues.size)
    val entries:Seq[QueueEntryRecord] = sync_cb(cb=> store.list_queue_entries(A,0, Long.MaxValue)(cb))
    expect(3) ( entries.size  )

  }

  test("load stored message") {
    val A = add_queue("A")
    val msg_keys = populate(A, "message 1"::"message 2"::"message 3"::Nil)

    val rc:Option[MessageRecord] = sync_cb( cb=> store.load_message(msg_keys.head._1, msg_keys.head._2)(cb) )
    expect(ascii("message 1").buffer) {
      rc.get.buffer
    }
  }

  test("get queue status") {
    val A = add_queue("my queue name")
    populate(A, "message 1"::"message 2"::"message 3"::Nil)

    val rc:Option[QueueRecord] = sync_cb( cb=> store.get_queue(A)(cb) )
    expect(ascii("my queue name")) {
      rc.get.binding_data.ascii
    }
  }

  test("list queue entries") {
    val A = add_queue("A")
    val msg_keys = populate(A, "message 1"::"message 2"::"message 3"::Nil)

    val rc:Seq[QueueEntryRecord] = sync_cb( cb=> store.list_queue_entries(A,0, Long.MaxValue)(cb) )
    expect(msg_keys.toSeq.map(_._3)) {
      rc.map( _.entry_seq )
    }
  }

  test("batch completes after a delay") {x}
  def x = {
    val A = add_queue("A")
    var batch = store.create_uow

    val m1 = add_message(batch, "message 1")
    batch.enqueue(entry(A, 1, m1))

    val tracker = new TaskTracker("unknown", 0)
    val task = tracker.task("uow complete")
    batch.on_complete(task.run)
    batch.release

    expect(false) {
      tracker.await(3, TimeUnit.SECONDS)
    }
    expect(true) {
      tracker.await(5, TimeUnit.SECONDS)
    }
  }

  test("flush cancels the delay") {
    val A = add_queue("A")
    var batch = store.create_uow

    val m1 = add_message(batch, "message 1")
    batch.enqueue(entry(A, 1, m1))

    val tracker = new TaskTracker("unknown", 0)
    val task = tracker.task("uow complete")
    batch.on_complete(task.run)
    batch.release

    store.flush_message(m1._1) {}

    expect(true) {
      tracker.await(1, TimeUnit.SECONDS)
    }
  }
}
