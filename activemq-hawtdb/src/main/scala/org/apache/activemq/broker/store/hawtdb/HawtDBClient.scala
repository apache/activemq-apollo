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
package org.apache.activemq.broker.store.hawtdb

import model.{AddQueue, AddQueueEntry, AddMessage}
import org.apache.activemq.apollo.store.{QueueEntryRecord, QueueStatus, MessageRecord}
import org.apache.activemq.apollo.dto.HawtDBStoreDTO
import java.io.File
import java.io.IOException
import java.util.concurrent.TimeUnit
import org.apache.activemq.apollo.store.QueueRecord
import org.fusesource.hawtbuf.proto.MessageBuffer
import org.fusesource.hawtbuf.proto.PBMessage
import org.apache.activemq.util.LockFile
import org.fusesource.hawtdb.api.{Transaction, TxPageFileFactory}
import java.util.HashSet
import collection.mutable.{HashMap, ListBuffer}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import org.fusesource.hawtdb.internal.journal.{JournalCallback, Journal, Location}
import org.fusesource.hawtdispatch.TaskTracker

import org.fusesource.hawtbuf.AsciiBuffer._
import org.apache.activemq.broker.store.hawtdb.model.Type._
import org.apache.activemq.broker.store.hawtdb.model._
import org.fusesource.hawtbuf._
import org.fusesource.hawtdispatch.ScalaDispatch._
import org.apache.activemq.apollo.broker.{Log, Logging, BaseService}

object HawtDBClient extends Log {
  
  type PB = PBMessage[_ <: PBMessage[_,_], _ <: MessageBuffer[_,_]]

  implicit def toPBMessage(value:TypeCreatable):PB = value.asInstanceOf[PB]

  val BEGIN = -1
  val COMMIT = -2

  val DATABASE_LOCKED_WAIT_DELAY = 10 * 1000

  val CLOSED_STATE = 1
  val OPEN_STATE = 2

  implicit def decodeMessageRecord(pb: AddMessage.Getter): MessageRecord = {
    val rc = new MessageRecord
    rc.protocol = pb.getProtocol
    rc.size = pb.getSize
    rc.value = pb.getValue
    rc.stream = pb.getStreamKey
    rc.expiration = pb.getExpiration
    rc
  }

  implicit def encodeMessageRecord(v: MessageRecord): AddMessage.Bean = {
    val pb = new AddMessage.Bean
    pb.setProtocol(v.protocol)
    pb.setSize(v.size)
    pb.setValue(v.value)
    pb.setStreamKey(v.stream)
    pb.setExpiration(v.expiration)
    pb
  }

  implicit def decodeQueueEntryRecord(pb: AddQueueEntry.Getter): QueueEntryRecord = {
    val rc = new QueueEntryRecord
    rc.messageKey = pb.getMessageKey
    rc.attachment = pb.getAttachment
    rc.size = pb.getSize
    rc.redeliveries = pb.getRedeliveries.toShort
    rc
  }

  implicit def encodeQueueEntryRecord(v: QueueEntryRecord): AddQueueEntry.Bean = {
    val pb = new AddQueueEntry.Bean
    pb.setMessageKey(v.messageKey)
    pb.setAttachment(v.attachment)
    pb.setSize(v.size)
    pb.setRedeliveries(v.redeliveries)
    pb
  }
}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class HawtDBClient() extends Logging {
  import HawtDBClient._

  override def log: Log = HawtDBClient

  val dispatchQueue = createQueue("hawtdb store")


  private val pageFileFactory = new TxPageFileFactory()
  private var journal: Journal = null

  private var lockFile: LockFile = null
  private var nextRecoveryPosition: Location = null
  private var lastRecoveryPosition: Location = null
  private val trackingGen = new AtomicLong(0)

  private val journalFilesBeingReplicated = new HashSet[Integer]()
  private var recovering = false

  //protected RootEntity rootEntity = new RootEntity()

  /////////////////////////////////////////////////////////////////////
  //
  // Helpers
  //
  /////////////////////////////////////////////////////////////////////

  private def directory = config.directory

  private def journalMaxFileLength = config.journalLogSize

  private def checkpointInterval = config.checkpointInterval

  private def cleanupInterval = config.cleanupInterval

  private def failIfDatabaseIsLocked = config.failIfLocked

  private def pageFile = pageFileFactory.getTxPageFile()


  /////////////////////////////////////////////////////////////////////
  //
  // Public interface
  //
  /////////////////////////////////////////////////////////////////////

  var config: HawtDBStoreDTO = null

  def lock(func: => Unit) {
    val lockFileName = new File(directory, "lock")
    lockFile = new LockFile(lockFileName, true)
    if (failIfDatabaseIsLocked) {
      lockFile.lock()
      func
    } else {
      val locked = try {
        lockFile.lock()
        true
      } catch {
        case e: IOException =>
          false
      }
      if (locked) {
        func
      } else {
        info("Database " + lockFileName + " is locked... waiting " + (DATABASE_LOCKED_WAIT_DELAY / 1000) + " seconds for the database to be unlocked.")
        dispatchQueue.dispatchAfter(DATABASE_LOCKED_WAIT_DELAY, TimeUnit.MILLISECONDS, ^ {lock(func _)})
      }
    }
  }

  def start() = {
    lock {

      journal = new Journal()
      journal.setDirectory(directory)
      journal.setMaxFileLength(config.journalLogSize)
      journal.start

      pageFileFactory.setFile(new File(directory, "db"))
      pageFileFactory.setDrainOnClose(false)
      pageFileFactory.setSync(true)
      pageFileFactory.setUseWorkerThread(true)
      pageFileFactory.open()

      withTx {tx =>
        if (!tx.allocator().isAllocated(0)) {
          //            rootEntity.allocate(tx)
        }
        //        rootEntity.load(tx)
      }
      pageFile.flush()
      //        recover()
      //        trackingGen.set(rootEntity.getLastMessageTracking() + 1)

      //      checkpointThread = new Thread("ActiveMQ Journal Checkpoint Worker") {
      //          public void run() {
      //              try {
      //                  long lastCleanup = System.currentTimeMillis()
      //                  long lastCheckpoint = System.currentTimeMillis()
      //
      //                  // Sleep for a short time so we can periodically check
      //                  // to see if we need to exit this thread.
      //                  long sleepTime = Math.min(checkpointInterval, 500)
      //                  while (opened.get()) {
      //                      Thread.sleep(sleepTime)
      //                      long now = System.currentTimeMillis()
      //                      if (now - lastCleanup >= cleanupInterval) {
      //                          checkpointCleanup(true)
      //                          lastCleanup = now
      //                          lastCheckpoint = now
      //                      } else if (now - lastCheckpoint >= checkpointInterval) {
      //                          checkpointCleanup(false)
      //                          lastCheckpoint = now
      //                      }
      //                  }
      //              } catch (InterruptedException e) {
      //                  // Looks like someone really wants us to exit this
      //                  // thread...
      //              }
      //          }
      //      }
      //      checkpointThread.start()

    }
  }

  def stop() = {
  }


  def addQueue(record: QueueRecord) = {
    val update = new AddQueue.Bean()
    update.setKey(record.key)
    update.setName(record.name)
    update.setQueueType(record.queueType)
    store(update)
  }

  def purge() = {
//    withSession {
//      session =>
//        session.list(schema.queue_name).map {
//          x =>
//            val qid: Long = x.name
//            session.remove(schema.entries \ qid)
//        }
//        session.remove(schema.queue_name)
//        session.remove(schema.message_data)
//    }
  }

  def listQueues: Seq[Long] = {
    null
//    withSession {
//      session =>
//        session.list(schema.queue_name).map {
//          x =>
//            val id: Long = x.name
//            id
//        }
//    }
  }

  def getQueueStatus(id: Long): Option[QueueStatus] = {
    null
//    withSession {
//      session =>
//        session.get(schema.queue_name \ id) match {
//          case Some(x) =>
//
//            val rc = new QueueStatus
//            rc.record = new QueueRecord
//            rc.record.key = id
//            rc.record.name = new AsciiBuffer(x.value)
//
//            //            rc.count = session.count( schema.entries \ id )
//
//            // TODO
//            //          rc.count =
//            //          rc.first =
//            //          rc.last =
//
//            Some(rc)
//          case None =>
//            None
//        }
//    }
  }


  def store(txs: Seq[HawtDBStore#HawtDBBatch]) {
//    withSession {
//      session =>
//              var operations = List[Operation]()
//              txs.foreach {
//                tx =>
//                  tx.actions.foreach {
//                    case (msg, action) =>
//                      var rc =
//                      if (action.store != null) {
//                        operations ::= Insert( schema.message_data \ (msg, action.store) )
//                      }
//                      action.enqueues.foreach {
//                        queueEntry =>
//                          val qid = queueEntry.queueKey
//                          val seq = queueEntry.queueSeq
//                          operations ::= Insert( schema.entries \ qid \ (seq, queueEntry) )
//                      }
//                      action.dequeues.foreach {
//                        queueEntry =>
//                          val qid = queueEntry.queueKey
//                          val seq = queueEntry.queueSeq
//                          operations ::= Delete( schema.entries \ qid, ColumnPredicate(seq :: Nil) )
//                      }
//                  }
//              }
//              session.batch(operations)
//    }
  }

  def loadMessage(id: Long): Option[MessageRecord] = {
    null
//    withSession {
//      session =>
//        session.get(schema.message_data \ id) match {
//          case Some(x) =>
//            val rc: MessageRecord = x.value
//            rc.key = id
//            Some(rc)
//          case None =>
//            None
//        }
//    }
  }

  def getQueueEntries(qid: Long): Seq[QueueEntryRecord] = {
    null
//    withSession {
//      session =>
//        session.list(schema.entries \ qid).map {
//          x =>
//            val rc: QueueEntryRecord = x.value
//            rc.queueKey = qid
//            rc.queueSeq = x.name
//            rc
//        }
//    }
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation
  //
  /////////////////////////////////////////////////////////////////////

  private def withTx[T](func: (Transaction) => T) {
    val tx = pageFile.tx
    var ok = false
    try {
      val rc = func(tx)
      ok = true
      rc
    } finally {
      if (ok) {
        tx.commit
      } else {
        tx.rollback
      }
    }
  }

  val next_batch_counter = new AtomicInteger(0)

  // Gets the next batch id.. after a while we may wrap around
  // start producing batch ids from zero
  val next_batch_id = {
    var rc = next_batch_counter.getAndIncrement
    while (rc < 0) {
      // We just wrapped around.. reset the counter to 0
      // Use a CAS operation so that only 1 thread resets the counter
      next_batch_counter.compareAndSet(rc + 1, 0)
      rc = next_batch_counter.getAndIncrement
    }
    rc
  }


  private def store(updates: List[TypeCreatable]):Unit = {
    val tracker = new TaskTracker("storing")
    store( updates, tracker.task(updates))
    tracker.await
  }

  private def store(update: TypeCreatable):Unit = {
    val tracker = new TaskTracker("storing")
    store( update, tracker.task(update))
    tracker.await
  }

  private def store(updates: List[TypeCreatable], onComplete: Runnable):Unit = {
    val batch = next_batch_id
    begin(batch)
    updates.foreach {update =>
      store(batch, update, null)
    }
    commit(batch, onComplete)
  }

  private def store(update: TypeCreatable, onComplete: Runnable):Unit = store(-1, update, onComplete)

  /**
   * All updated are are funneled through this method. The updates are logged to
   * the journal and then the indexes are update.  onFlush will be called back once
   * this all completes and the index has the update.
   *
   * @throws IOException
   */
  private def store(batch: Int, update: TypeCreatable, onComplete: Runnable):Unit = {
    val kind = update.asInstanceOf[TypeCreatable]
    val frozen = update.freeze
    val baos = new DataByteArrayOutputStream(frozen.serializedSizeUnframed + 1)
    baos.writeByte(kind.toType().getNumber())
    baos.writeInt(batch)
    frozen.writeUnframed(baos)

    journal(baos.toBuffer()) {location =>
      store(batch, update, onComplete, location)
    }
  }


  /**
   */
  private def begin(batch: Int):Unit = {
    val baos = new DataByteArrayOutputStream(5)
    baos.writeByte(BEGIN)
    baos.writeInt(batch)
    journal(baos.toBuffer) {location =>
      begin(batch, location)
    }
  }

  /**
   */
  private def commit(batch: Int, onComplete: Runnable):Unit = {
    val baos = new DataByteArrayOutputStream(5)
    baos.writeByte(COMMIT)
    baos.writeInt(batch)
    journal(baos.toBuffer) {location =>
      commit(batch, onComplete, location)
    }
  }

  private def journal(data: Buffer)(cb: (Location) => Unit):Unit = {
    val start = System.currentTimeMillis()
    try {
      journal.write(data, new JournalCallback() {
        def success(location: Location) = {
          cb(location)
        }
      })
    } finally {
      val end = System.currentTimeMillis()
      if (end - start > 1000) {
        warn("KahaDB long enqueue time: Journal add took: " + (end - start) + " ms")
      }
    }
  }


  /**
   * Move all the messages that were in the journal into long term storage. We
   * just replay and do a checkpoint.
   *
   * @throws IOException
   * @throws IOException
   * @throws IllegalStateException
   */
  def recover = {
    try {
      val start = System.currentTimeMillis()
      recovering = true
      var location = getRecoveryPosition()
      if (location != null) {
        var counter = 0
        var uow: Transaction = null
        val uowCounter = 0
        while (location != null) {
          import BufferEditor.BIG_ENDIAN._

          var data = journal.read(location)
          val updateType = readByte(data)
          val batch = readInt(data)
          updateType match {
            case BEGIN => begin(batch, location)
            case COMMIT => commit(batch, null, location)
            case _ =>
              val update = decode(location, updateType, data)
              store(batch, update, null, location)
          }

          counter += 1
          location = journal.getNextLocation(location)
        }
        val end = System.currentTimeMillis()
        info("Processed %d operations from the journal in %,.3f seconds.", counter, ((end - start) / 1000.0f))
      }

      // We may have to undo some index updates.
//      withTx {tx =>
//        recoverIndex(tx)
//      }
    } finally {
      recovering = false
    }
  }

  def decode(location:Location, updateType:Int, value:Buffer) = {
      val t = Type.valueOf(updateType);
      if (t == null) {
          throw new IOException("Could not load journal record. Invalid type at location: " + location);
      }
      t.parseUnframed(value).asInstanceOf[TypeCreatable]
  }


//  def incrementalRecover() = {
//    try {
//      recovering = true
//      if (nextRecoveryPosition == null) {
//        if (lastRecoveryPosition == null) {
//          nextRecoveryPosition = getRecoveryPosition()
//        } else {
//          nextRecoveryPosition = journal.getNextLocation(lastRecoveryPosition)
//        }
//      }
//      while (nextRecoveryPosition != null) {
//        lastRecoveryPosition = nextRecoveryPosition
//        rootEntity.setLastUpdate(lastRecoveryPosition)
//        val message = load(lastRecoveryPosition)
//        val location = lastRecoveryPosition
//
//        withTx {tx =>
//          updateIndex(tx, message.toType(), (MessageBuffer) message, location)
//        }
//        nextRecoveryPosition = journal.getNextLocation(lastRecoveryPosition)
//      }
//    } finally {
//      recovering = false
//    }
//  }


  def getRecoveryPosition(): Location = {
//    if (rootEntity.getLastUpdate() != null) {
//      // Start replay at the record after the last one recorded in the
//      // index file.
//      return journal.getNextLocation(rootEntity.getLastUpdate());
//    }

    // This loads the first position.
    return journal.getNextLocation(null);
  }


  private var batches = new HashMap[Int, ListBuffer[Update]]()
  private case class Update(update: TypeCreatable, location: Location)

  private def store(batch: Int, update: TypeCreatable, onComplete: Runnable, location: Location): Unit = {
    if (batch == -1) {
      // update is not part of the batch.. apply it now.
      withTx {tx =>
        store(tx, update, location)
      }
      if (onComplete != null) {
        onComplete.run
      }
    } else {
      // if the update was part of a batch don't apply till the batch is committed.
      batches.get(batch) match {
        case Some(updates)=> updates += Update(update, location)
        case None =>
      }
    }
  }

  private def begin(batch: Int, location: Location): Unit = {
    assert( batches.get(batch).isEmpty )
    batches.put(batch, ListBuffer())
  }

  private def commit(batch: Int, onComplete: Runnable, location: Location): Unit = {
    // apply all the updates in the batch as a single unit of work.
    withTx {tx =>
      batches.get(batch) match {
        case Some(updates) =>
          updates.foreach {update =>
            store(tx, update.update, update.location)
          }
          if (onComplete != null) {
            onComplete.run
          }
        case None =>
      }
    }
  }

  private def store(tx: Transaction, update: TypeCreatable, location: Location): Unit = {

  }


}