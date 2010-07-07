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

import java.{lang=>jl}
import java.{util=>ju}

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
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import org.fusesource.hawtdb.internal.journal.{JournalCallback, Journal, Location}
import org.fusesource.hawtdispatch.TaskTracker

import org.fusesource.hawtbuf.AsciiBuffer._
import org.apache.activemq.broker.store.hawtdb.model.Type._
import org.apache.activemq.broker.store.hawtdb.model._
import org.fusesource.hawtbuf._
import org.fusesource.hawtdispatch.ScalaDispatch._
import collection.mutable.{LinkedHashMap, HashMap, ListBuffer}
import collection.JavaConversions
import java.util.{TreeSet, HashSet}

import org.fusesource.hawtdb.api._
import org.apache.activemq.apollo.broker.{DispatchLogging, Log, Logging, BaseService}

object HawtDBClient extends Log {
  val BEGIN = -1
  val COMMIT = -2
  val ROLLBACK = -3

  val DATABASE_LOCKED_WAIT_DELAY = 10 * 1000

  val CLOSED_STATE = 1
  val OPEN_STATE = 2
}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class HawtDBClient(hawtDBStore: HawtDBStore) extends DispatchLogging {
  import HawtDBClient._
  import Helpers._

  override def log: Log = HawtDBClient

  def dispatchQueue = hawtDBStore.dispatchQueue


  private val pageFileFactory = new TxPageFileFactory()
  private var journal: Journal = null

  private var lockFile: LockFile = null
  private val trackingGen = new AtomicLong(0)
  private val lockedDatatFiles = new HashSet[jl.Integer]()

  private var recovering = false
  private var nextRecoveryPosition: Location = null
  private var lastRecoveryPosition: Location = null
  private var recoveryCounter = 0

  var databaseRootRecord = new DatabaseRootRecord.Bean


  val next_batch_counter = new AtomicInteger(0)
  private var batches = new LinkedHashMap[Int, (Location, ListBuffer[Update])]()

  /////////////////////////////////////////////////////////////////////
  //
  // Helpers
  //
  /////////////////////////////////////////////////////////////////////

  private def directory = config.directory

  private def journalMaxFileLength = config.journalLogSize

  private def checkpointInterval = config.indexFlushInterval

  private def cleanupInterval = config.cleanupInterval

  private def failIfDatabaseIsLocked = config.failIfLocked

  private def pageFile = pageFileFactory.getTxPageFile()


  /////////////////////////////////////////////////////////////////////
  //
  // Public interface used by the HawtDBStore
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

  def createJournal() = {
    val journal = new Journal()
    journal.setDirectory(directory)
    journal.setMaxFileLength(config.journalLogSize)
    journal
  }

  val schedual_version = new AtomicInteger()

  def start(onComplete:Runnable) = {
    lock {

      journal = createJournal()
      journal.start

      pageFileFactory.setFile(new File(directory, "db"))
      pageFileFactory.setDrainOnClose(false)
      pageFileFactory.setSync(true)
      pageFileFactory.setUseWorkerThread(true)
      pageFileFactory.open()

      withTx { tx =>
          val helper = new TxHelper(tx)
          import helper._

          if (!tx.allocator().isAllocated(0)) {
            val rootPage = tx.alloc()
            assert(rootPage == 0)

            databaseRootRecord.setQueueIndexPage(alloc(QUEUE_INDEX_FACTORY))
            databaseRootRecord.setMessageKeyIndexPage(alloc(MESSAGE_KEY_INDEX_FACTORY))
            databaseRootRecord.setDataFileRefIndexPage(alloc(DATA_FILE_REF_INDEX_FACTORY))
            databaseRootRecord.setMessageRefsIndexPage(alloc(MESSAGE_REFS_INDEX_FACTORY))
            databaseRootRecord.setSubscriptionIndexPage(alloc(SUBSCRIPTIONS_INDEX_FACTORY))

            tx.put(DATABASE_ROOT_RECORD_ACCESSOR, 0, databaseRootRecord.freeze)
            databaseRootRecord = databaseRootRecord.copy
          } else {
            databaseRootRecord = tx.get(DATABASE_ROOT_RECORD_ACCESSOR, 0).copy
          }
      }

      pageFile.flush()
      recover(onComplete)

      // Schedual periodic jobs.. they keep executing while schedual_version remains the same.
      schedualCleanup(schedual_version.get())
      schedualFlush(schedual_version.get())
    }
  }

  def stop() = {
    // this cancels schedualed jobs...
    schedual_version.incrementAndGet
    flush
  }

  def addQueue(record: QueueRecord) = {
    val update = new AddQueue.Bean()
    update.setKey(record.key)
    update.setName(record.name)
    update.setQueueType(record.queueType)
    store(update)
  }

  def removeQueue(queueKey: Long):Boolean = {
    val update = new RemoveQueue.Bean()
    update.setKey(queueKey)
    store(update)
    true
  }

  def store(txs: Seq[HawtDBStore#HawtDBBatch]) {
    var batch = List[TypeCreatable]()
    txs.foreach {
      tx =>
        tx.actions.foreach {
          case (msg, action) =>
            if (action.messageRecord != null) {
              val update: AddMessage.Bean = action.messageRecord
              batch ::= update
            }
            action.enqueues.foreach {
              queueEntry =>
                val update: AddQueueEntry.Bean = queueEntry
                batch ::= update
            }
            action.dequeues.foreach {
              queueEntry =>
                val qid = queueEntry.queueKey
                val seq = queueEntry.queueSeq
                batch ::= new RemoveQueueEntry.Bean().setQueueKey(qid).setQueueSeq(seq)
            }
        }
    }
    store(batch)
  }

  def purge() = {
    val update = new Purge.Bean()
    store(update)
  }

  def listQueues: Seq[Long] = {
    withTx { tx =>
        val helper = new TxHelper(tx)
        import JavaConversions._
        import helper._
        queueIndex.iterator.map {
          entry =>
            entry.getKey.longValue
        }.toSeq
    }
  }

  def getQueueStatus(queueKey: Long): Option[QueueStatus] = {
    withTx { tx =>
        val helper = new TxHelper(tx)
        import JavaConversions._
        import helper._

        val queueRecord = queueIndex.get(queueKey)
        if (queueRecord != null) {
          val rc = new QueueStatus
          rc.record = new QueueRecord
          rc.record.key = queueKey
          rc.record.name = queueRecord.getInfo.getName
          rc.record.queueType = queueRecord.getInfo.getQueueType
          rc.count = queueRecord.getCount.toInt
          rc.size = queueRecord.getSize

          // TODO
          // rc.first =
          // rc.last =

          Some(rc)
        } else {
          None
        }
    }
  }


  def getQueueEntries(queueKey: Long): Seq[QueueEntryRecord] = {
    withTx { tx =>
        val helper = new TxHelper(tx)
        import JavaConversions._
        import helper._

        val queueRecord = queueIndex.get(queueKey)
        if (queueRecord != null) {
          val entryIndex = queueEntryIndex(queueRecord)
          entryIndex.iterator.map {
            entry =>
              val rc: QueueEntryRecord = entry.getValue
              rc
          }.toSeq
        } else {
          Nil.toSeq
        }
    }
  }

  def loadMessage(messageKey: Long): Option[MessageRecord] = {
    withTx { tx =>
        val helper = new TxHelper(tx)
        import JavaConversions._
        import helper._

        val location = messageKeyIndex.get(messageKey)
        if (location != null) {
          load(location, classOf[AddMessage.Getter]) match {
            case Some(x) =>
              val messageRecord: MessageRecord = x
              Some(messageRecord)
            case None => None
          }
        } else {
          None
        }
    }
  }


  /////////////////////////////////////////////////////////////////////
  //
  // Batch/Transactional interface to storing/accessing journaled updates.
  //
  /////////////////////////////////////////////////////////////////////

  private def load[T <: TypeCreatable](location: Location, expected: Class[T]): Option[T] = {
    try {
      load(location) match {
        case (updateType, batch, data) =>
          Some(expected.cast(decode(location, updateType, data)))
      }
    } catch {
      case e: Exception =>
        debug("Could not load journal record at: %s", location)
        None
    }
  }

  private def store(updates: List[TypeCreatable]): Unit = {
    val tracker = new TaskTracker("storing")
    store(updates, tracker.task(updates))
    tracker.await
  }

  private def store(update: TypeCreatable): Unit = {
    val tracker = new TaskTracker("storing")
    store(update, tracker.task(update))
    tracker.await
  }

  private def store(updates: List[TypeCreatable], onComplete: Runnable): Unit = {
    val batch = next_batch_id
    begin(batch)
    updates.foreach {
      update =>
        store(batch, update, null)
    }
    commit(batch, onComplete)
  }

  private def store(update: TypeCreatable, onComplete: Runnable): Unit = store(-1, update, onComplete)

  /**
   * All updated are are funneled through this method. The updates are logged to
   * the journal and then the indexes are update.  onFlush will be called back once
   * this all completes and the index has the update.
   *
   * @throws IOException
   */
  private def store(batch: Int, update: TypeCreatable, onComplete: Runnable): Unit = {
    val kind = update.asInstanceOf[TypeCreatable]
    val frozen = update.freeze
    val baos = new DataByteArrayOutputStream(frozen.serializedSizeUnframed + 1)
    baos.writeByte(kind.toType().getNumber())
    baos.writeInt(batch)
    frozen.writeUnframed(baos)

    val buffer = baos.toBuffer()
    append(buffer) {
      location =>
        executeStore(batch, update, null, location)
    }
    if(onComplete!=null) {
      onComplete.run
    }
  }

  /**
   */
  private def begin(batch: Int): Unit = {
    val baos = new DataByteArrayOutputStream(5)
    baos.writeByte(BEGIN)
    baos.writeInt(batch)
    append(baos.toBuffer) {
      location =>
        executeBegin(batch, location)
    }
  }

  /**
   */
  private def commit(batch: Int, onComplete: Runnable): Unit = {
    val baos = new DataByteArrayOutputStream(5)
    baos.writeByte(COMMIT)
    baos.writeInt(batch)
    append(baos.toBuffer) {
      location =>
        executeCommit(batch, onComplete, location)
    }
  }

  private def rollback(batch: Int, onComplete: Runnable): Unit = {
    val baos = new DataByteArrayOutputStream(5)
    baos.writeByte(ROLLBACK)
    baos.writeInt(batch)
    append(baos.toBuffer) {
      location =>
        executeRollback(batch, onComplete, location)
    }
  }

  def load(location: Location) = {
    var data = read(location)
    val editor = data.bigEndianEditor
    val updateType = editor.readByte()
    val batch = editor.readInt
    (updateType, batch, data)
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Methods related to recovery
  //
  /////////////////////////////////////////////////////////////////////

  /**
   * Recovers the journal and rollsback any in progress batches that
   * were in progress and never committed.
   *
   * @throws IOException
   * @throws IOException
   * @throws IllegalStateException
   */
  def recover(onComplete:Runnable): Unit = {
    recoveryCounter = 0
    lastRecoveryPosition = null
    val start = System.currentTimeMillis()
    incrementalRecover

    store(new AddTrace.Bean().setMessage("RECOVERED"), ^ {
      // Rollback any batches that did not complete.
      batches.keysIterator.foreach {
        batch =>
          rollback(batch, null)
      }

      val end = System.currentTimeMillis()
      info("Processed %d operations from the journal in %,.3f seconds.", recoveryCounter, ((end - start) / 1000.0f))
      onComplete.run
    })
  }


  /**
   * incrementally recovers the journal.  It can be run again and again
   * if the journal is being appended to.
   */
  def incrementalRecover(): Unit = {

    // Is this our first incremental recovery pass?
    if (lastRecoveryPosition == null) {
      if (databaseRootRecord.hasFirstBatchLocation) {
        // we have to start at the first in progress batch usually...
        nextRecoveryPosition = databaseRootRecord.getFirstBatchLocation
      } else {
        // but perhaps there were no batches in progress..
        if (databaseRootRecord.hasLastUpdateLocation) {
          // then we can just continue from the last update applied to the index
          lastRecoveryPosition = databaseRootRecord.getLastUpdateLocation
          nextRecoveryPosition = journal.getNextLocation(lastRecoveryPosition)
        } else {
          // no updates in the index?.. start from the first record in the journal.
          nextRecoveryPosition = journal.getNextLocation(null)
        }
      }
    } else {
      nextRecoveryPosition = journal.getNextLocation(lastRecoveryPosition)
    }

    try {
      recovering = true

      // Continue recovering until journal runs out of records.
      while (nextRecoveryPosition != null) {
        lastRecoveryPosition = nextRecoveryPosition
        recover(lastRecoveryPosition)
        nextRecoveryPosition = journal.getNextLocation(lastRecoveryPosition)
      }

    } finally {
      recovering = false
    }
  }

  /**
   * Recovers the logged record at the specified location.
   */
  def recover(location: Location): Unit = {
    var data = journal.read(location)

    val editor = data.bigEndianEditor
    val updateType = editor.readByte()
    val batch = editor.readInt()

    updateType match {
      case BEGIN => executeBegin(batch, location)
      case COMMIT => executeCommit(batch, null, location)
      case _ =>
        val update = decode(location, updateType, data)
        executeStore(batch, update, null, location)
    }

    recoveryCounter += 1
  }


  /////////////////////////////////////////////////////////////////////
  //
  // Methods for Journal access
  //
  /////////////////////////////////////////////////////////////////////

  private def append(data: Buffer)(cb: (Location) => Unit): Unit = {
    benchmarkLatency { done =>
      journal.write(data, new JournalCallback() {
        def success(location: Location) = {
          done("journal append")
          cb(location)
          done("journal append + index update")
        }
      })
      done("journal enqueue")
    }
  }

  /**
   */
  def benchmarkLatency[R](func: (String=>Unit)=>R ):R = {
    val start = System.nanoTime
    func { label=>
      var end = System.nanoTime
      warn("latencey: %s is %,.3f ms", label, ( (end - start).toFloat / TimeUnit.SECONDS.toMillis(1)))
    }
  }

  def read(location: Location) = journal.read(location)

  /////////////////////////////////////////////////////////////////////
  //
  // Methods that execute updates stored in the journal by indexing them
  // Used both in normal operation and durring recovery.
  //
  /////////////////////////////////////////////////////////////////////

  private def executeBegin(batch: Int, location: Location): Unit = {
    assert(batches.get(batch).isEmpty)
    batches.put(batch, (location, ListBuffer()))
  }

  private def executeCommit(batch: Int, onComplete: Runnable, location: Location): Unit = {
    // apply all the updates in the batch as a single unit of work.
    batches.remove(batch) match {
      case Some((_, updates)) =>
        // When recovering.. we only want to redo updates that committed
        // after the last update location.
        if (!recovering || isAfterLastUpdateLocation(location)) {
          withTx { tx =>
            // index the updates
              updates.foreach {
                update =>
                  index(tx, update.update, update.location)
              }
              updateLocations(tx, location)
          }
        }
      case None =>
        // when recovering..  we are more lax due recovery starting
        // in the middle of a stream of in progress batches
        assert(recovering)
    }
    if (onComplete != null) {
      onComplete.run
    }
  }

  private def executeRollback(batch: Int, onComplete: Runnable, location: Location): Unit = {
    // apply all the updates in the batch as a single unit of work.
    batches.remove(batch) match {
      case Some((_, _)) =>
        if (!recovering || isAfterLastUpdateLocation(location)) {
          withTx { tx =>
              updateLocations(tx, location)
          }
        }
      case None =>
        // when recovering..  we are more lax due recovery starting
        // in the middle of a stream of in progress batches
        assert(recovering)
    }
    if (onComplete != null) {
      onComplete.run
    }
  }

  private def executeStore(batch: Int, update: TypeCreatable, onComplete: Runnable, location: Location): Unit = {
    if (batch == -1) {
      // update is not part of the batch..

      // When recovering.. we only want to redo updates that happen
      // after the last update location.
      if (!recovering || isAfterLastUpdateLocation(location)) {
        withTx { tx =>
          // index the update now.
            index(tx, update, location)
            updateLocations(tx, location)
        }
      }

      if (onComplete != null) {
        onComplete.run
      }
    } else {

      // only the commit/rollback in batch can have an onCompelte handler
      assert(onComplete == null)

      // if the update was part of a batch don't apply till the batch is committed.
      batches.get(batch) match {
        case Some((_, updates)) =>
          updates += Update(update, location)
        case None =>
          // when recovering..  we are more lax due recovery starting
          // in the middle of a stream of in progress batches
          assert(recovering)
      }
    }
  }


  private def index(tx: Transaction, update: TypeCreatable, location: Location): Unit = {

    object Process extends TxHelper(tx) {
      import JavaConversions._

      def apply(x: AddMessage.Getter): Unit = {

        val messageKey = x.getMessageKey()
        if (messageKey > databaseRootRecord.getLastMessageKey) {
          databaseRootRecord.setLastMessageKey(messageKey)
        }

        val prevLocation = messageKeyIndex.put(messageKey, location)
        if (prevLocation != null) {
          // Message existed.. undo the index update we just did. Chances
          // are it's a transaction replay.
          messageKeyIndex.put(messageKey, prevLocation)
          if (location == prevLocation) {
            warn("Message replay detected for: %d", messageKey)
          } else {
            error("Message replay with different location for: %d", messageKey)
          }
        } else {
          val fileId:jl.Integer = location.getDataFileId()
          addAndGet(dataFileRefIndex, fileId, 1)
        }
      }

      def removeMessage(key:Long) = {
        val location = messageKeyIndex.remove(key)
        if (location != null) {
          val fileId:jl.Integer = location.getDataFileId()
          addAndGet(dataFileRefIndex, fileId, -1)
        } else {
          error("Cannot remove message, it did not exist: %d", key)
        }
      }

      def apply(x: AddQueue.Getter): Unit = {
        val queueKey = x.getKey
        if (queueIndex.get(queueKey) == null) {

          if (queueKey > databaseRootRecord.getLastQueueKey) {
            databaseRootRecord.setLastQueueKey(queueKey)
          }

          val queueRecord = new QueueRootRecord.Bean
          queueRecord.setEntryIndexPage(alloc(QUEUE_ENTRY_INDEX_FACTORY))
          queueRecord.setTrackingIndexPage(alloc(QUEUE_TRACKING_INDEX_FACTORY))
          queueRecord.setInfo(x)
          queueIndex.put(queueKey, queueRecord.freeze)
        }
      }

      def apply(x: RemoveQueue.Getter): Unit = {
        val queueRecord = queueIndex.remove(x.getKey)
        if (queueRecord != null) {
          val trackingIndex = queueTrackingIndex(queueRecord)
          val entryIndex = queueEntryIndex(queueRecord)

          trackingIndex.iterator.map { entry=>
            val messageKey = entry.getKey
            if( addAndGet(messageRefsIndex, messageKey, -1) == 0 ) {
              // message is no longer referenced.. we can remove it..
              removeMessage(messageKey.longValue)
            }
          }

          entryIndex.destroy
          trackingIndex.destroy
        }
      }

      def apply(x: AddQueueEntry.Getter): Unit = {
        val queueKey = x.getQueueKey
        val queueRecord = queueIndex.get(queueKey)
        if (queueRecord != null) {
          val trackingIndex = queueTrackingIndex(queueRecord)
          val entryIndex = queueEntryIndex(queueRecord)

          // a message can only appear once in a queue (for now).. perhaps we should
          // relax this constraint.
          val messageKey = x.getMessageKey
          val queueSeq = x.getQueueSeq

          val existing = trackingIndex.put(messageKey, queueSeq)
          if (existing == null) {
            val previous = entryIndex.put(queueSeq, x.freeze)
            if (previous == null) {

              val queueRecordUpdate = queueRecord.copy
              queueRecordUpdate.setCount(queueRecord.getCount + 1)
              queueRecordUpdate.setSize(queueRecord.getSize + x.getSize)
              queueIndex.put(queueKey, queueRecordUpdate.freeze)

              addAndGet(messageRefsIndex, new jl.Long(messageKey), 1)
            } else {
              error("Duplicate queue entry seq %d", x.getQueueSeq)
            }
          } else {
            error("Duplicate queue entry message %d", x.getMessageKey)
          }
        } else {
          error("Queue not found: %d", x.getQueueKey)
        }
      }

      def apply(x: RemoveQueueEntry.Getter): Unit = {
        val queueKey = x.getQueueKey
        val queueRecord = queueIndex.get(queueKey)
        if (queueRecord != null) {
          val trackingIndex = queueTrackingIndex(queueRecord)
          val entryIndex = queueEntryIndex(queueRecord)

          val queueSeq = x.getQueueSeq
          val queueEntry = entryIndex.remove(queueSeq)
          if (queueEntry != null) {
            val messageKey = queueEntry.getMessageKey
            val existing = trackingIndex.remove(messageKey)
            if (existing == null) {
              error("Tracking entry not found for message %d", queueEntry.getMessageKey)
            }
            if( addAndGet(messageRefsIndex, new jl.Long(messageKey), -1) == 0 ) {
              // message is no longer referenced.. we can remove it..
              removeMessage(messageKey)
            }
          } else {
            error("Queue entry not found for seq %d", x.getQueueSeq)
          }
        } else {
          error("Queue not found: %d", x.getQueueKey)
        }
      }

      def apply(x: Purge.Getter): Unit = {

        // Remove all the queues...
        queueIndex.iterator.map {
          entry =>
            entry.getKey
        }.foreach {
          key =>
            apply(new RemoveQueue.Bean().setKey(key.intValue))
        }

        // Remove stored messages...
        messageKeyIndex.clear
        messageRefsIndex.clear
        dataFileRefIndex.clear
        databaseRootRecord.setLastMessageKey(0)

        cleanup(tx);
        info("Store purged.");
      }

      def apply(x: AddTrace.Getter): Unit = {
        // trace messages are informational messages in the journal used to log
        // historical info about store state.  They don't update the indexes.
      }
    }

    update match {
      case x: AddMessage.Getter =>
        Process(x)
      case x: AddQueueEntry.Getter =>
        Process(x)
      case x: RemoveQueueEntry.Getter =>
        Process(x)

      case x: AddQueue.Getter =>
        Process(x)
      case x: RemoveQueue.Getter =>
        Process(x)

      case x: AddTrace.Getter =>
        Process(x)
      case x: Purge.Getter =>
        Process(x)

      case x: AddSubscription.Getter =>
      case x: RemoveSubscription.Getter =>

      case x: AddMap.Getter =>
      case x: RemoveMap.Getter =>
      case x: PutMapEntry.Getter =>
      case x: RemoveMapEntry.Getter =>

      case x: OpenStream.Getter =>
      case x: WriteStream.Getter =>
      case x: CloseStream.Getter =>
      case x: RemoveStream.Getter =>
    }
  }


  /////////////////////////////////////////////////////////////////////
  //
  // Periodic Maintance
  //
  /////////////////////////////////////////////////////////////////////

  def schedualFlush(version:Int): Unit = {
    def try_flush() = {
      if (version == schedual_version.get) {
        hawtDBStore.executor_pool {
          flush
          schedualFlush(version)
        }
      }
    }
    dispatchQueue.dispatchAfter(config.indexFlushInterval, TimeUnit.MILLISECONDS, ^ {try_flush})
  }

  def flush() = {
    val start = System.currentTimeMillis()
    pageFile.flush
    val end = System.currentTimeMillis()
    if (end - start > 1000) {
      warn("Index flush latency: %,.3f seconds", ((end - start) / 1000.0f))
    }
  }

  def schedualCleanup(version:Int): Unit = {
    def try_cleanup() = {
      if (version == schedual_version.get) {
        hawtDBStore.executor_pool {
          withTx {tx =>
            cleanup(tx)
          }
          schedualCleanup(version)
        }
      }
    }
    dispatchQueue.dispatchAfter(config.cleanupInterval, TimeUnit.MILLISECONDS, ^ {try_cleanup})
  }

  /**
   * @param tx
   * @throws IOException
   */
  def cleanup(tx:Transaction) = {
    val helper = new TxHelper(tx)
    import JavaConversions._
    import helper._

    debug("Cleanup started.")
    val gcCandidateSet = new TreeSet[jl.Integer](journal.getFileMap().keySet())

    // Don't cleanup locked data files
    if (lockedDatatFiles != null) {
      gcCandidateSet.removeAll(lockedDatatFiles)
    }

    // Don't GC files that we will need for recovery..
    val upto = if (databaseRootRecord.hasFirstBatchLocation) {
      Some(databaseRootRecord.getFirstBatchLocation.getDataFileId)
    } else {
      if (databaseRootRecord.hasLastUpdateLocation) {
        Some(databaseRootRecord.getLastUpdateLocation.getDataFileId)
      } else {
        None
      }
    }

    upto match {
      case Some(dataFile) =>
        var done = false
        while (!done && !gcCandidateSet.isEmpty()) {
          val last = gcCandidateSet.last()
          if (last.intValue >= dataFile) {
            gcCandidateSet.remove(last)
          } else {
            done = true
          }
        }

      case None =>
    }

    if (!gcCandidateSet.isEmpty() ) {
      dataFileRefIndex.iterator.foreach { entry =>
        gcCandidateSet.remove(entry.getKey)
      }
      if (!gcCandidateSet.isEmpty()) {
        debug("Cleanup removing the data files: %s", gcCandidateSet)
        journal.removeDataFiles(gcCandidateSet)
      }
    }
    debug("Cleanup done.")
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Helper Methods / Classes
  //
  /////////////////////////////////////////////////////////////////////

  private case class Update(update: TypeCreatable, location: Location)

  private class TxHelper(private val _tx: Transaction) {
    lazy val queueIndex = QUEUE_INDEX_FACTORY.open(_tx, databaseRootRecord.getQueueIndexPage)
    lazy val dataFileRefIndex = DATA_FILE_REF_INDEX_FACTORY.open(_tx, databaseRootRecord.getDataFileRefIndexPage)
    lazy val messageKeyIndex = MESSAGE_KEY_INDEX_FACTORY.open(_tx, databaseRootRecord.getMessageKeyIndexPage)
    lazy val messageRefsIndex = MESSAGE_REFS_INDEX_FACTORY.open(_tx, databaseRootRecord.getMessageRefsIndexPage)
    lazy val subscriptionIndex = SUBSCRIPTIONS_INDEX_FACTORY.open(_tx, databaseRootRecord.getSubscriptionIndexPage)

    def addAndGet[K](index:SortedIndex[K, jl.Integer], key:K, amount:Int):Int = {
      var counter = index.get(key)
      if( counter == null ) {
        if( amount!=0 ) {
          index.put(key, amount)
        }
        amount
      } else {
        val update = counter.intValue + amount
        if( update == 0 ) {
          index.remove(key)
        } else {
          index.put(key, update)
        }
        update
      }
    }

    def queueEntryIndex(root: QueueRootRecord.Getter) = QUEUE_ENTRY_INDEX_FACTORY.open(_tx, root.getEntryIndexPage)

    def queueTrackingIndex(root: QueueRootRecord.Getter) = QUEUE_TRACKING_INDEX_FACTORY.open(_tx, root.getTrackingIndexPage)

    def alloc(factory: IndexFactory[_, _]) = {
      val rc = _tx.alloc
      factory.create(_tx, rc)
      rc
    }
  }

  private def withTx[T](func: (Transaction) => T): T = {
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

  private def isAfterLastUpdateLocation(location: Location) = {
    val lastUpdate: Location = databaseRootRecord.getLastUpdateLocation
    lastUpdate.compareTo(location) < 0
  }

  private def updateLocations(tx: Transaction, lastUpdate: Location): Unit = {
    databaseRootRecord.setLastUpdateLocation(lastUpdate)
    if (batches.isEmpty) {
      databaseRootRecord.clearFirstBatchLocation
    } else {
      databaseRootRecord.setFirstBatchLocation(batches.head._2._1)
    }
    tx.put(DATABASE_ROOT_RECORD_ACCESSOR, 0, databaseRootRecord.freeze)
    databaseRootRecord = databaseRootRecord.copy
  }
}