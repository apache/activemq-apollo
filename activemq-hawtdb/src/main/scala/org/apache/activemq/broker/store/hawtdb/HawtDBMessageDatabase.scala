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

import org.apache.activemq.apollo.broker.{Log, Logging, BaseService}
import collection.Seq
import org.fusesource.hawtdispatch.ScalaDispatch._
import org.apache.activemq.broker.store.{StoreTransaction, StoredMessage, StoredQueue, BrokerDatabase}
import org.fusesource.hawtdispatch.BaseRetained
import java.io.{IOException, File}
import org.apache.activemq.util.LockFile
import java.util.concurrent.TimeUnit
import org.fusesource.hawtdb.internal.journal.{Location, Journal}
import java.util.HashSet
import org.fusesource.hawtdb.api.{Transaction, TxPageFileFactory}
import java.util.concurrent.atomic.AtomicLong

object HawtDBMessageDatabase extends Log {
  val DATABASE_LOCKED_WAIT_DELAY = 10 * 1000;
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class HawtDBMessageDatabase extends BaseService with Logging with BrokerDatabase {
  import HawtDBMessageDatabase._
  override protected def log = HawtDBMessageDatabase

  val dispatchQueue = createQueue("hawtdb message database")

  val pageFileFactory = new TxPageFileFactory()

  def pageFile = pageFileFactory.getTxPageFile

  var journal: Journal = null

  var rootEntity = new RootEntity();
  var lockFile: LockFile = null


  var failIfDatabaseIsLocked = false
  var deleteAllMessages = false
  var directory: File = null

  var checkpointInterval = 5 * 1000L
  var cleanupInterval = 30 * 1000L

  var nextRecoveryPosition: Location = null
  var lastRecoveryPosition: Location = null
  val trackingGen = new AtomicLong(0);

  val journalFilesBeingReplicated = new HashSet[Integer] ()
  var recovering = false
  var journalMaxFileLength = 1024 * 1024 * 20


  def lock(func: => Unit) = {
    var lockFileName = new File(directory, "lock");
    lockFile = new LockFile(lockFileName, true);
    if (failIfDatabaseIsLocked) {
      lockFile.lock();
      func
    } else {
      def tryLock:Unit = {
        try {
          lockFile.lock();
          func
        } catch {
          case e: IOException =>
            info("Database %s is locked... waiting %d seconds for the database to be unlocked.", lockFileName, (DATABASE_LOCKED_WAIT_DELAY / 1000));
            dispatchQueue.dispatchAfter(DATABASE_LOCKED_WAIT_DELAY, TimeUnit.MILLISECONDS, ^ {tryLock})
        }
      }
      tryLock
    }
  }

  def getJournal() = {
    if (journal == null) {
      journal = new Journal();
      journal.setDirectory(directory);
      journal.setMaxFileLength(journalMaxFileLength);
    }
    journal
  }

  def execute[T](closure: (Transaction)=> T) {
    val tx = pageFile.tx();
    var committed = false;
    try {
      val rc = closure(tx)
      tx.commit();
      committed = true;
      rc
    } finally {
      if (!committed) {
        tx.rollback();
      }
    }
  }

  protected def _start(onCompleted: Runnable) = {
    if (directory == null) {
      throw new IllegalArgumentException("The directory property must be set.");
    }

    lock {

      pageFileFactory.setFile(new File(directory, "db"));
      pageFileFactory.setDrainOnClose(false);
      pageFileFactory.setSync(true);
      pageFileFactory.setUseWorkerThread(true);

      if (deleteAllMessages) {
        getJournal().start();
        journal.delete();
        journal.close();
        journal = null;
        pageFileFactory.getFile().delete();
        rootEntity = new RootEntity();
        info("Persistence store purged.");
        deleteAllMessages = false;
      }

      getJournal().start();
      pageFileFactory.open();

      execute { tx =>
        if (!tx.allocator().isAllocated(0)) {
          rootEntity.allocate(tx);
        }
        rootEntity.load(tx);
      }
      pageFile.flush();
      onCompleted.run
    }



    //    checkpointThread = new Thread("ActiveMQ Journal Checkpoint Worker") {
    //        public void run() {
    //            try {
    //                long lastCleanup = System.currentTimeMillis();
    //                long lastCheckpoint = System.currentTimeMillis();
    //
    //                // Sleep for a short time so we can periodically check
    //                // to see if we need to exit this thread.
    //                long sleepTime = Math.min(checkpointInterval, 500);
    //                while (opened.get()) {
    //                    Thread.sleep(sleepTime);
    //                    long now = System.currentTimeMillis();
    //                    if (now - lastCleanup >= cleanupInterval) {
    //                        checkpointCleanup(true);
    //                        lastCleanup = now;
    //                        lastCheckpoint = now;
    //                    } else if (now - lastCheckpoint >= checkpointInterval) {
    //                        checkpointCleanup(false);
    //                        lastCheckpoint = now;
    //                    }
    //                }
    //            } catch (InterruptedException e) {
    //                // Looks like someone really wants us to exit this
    //                // thread...
    //            }
    //        }
    //    };
    //    checkpointThread.start();

    //    recover();
    //    trackingGen.set(rootEntity.getLastMessageTracking() + 1);

  }


  protected def _stop(onCompleted: Runnable) = {
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the BrokerDatabase interface
  //
  /////////////////////////////////////////////////////////////////////

  def createStoreTransaction() = new HawtDBStoreTransaction

  def loadMessage(id: Long)(cb: (Option[StoredMessage]) => Unit) = {}

  def listQueues(cb: (Seq[Long]) => Unit) = {}

  def getQueueInfo(id: Long)(cb: (Option[StoredQueue]) => Unit) = {}

  def flushMessage(id: Long)(cb: => Unit) = {}

  def addQueue(record: StoredQueue)(cb: (Option[Long]) => Unit) = {}


  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the StoreTransaction interface
  //
  /////////////////////////////////////////////////////////////////////
  class HawtDBStoreTransaction extends BaseRetained with StoreTransaction {
    def store(delivery: StoredMessage) = {}

    def enqueue(queue: Long, seq: Long, msg: Long) = {}

    def dequeue(queue: Long, seq: Long, msg: Long) = {}

  }


}