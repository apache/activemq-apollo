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

import collection.Seq
import org.fusesource.hawtdispatch.ScalaDispatch._
import org.fusesource.hawtdispatch.BaseRetained
import java.io.{IOException, File}
import org.apache.activemq.util.LockFile
import org.fusesource.hawtdb.internal.journal.{Location, Journal}
import java.util.HashSet
import org.fusesource.hawtdb.api.{Transaction, TxPageFileFactory}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, TimeUnit}
import org.apache.activemq.apollo.dto.HawtDBStoreDTO
import org.apache.activemq.apollo.broker._
import ReporterLevel._
import store.HawtDBManager
import org.apache.activemq.broker.store.{Store, StoreTransaction}
import org.apache.activemq.apollo.store.{QueueStatus, MessageRecord, QueueRecord}

object HawtDBStore extends Log {
  val DATABASE_LOCKED_WAIT_DELAY = 10 * 1000;

  /**
   * Creates a default a configuration object.
   */
  def default() = {
    val rc = new HawtDBStoreDTO
    rc.directory = new File("activemq-data")
    rc
  }

  /**
   * Validates a configuration object.
   */
  def validate(config: HawtDBStoreDTO, reporter:Reporter):ReporterLevel = {
     new Reporting(reporter) {
      if( config.directory == null ) {
        error("hawtdb store must be configured with a directroy.")
      }
    }.result
  }}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class HawtDBStore extends BaseService with Logging with Store {
  import HawtDBStore._
  override protected def log = HawtDBStore

  val dispatchQueue = createQueue("hawtdb message database")
  val writeQueue = Executors.newSingleThreadExecutor
  val readQueue = Executors.newCachedThreadPool
  var config: HawtDBStoreDTO  = default
  var manager:HawtDBManager = null

  /**
   * Validates and then applies the configuration.
   */
  def configure(config: HawtDBStoreDTO, reporter:Reporter) = ^{
    if ( validate(config, reporter) < ERROR ) {
      this.config = config
      if( serviceState.isStarted ) {
        // TODO: apply changes while he broker is running.
        reporter.report(WARN, "Updating the hawtdb configuration at runtime is not yet supported.  You must restart the broker for the change to take effect.")
      }
    }
  } |>>: dispatchQueue

  protected def _start(onCompleted: Runnable) = {
    writeQueue {
      manager = new HawtDBManager
      manager.setStoreDirectory(config.directory)
      manager.start()
      onCompleted.run
    }
  }

  protected def _stop(onCompleted: Runnable) = {
    writeQueue {
      manager.stop()
      onCompleted.run
    }
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the BrokerDatabase interface
  //
  /////////////////////////////////////////////////////////////////////

  def addQueue(record: QueueRecord)(cb: (Option[Long]) => Unit) = {}

  def getQueueStatus(id: Long)(cb: (Option[QueueStatus]) => Unit) = {}

  def listQueues(cb: (Seq[Long]) => Unit) = {}

  def loadMessage(id: Long)(cb: (Option[MessageRecord]) => Unit) = {}

  def flushMessage(id: Long)(cb: => Unit) = {}

  def createStoreTransaction() = new HawtDBStoreTransaction


  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the StoreTransaction interface
  //
  /////////////////////////////////////////////////////////////////////
  class HawtDBStoreTransaction extends BaseRetained with StoreTransaction {

    def store(delivery: MessageRecord) = {

    }

    def enqueue(queue: Long, seq: Long, msg: Long) = {}

    def dequeue(queue: Long, seq: Long, msg: Long) = {}

  }


}