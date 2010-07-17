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

import org.apache.activemq.apollo.dto.{StoreStatusDTO, StoreDTO}
import org.apache.activemq.apollo.util._
import ReporterLevel._

/**
 * <p>
 * The Store is service which offers asynchronous persistence services
 * to a Broker.
 * </p>
 *
 *  @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Store extends ServiceTrait {

  def storeStatusDTO(callback:(StoreStatusDTO)=>Unit)

  /**
   * @returns true if the store implementation can handle accepting
   *          MessageRecords with DirectBuffers in them.
   */
  def supportsDirectBuffers() = false

  /**
   * Creates a store uow which is used to perform persistent
   * operations as unit of work.
   */
  def createStoreUOW():StoreUOW

  /**
   * Supplies configuration data to the Store.  This will be called
   * before the store is started, but may also occur after the the Store 
   * is started.
   */
  def configure(config: StoreDTO, reporter:Reporter):Unit

  /**
   * Removes all previously stored data.
   */
  def purge(callback: =>Unit):Unit

  /**
   * Ges the last queue key identifier stored.
   */
  def getLastQueueKey(callback:(Option[Long])=>Unit):Unit

  /**
   * Adds a queue.
   * 
   * This method auto generates and assigns the key field of the queue record and
   * returns true if it succeeded.
   */
  def addQueue(record:QueueRecord)(callback:(Boolean)=>Unit):Unit

  /**
   * Removes a queue. Success is reported via the callback.
   */
  def removeQueue(queueKey:Long)(callback:(Boolean)=>Unit):Unit

  /**
   * Loads the queue information for a given queue key.
   */
  def getQueue(queueKey:Long)(callback:(Option[QueueRecord])=>Unit )

  /**
   * Gets a listing of all queue entry sequences previously added
   * and reports them to the callback.
   */
  def listQueues(callback: (Seq[Long])=>Unit )

  /**
   * Groups all the entries in the specified queue into ranges containing up limit entries
   * big and returns those ranges.  Allows you to incrementally, load all the entries in
   * a queue.
   */
  def listQueueEntryRanges(queueKey:Long, limit:Int)(callback:(Seq[QueueEntryRange])=>Unit )

  /**
   * Loads all the queue entry records for the given queue id between the first and last provided
   * queue sequences (inclusive).
   */
  def listQueueEntries(queueKey:Long, firstSeq:Long, lastSeq:Long)(callback:(Seq[QueueEntryRecord])=>Unit )

  /**
   * Removes a the delivery associated with the provided from any
   * internal buffers/caches.  The callback is executed once, the message is
   * no longer buffered.
   */
  def flushMessage(messageKey:Long)(callback: =>Unit)

  /**
   * Loads a delivery with the associated id from persistent storage.
   */
  def loadMessage(messageKey:Long)(callback:(Option[MessageRecord])=>Unit )

}

/**
 * Optional interface that stores can implement to give protocols direct access to the file system
 * for them to be able to do
 */

