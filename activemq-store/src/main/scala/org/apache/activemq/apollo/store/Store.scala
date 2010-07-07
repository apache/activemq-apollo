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

import _root_.java.lang.{String}
import org.fusesource.hawtbuf._
import org.apache.activemq.Service
import org.fusesource.hawtdispatch.{Retained}
import org.apache.activemq.apollo.store._
import org.apache.activemq.apollo.broker.Reporter
import org.apache.activemq.apollo.dto.StoreDTO

/**
 * A store batch is used to perform persistent
 * operations as a unit of work.
 * 
 * The batch implements the Retained interface and is
 * thread safe.  Once the batch is no longer retained,
 * the unit of work is executed.  
 *
 * The disposer assigned to the batch will
 * be executed once the unit of work is persisted
 * or it has been negated by subsequent storage
 * operations.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait StoreBatch extends Retained {

  /**
   * Stores a message.  Messages a reference counted, so make sure you also 
   * enqueue it to queue if you don't want it to be discarded right away.
   * 
   * This method auto generates and assigns the key field of the message record and
   * returns it.
   */
  def store(message:MessageRecord):Long

  /**
   * Adds a queue entry
   */
  def enqueue(entry:QueueEntryRecord)

  /**
   * Removes a queue entry
   */
  def dequeue(entry:QueueEntryRecord)


  /**
   * Causes the batch to flush eagerly, callback is called once flushed.
   */
  def eagerFlush(callback: Runnable)

}

/**
 *  @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Store extends Service {

  /**
   * Creates a store batch which is used to perform persistent
   * operations as unit of work.
   */
  def createStoreBatch():StoreBatch

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
   * Adds a queue.
   * 
   * This method auto generates and assigns the key field of the queue record and
   * returns it via the callback.
   */
  def addQueue(record:QueueRecord)(callback:(Option[Long])=>Unit):Unit

  /**
   * Removes a queue. Success is reported via the callback.
   */
  def removeQueue(queueKey:Long)(callback:(Boolean)=>Unit):Unit

  /**
   * Loads the queue information for a given queue key.
   */
  def getQueueStatus(queueKey:Long)(callback:(Option[QueueStatus])=>Unit )

  /**
   * Gets a listing of all queue entry sequences previously added
   * and reports them to the callback.
   */
  def listQueues(callback: (Seq[Long])=>Unit )

  /**
   * Loads the queue information for a given queue id.
   */
  def listQueueEntries(queueKey:Long)(callback:(Seq[QueueEntryRecord])=>Unit )

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

