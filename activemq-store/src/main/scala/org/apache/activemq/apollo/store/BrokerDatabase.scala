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

class StoredQueue {
  var id:Long = -1
  var name:AsciiBuffer = null
  var parent:AsciiBuffer = null
  var config:String = null
  var first:Long = -1
  var last:Long = -1
  var count:Int = 0
}

class StoredMessage {
  var id:Long = -1
  var protocol:AsciiBuffer = null
  var value:Buffer = null
  var size:Int = 0
}

/**
 * A StoreTransaction is used to perform persistent
 * operations as unit of work.
 *
 * The disposer assigned to the store transaction will
 * be executed once all associated persistent operations
 * have been persisted.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait StoreTransaction extends Retained {

  /**
   * Assigns the delivery a store id if it did not already
   * have one assigned.
   */
  def store(delivery:StoredMessage)

  /**
   * Adds a delivery to a specified queue at a the specified position in the queue.
   */
  def enqueue(queue:Long, seq:Long, msg:Long)

  /**
   * Removes a delivery from a specified queue at a the specified position in the queue.
   */
  def dequeue(queue:Long, seq:Long, msg:Long)

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait BrokerDatabase extends Service {


  /**
   * Stores a queue, calls back with a unquie id for the stored queue.
   */
  def addQueue(record:StoredQueue)(cb:(Option[Long])=>Unit):Unit

  /**
   * Loads the queue information for a given queue id.
   */
  def getQueueInfo(id:Long)(cb:(Option[StoredQueue])=>Unit )

  /**
   * gets a listing of all queues previously added.
   */
  def listQueues(cb: (Seq[Long])=>Unit )

  /**
   * Removes a the delivery associated with the provided from any
   * internal buffers/caches.  The callback is executed once, the message is
   * no longer buffered.
   */
  def flushDelivery(id:Long)(cb: =>Unit)

  /**
   * Loads a delivery with the associated id from persistent storage.
   */
  def loadDelivery(id:Long)(cb:(Option[StoredMessage])=>Unit )

  /**
   * Creates a StoreTransaction which is used to perform persistent
   * operations as unit of work.
   */
  def createStoreTransaction():StoreTransaction

}

