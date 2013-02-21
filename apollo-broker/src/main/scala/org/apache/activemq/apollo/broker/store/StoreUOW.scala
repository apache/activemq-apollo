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
package org.apache.activemq.apollo.broker.store

import org.fusesource.hawtdispatch.{Retained}
import org.apache.activemq.apollo.broker.store._
import org.fusesource.hawtbuf.Buffer

/**
 * A store uow is used to perform persistent
 * operations as a unit of work.
 *
 * The uow implements the Retained interface and is
 * thread safe.  Once the uow is no longer retained,
 * the unit of work is executed.
 *
 * The disposer assigned to the uow will
 * be executed once the unit of work is persisted
 * or it has been negated by subsequent storage
 * operations.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait StoreUOW extends Retained {

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
   * Creates or updates a map entry.  Set value to null to
   * remove the entry.
   */
  def put(key:Buffer, value:Buffer)

  /**
   * Marks this uow as needing to be completed
   * as soon as possible.  If not called, the Store
   * implementation may delay completing the uow in
   * the hopes that a subsequent uow will cancel negate
   * all it operations and thus avoid the cost of the
   * persistence operations.
   */
  def complete_asap()

  /**
   * The specified callback is executed once the UOW
   * has written to disk and flushed of the application
   * buffers.
   */
  def on_flush(callback: (Boolean)=>Unit):Unit
  def on_flush(callback: =>Unit):Unit = on_flush((canceled)=>{callback})

  /**
   * The specified callback is executed once the UOW
   * has fully completed, that is it's been flushed and
   * and synced to disk.
   */
  def on_complete(callback: (Boolean)=>Unit):Unit
  def on_complete(callback: =>Unit):Unit = on_complete((canceled)=>{callback})

}
