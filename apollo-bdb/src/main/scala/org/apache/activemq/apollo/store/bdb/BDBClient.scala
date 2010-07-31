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
package org.apache.activemq.apollo.store.bdb

import dto.BDBStoreDTO
import java.{lang=>jl}
import java.{util=>ju}

import org.apache.activemq.apollo.dto.HawtDBStoreDTO
import java.util.concurrent.atomic.AtomicInteger
import collection.mutable.ListBuffer
import org.apache.activemq.apollo.store._
import org.apache.activemq.apollo.util._
import com.sleepycat.je._

object BDBClient extends Log
/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class BDBClient(store: BDBStore) extends DispatchLogging {

  import HelperTrait._

  override def log: Log = BDBClient

  def dispatchQueue = store.dispatchQueue

  /////////////////////////////////////////////////////////////////////
  //
  // Helpers
  //
  /////////////////////////////////////////////////////////////////////

  private def directory = config.directory

  /////////////////////////////////////////////////////////////////////
  //
  // Public interface used by the BDBStore
  //
  /////////////////////////////////////////////////////////////////////

  var config: BDBStoreDTO = null

  var environment:Environment = _

  def start() = {
    val env_config = new EnvironmentConfig();
    env_config.setAllowCreate(true);
    env_config.setTransactional(true);
    env_config.setTxnSerializableIsolation(false)

    directory.mkdirs
    environment = new Environment(directory, env_config);

    with_ctx { ctx=>
      import ctx._
      messages_db
      message_refs_db
      queues_db
    }
  }

  def stop() = {
    environment.close
  }

  case class TxContext(tx:Transaction) {

    def with_entries_db[T](queueKey:Long)(func: (Database) => T): T = {
      val db = environment.openDatabase(tx, entries_db_name(queueKey), long_key_conf)
      try {
        func(db)
      } finally {
        db.close
      }
    }


    private var _messages_db:Database = _
    def messages_db:Database = {
      if( _messages_db==null ) {
        _messages_db = environment.openDatabase(tx, "messages", long_key_conf)
      }
      _messages_db
    }


    private var _message_refs_db:Database = _
    def message_refs_db:Database = {
      if( _message_refs_db==null ) {
        _message_refs_db = environment.openDatabase(tx, "message_refs", long_key_conf)
      }
      _message_refs_db
    }

    private var _queues_db:Database = _
    def queues_db:Database = {
      if( _queues_db==null ) {
        _queues_db = environment.openDatabase(tx, "queues", long_key_conf)
      }
      _queues_db
    }

    def close(ok:Boolean) = {
      if( _messages_db!=null ) {
        _messages_db.close
      }
      if( _message_refs_db!=null ) {
        _message_refs_db.close
      }
      if( _queues_db!=null ) {
        _queues_db.close
      }

      if(ok){
        tx.commit
      } else {
        tx.abort
      }
    }

  }


  def with_ctx[T](func: (TxContext) => T): T = {
    val ctx = TxContext(environment.beginTransaction(null, null));
    var ok = false
    try {
      val rc = func(ctx)
      ok = true
      rc
    } finally {
      ctx.close(ok)
    }
  }


  def purge() = {

    with_ctx { ctx=>
      import ctx._


      def remove_db(name: String): Unit = {
        try {
          environment.removeDatabase(tx, name)
        } catch {
          case x: DatabaseNotFoundException =>
        }
      }

      listQueues.foreach{ queueKey=>
        val name = entries_db_name(queueKey)
        remove_db(name)
      }
      remove_db("messages")
      remove_db("message_refs")
      remove_db("queues")

      messages_db
      message_refs_db
      queues_db
    }

  }

  def add_and_get[K](db:Database, key:DatabaseEntry, amount:Int, tx:Transaction):Int = {
    db.get(tx, key) match {
      case None =>
        if( amount!=0 ) {
          db.put(tx, key, amount)
        }
        amount

      case Some(value) =>
        val counter:Int = value
        val update = counter + amount
        if( update == 0 ) {
          db.delete(tx, key)
        } else {
          db.put(tx, key, update)
        }
        update
    }
  }

  def addQueue(record: QueueRecord, callback:Runnable) = {
    with_ctx { ctx=>
      import ctx._
      queues_db.put(tx, record.key, record)
      with_entries_db(record.key) { entriesdb=> 
      }
    }
    callback.run
  }

  def removeQueue(queueKey: Long, callback:Runnable) = {
    with_ctx { ctx=>
      import ctx._

      queues_db.delete(tx, queueKey)
      with_entries_db(queueKey) { entries_db=>

        entries_db.cursor(tx) { (key,value)=>
          val queueEntry:QueueEntryRecord = value
          if( add_and_get(message_refs_db, queueEntry.messageKey, -1, tx)==0 ) {
            messages_db.delete(tx, queueEntry.messageKey)
          }
          true // keep cursoring..
        }

      }

      environment.removeDatabase(tx, entries_db_name(queueKey))
    }
    callback.run
  }

  def store(uows: Seq[BDBStore#DelayableUOW], callback:Runnable) {
    with_ctx { ctx=>
      import ctx._

      uows.foreach { uow =>
          uow.actions.foreach {
            case (msg, action) =>

              if (action.messageRecord != null) {
                messages_db.put(tx, action.messageRecord.key, action.messageRecord)
              }

              action.enqueues.foreach { queueEntry =>
                with_entries_db(queueEntry.queueKey) { entries_db=>
                  entries_db.put(tx, queueEntry.queueSeq, queueEntry)
                  add_and_get(message_refs_db, queueEntry.messageKey, 1, tx)
                }
              }

              action.dequeues.foreach { queueEntry =>
                with_entries_db(queueEntry.queueKey) { entries_db=>
                  entries_db.delete(tx, queueEntry.queueSeq)
                  if( add_and_get(message_refs_db, queueEntry.messageKey, -1, tx)==0 ) {
                    messages_db.delete(tx, queueEntry.messageKey)
                  }
                }
              }
          }
      }
    }
    callback.run
  }

  def listQueues: Seq[Long] = {
    val rc = ListBuffer[Long]()
    with_ctx { ctx=>
      import ctx._

      queues_db.cursor(tx) { (key, _) =>
        rc += key
        true // to continue cursoring.
      }
    }

    rc
  }

  def getQueue(queueKey: Long): Option[QueueRecord] = {
    with_ctx { ctx=>
      import ctx._
      queues_db.get(tx, to_DatabaseEntry(queueKey)).map( x=> to_QueueRecord(x)  )
    }
  }

  def listQueueEntryGroups(queueKey: Long, limit: Int) : Seq[QueueEntryRange] = {
    var rc = ListBuffer[QueueEntryRange]()
    with_ctx { ctx=>
      import ctx._

      with_entries_db(queueKey) { entries_db=>

        var group:QueueEntryRange = null

        entries_db.cursor(tx) { (key, value) =>

          if( group == null ) {
            group = new QueueEntryRange
            group.firstQueueSeq = key
          }

          val entry:QueueEntryRecord = value

          group.lastQueueSeq = key
          group.count += 1
          group.size += entry.size

          if( group.count == limit) {
            rc += group
            group = null
          }

          true // to continue cursoring.
        }

        if( group!=null ) {
          rc += group
        }

      }
    }
    rc
  }

  def getQueueEntries(queueKey: Long, firstSeq:Long, lastSeq:Long): Seq[QueueEntryRecord] = {
    var rc = ListBuffer[QueueEntryRecord]()
    with_ctx { ctx=>
      import ctx._

      with_entries_db(queueKey) { entries_db=>
        entries_db.cursor_from(tx, to_DatabaseEntry(firstSeq)) { (key, value) =>
          val queueSeq:Long = key
          val entry:QueueEntryRecord = value
          rc += entry
          queueSeq < lastSeq
        }
      }
    }
    rc
  }

  val metric_load_from_index_counter = new TimeCounter
  var metric_load_from_index = metric_load_from_index_counter(false)

  def loadMessages(requests: ListBuffer[(Long, (Option[MessageRecord])=>Unit)]) = {
    val records = with_ctx { ctx=>
      import ctx._

      requests.flatMap { case (messageKey, callback)=>
        val record = metric_load_from_index_counter.time {
          messages_db.get(tx, to_DatabaseEntry(messageKey)).map ( to_MessageRecord _ )
        }
        record match {
          case None =>
          debug("Message not indexed: %s", messageKey)
          callback(None)
          None
          case Some(x) => Some((record, callback))
        }
      }
    }

    records.foreach { case (record, callback)=>
      callback( record )
    }

  }


  def getLastMessageKey:Long = {
    with_ctx { ctx=>
      import ctx._

      messages_db.last_key(tx).map(to_long _).getOrElse(0)
    }
  }

  def getLastQueueKey:Long = {
    with_ctx { ctx=>
      import ctx._

      queues_db.last_key(tx).map(to_long _).getOrElse(0)
    }
  }

}