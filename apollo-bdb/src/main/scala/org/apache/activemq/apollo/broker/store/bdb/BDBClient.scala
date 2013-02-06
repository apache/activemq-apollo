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
package org.apache.activemq.apollo.broker.store.bdb

import dto.BDBStoreDTO
import java.{lang=>jl}
import java.{util=>ju}

import collection.mutable.ListBuffer
import org.apache.activemq.apollo.broker.store._
import org.apache.activemq.apollo.util._
import java.io.{InputStream, OutputStream}
import com.sleepycat.je._
import org.fusesource.hawtbuf.Buffer
import FileSupport._
import org.apache.activemq.apollo.broker.store.bdb.HelperTrait.to_database_entry

object BDBClient extends Log {
  final val STORE_SCHEMA_PREFIX = "bdb_store:"
  final val STORE_SCHEMA_VERSION = 2
}
/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class BDBClient(store: BDBStore) {

  import HelperTrait._

  import BDBClient._

  def dispatchQueue = store.dispatch_queue

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

  var direct_buffer_allocator: FileDirectBufferAllocator = _

  def direct_buffer_file = {
    import FileSupport._
    config.directory / "dbuffer.dat"
  }

  def start() = {
    val env_config = new EnvironmentConfig();
    env_config.setAllowCreate(true);
    env_config.setTransactional(true);
    env_config.setTxnSerializableIsolation(false)

    directory.mkdirs

    val version_file = directory / "store-version.txt"
    if( version_file.exists( ) ) {
      val ver = try {
        var tmp: String = version_file.read_text().trim()
        if( tmp.startsWith(STORE_SCHEMA_PREFIX) ) {
          tmp.stripPrefix(STORE_SCHEMA_PREFIX).toInt
        } else {
          -1
        }
      } catch {
        case e:Throwable => throw new Exception("Unexpected version file format: "+version_file)
      }
      ver match {
        case STORE_SCHEMA_VERSION => // All is good.
        case _ => throw new Exception("Cannot open the store.  It's schema version is not supported.")
      }
    }
    version_file.write_text(STORE_SCHEMA_PREFIX+STORE_SCHEMA_VERSION)

    direct_buffer_allocator = new FileDirectBufferAllocator(direct_buffer_file)
    environment = new Environment(directory, env_config);

    with_ctx() { ctx=>
      import ctx._
      messages_db
      message_refs_db
      queues_db

      lobs_db.cursor(tx) { (_,value)=>
        val v = decode_lob_value(value)
        direct_buffer_allocator.alloc_at(v._1, v._2)
        true
      }
    }
  }

  def stop() = {
    environment.close
    direct_buffer_allocator.close
    direct_buffer_allocator = null
  }

  case class TxContext(tx:Transaction) {

    def with_entries_db[T](queue_key:Long)(func: (Database) => T): T = {
      val db = environment.openDatabase(tx, entries_db_name(queue_key), long_key_conf)
      try {
        func(db)
      } finally {
        db.close
      }
    }

    private var _entries_db:Database = _
    def entries_db:Database = {
      if( _entries_db==null ) {
        _entries_db = environment.openDatabase(tx, "entries", long_long_key_conf)
      }
      _entries_db
    }

    private var _messages_db:Database = _
    def messages_db:Database = {
      if( _messages_db==null ) {
        _messages_db = environment.openDatabase(tx, "messages", long_key_conf)
      }
      _messages_db
    }

    private var _lobs_db:Database = _
    def lobs_db:Database = {
      if( _lobs_db==null ) {
        _lobs_db = environment.openDatabase(tx, "lobs", long_key_conf)
      }
      _lobs_db
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

    private var _map_db:Database = _
    def map_db:Database = {
      if( _map_db==null ) {
        _map_db = environment.openDatabase(tx, "map", buffer_key_conf)
      }
      _map_db
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
      if( _entries_db!=null ) {
        _entries_db.close
      }
      if( _map_db!=null ) {
        _map_db.close
      }
      if( _lobs_db!=null ) {
        _lobs_db.close
      }

      if(ok){
        tx.commit
      } else {
        tx.abort
      }
    }

  }


  def with_ctx[T](sync:Boolean=true)(func: (TxContext) => T): T = {
    var error:Throwable = null
    var rc:Option[T] = None

    // We will loop until the tx succeeds.  Perhaps it's
    // failing due to a temporary condition like low disk space.
    while(!rc.isDefined) {


      val ctx = if(sync) {
        TxContext(environment.beginTransaction(null, null));
      } else {
        TxContext(environment.beginTransaction(null, new TransactionConfig().setDurability(Durability.COMMIT_NO_SYNC)))
      }

      try {
        rc = Some(func(ctx))
      } catch {
        case e:Throwable =>
          if( error==null ) {
            warn(e, "Message store transaction failed. Will keep retrying after every second.")
          }
          error = e
      } finally {
        ctx.close(rc.isDefined)
      }

      if (!rc.isDefined) {
        // We may need to give up if the store is being stopped.
        if ( !store.service_state.is_started ) {
          throw error
        }
        Thread.sleep(1000)
      }
    }

    if( error!=null ) {
      info("Store recovered from inital failure.")
    }
    rc.get
  }

  def purge() = {

    with_ctx() { ctx=>
      import ctx._


      def remove_db(name: String): Unit = {
        try {
          environment.removeDatabase(tx, name)
        } catch {
          case x: DatabaseNotFoundException =>
        }
      }

      listQueues.foreach{ queue_key=>
        val name = entries_db_name(queue_key)
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
    with_ctx() { ctx=>
      import ctx._
      queues_db.put(tx, record.key, record)
    }
    callback.run
  }

  def decrement_message_reference(ctx:TxContext, msg_key:Long) = {
    import ctx._
    if( add_and_get(message_refs_db, msg_key, -1, tx)==0 ) {
      messages_db.delete(tx, msg_key)
      lobs_db.get(tx, to_database_entry(msg_key)).foreach { v=>
        val location  = decode_lob_value(v)
        direct_buffer_allocator.free(location._1, location._2)
      }
      lobs_db.delete(tx, msg_key)
    }
  }

  def removeQueue(queue_key: Long, callback:Runnable) = {
    with_ctx() { ctx=>
      import ctx._

      queues_db.delete(tx, queue_key)

      entries_db.cursor_from(tx, (queue_key, 0L)) { (key,value)=>
        val current_key:(Long,Long)=key
        if( current_key._1 == queue_key ) {
          val queueEntry:QueueEntryRecord = value
          entries_db.delete(tx, key)
          decrement_message_reference(ctx, queueEntry.message_key)
          true // keep cursoring..
        } else {
          false
        }
      }
    }
    callback.run
  }

  def store(uows: Seq[BDBStore#DelayableUOW]) {
    val sync = uows.find( _.flush_sync ).isDefined
    with_ctx(sync) { ctx=>
      import ctx._
      var sync_lobs = false
      uows.foreach { uow =>

          for((key,value) <- uow.map_actions) {
            if( value==null ) {
              map_db.delete(tx, key)
            } else {
              map_db.put(tx, key, value)
            }
          }

          uow.actions.foreach {
            case (msg, action) =>

              val message_record = action.message_record
              if (message_record != null) {
                import PBSupport._

                val pb = if( message_record.direct_buffer != null ) {
                  val r = to_pb(action.message_record).copy
                  val buffer = direct_buffer_allocator.copy(message_record.direct_buffer)
                  r.setDirectOffset(buffer.offset)
                  r.setDirectSize(buffer.size)
                  lobs_db.put(tx, message_record.key, (buffer.offset, buffer.size))
                  sync_lobs = true
                  r.freeze
                } else {
                  to_pb(action.message_record)
                }

                messages_db.put(tx, action.message_record.key, pb.freeze)
              }

              action.enqueues.foreach { queueEntry =>
                entries_db.put(tx, (queueEntry.queue_key, queueEntry.entry_seq), queueEntry)
                add_and_get(message_refs_db, queueEntry.message_key, 1, tx)
              }

              action.dequeues.foreach { queueEntry =>
                entries_db.delete(tx, (queueEntry.queue_key, queueEntry.entry_seq))
                decrement_message_reference(ctx, queueEntry.message_key)
              }
          }
      }
      if( sync_lobs ) {
        direct_buffer_allocator.sync
      }
    }
  }

  def listQueues: Seq[Long] = {
    val rc = ListBuffer[Long]()
    with_ctx() { ctx=>
      import ctx._

      queues_db.cursor(tx) { (key, _) =>
        rc += key
        true // to continue cursoring.
      }
    }

    rc
  }

  def getQueue(queue_key: Long): Option[QueueRecord] = {
    with_ctx() { ctx=>
      import ctx._
      queues_db.get(tx, to_database_entry(queue_key)).map( x=> to_queue_record(x)  )
    }
  }

  def listQueueEntryGroups(queue_key: Long, limit: Int) : Seq[QueueEntryRange] = {
    var rc = ListBuffer[QueueEntryRange]()
    with_ctx() { ctx=>
      import ctx._
      var group:QueueEntryRange = null

      entries_db.cursor_from(tx, (queue_key, 0L)) { (key, value) =>
        val current_key:(Long,Long)= key
        if( current_key._1 == queue_key ) {
          if( group == null ) {
            group = new QueueEntryRange
            group.first_entry_seq = current_key._2
          }

          val entry:QueueEntryRecord = value

          group.last_entry_seq = current_key._2
          group.count += 1
          group.size += entry.size

          if(group.expiration == 0){
            group.expiration = entry.expiration
          } else {
            if( entry.expiration != 0 ) {
              group.expiration = entry.expiration.min(group.expiration)
            }
          }

          if( group.count == limit) {
            rc += group
            group = null
          }

          true // to continue cursoring.

        } else {
          false
        }
      }

      if( group!=null ) {
        rc += group
      }

    }
    rc
  }

  def getQueueEntries(queue_key: Long, firstSeq:Long, lastSeq:Long): Seq[QueueEntryRecord] = {
    var rc = ListBuffer[QueueEntryRecord]()
    with_ctx() { ctx=>
      import ctx._
      entries_db.cursor_from(tx, (queue_key, firstSeq)) { (key, value) =>
        val current_key:(Long,Long) = key
        if( current_key._1 == queue_key ) {

          val entry_seq = current_key._2
          val entry:QueueEntryRecord = value
          rc += entry
          entry_seq < lastSeq

        } else {
          false
        }
      }
    }
    rc
  }

  val metric_load_from_index_counter = new TimeCounter
  var metric_load_from_index = metric_load_from_index_counter(false)

  def loadMessages(requests: ListBuffer[(Long, (Option[MessageRecord])=>Unit)]):Unit = {

    val missing = with_ctx() { ctx=>
      import ctx._
      requests.flatMap { x =>
        val (message_key, callback) = x
        val record = metric_load_from_index_counter.time {
          messages_db.get(tx, to_database_entry(message_key)).map{ data=>
            import PBSupport._
            val pb:MessagePB.Buffer = data
            val rc = from_pb(pb)
            if( pb.hasDirectSize ) {
              rc.direct_buffer = direct_buffer_allocator.slice(pb.getDirectOffset, pb.getDirectSize)
            }
            rc
          }
        }
        if( record.isDefined ) {
          callback(record)
          None
        } else {
          Some(x)
        }
      }
    }

    if (missing.isEmpty)
      return

    // There's a small chance that a message was missing, perhaps we started a read tx, before the
    // write tx completed.  Lets try again..
    with_ctx() { ctx=>
      import ctx._
      missing.foreach { x =>
        val (message_key, callback) = x
        val record = metric_load_from_index_counter.time {
          messages_db.get(tx, to_database_entry(message_key)).map{ data=>
            import PBSupport._
            val pb:MessagePB.Buffer = data
            val rc = from_pb(pb)
            if( pb.hasDirectSize ) {
              rc.direct_buffer = direct_buffer_allocator.slice(pb.getDirectOffset, pb.getDirectSize)
            }
            rc
          }
        }
        callback(record)
      }
    }
  }


  def getLastMessageKey:Long = {
    with_ctx() { ctx=>
      import ctx._

      messages_db.last_key(tx).map(to_long _).getOrElse(0)
    }
  }


  def get(key: Buffer):Option[Buffer] = {
    with_ctx() { ctx=>
      import ctx._
      map_db.get(tx, to_database_entry(key)).map(x=> to_buffer(x))
    }
  }

  def get_prefixed_map_entries(prefix:Buffer):Seq[(Buffer, Buffer)] = {
    val rc = ListBuffer[(Buffer, Buffer)]()
    with_ctx() { ctx=>
      import ctx._
      map_db.cursor_prefixed(tx, prefix) { (key, value) =>
        rc += to_buffer(key) -> to_buffer(value)
        true
      }
    }
    rc
  }

  def getLastQueueKey:Long = {
    with_ctx() { ctx=>
      import ctx._

      queues_db.last_key(tx).map(to_long _).getOrElse(0)
    }
  }

  def export_data(os:OutputStream):Option[String] = {
    try {
      val manager = ExportStreamManager(os, 1)

      with_ctx() { ctx=>
        import ctx._

        messages_db.cursor(tx) { (_, value) =>
          val record = MessagePB.FACTORY.parseFramed(value.getData)
          manager.store_message(record)
          true
        }

        entries_db.cursor(tx) { (key, value) =>
          val record = QueueEntryPB.FACTORY.parseFramed(value.getData)
          manager.store_queue_entry(record)
          true
        }

        queues_db.cursor(tx) { (_, value) =>
          val record = QueuePB.FACTORY.parseFramed(value)
          manager.store_queue(record)
          true
        }

        map_db.cursor(tx) { (key,value) =>
          val record = new MapEntryPB.Bean
          record.setKey(key)
          record.setValue(value)
          manager.store_map_entry(record)
          true
        }
      }

      manager.finish
      None
    } catch {
      case x:Exception=>
        Some(x.getMessage)
    }
  }

  def import_data(is:InputStream):Option[String] = {
    try {
      val manager = ImportStreamManager(is)
      if(manager.version!=1) {
        return Some("Cannot import from an export file of version: "+manager.version)
      }

      purge

      with_ctx() { ctx=>
        import ctx._

        while(manager.getNext match {

          case record:MessagePB.Buffer =>
            messages_db.put(tx, record.getMessageKey, record.toFramedBuffer)
            true

          case record:QueueEntryPB.Buffer =>
            entries_db.put(tx, (record.getQueueKey, record.getQueueSeq), record.toFramedBuffer)
            add_and_get(message_refs_db, record.getMessageKey, 1, tx)
            true

          case record:QueuePB.Buffer =>
            var key: Long = record.getKey
            queues_db.put(tx, key, record.toFramedBuffer)
            true

          case record:MapEntryPB.Buffer =>
            map_db.put(tx, record.getKey, record.getValue)
            true

          case null =>
            false
        }) { // keep looping
        }

      }
      None

    } catch {
      case x:Exception=>
        Some(x.getMessage)
    }
  }
}
