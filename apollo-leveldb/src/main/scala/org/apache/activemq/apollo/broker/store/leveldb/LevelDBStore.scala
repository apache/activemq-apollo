package org.apache.activemq.apollo.broker.store.leveldb

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

import dto.{LevelDBStoreDTO, LevelDBStoreStatusDTO}
import collection.Seq
import org.fusesource.hawtdispatch._
import java.util.concurrent._
import atomic.{AtomicReference, AtomicLong}
import org.apache.activemq.apollo.broker.store._
import org.apache.activemq.apollo.util._
import org.fusesource.hawtdispatch.ListEventAggregator
import org.apache.activemq.apollo.dto.StoreStatusDTO
import org.apache.activemq.apollo.util.OptionSupport._
import java.io._
import org.apache.activemq.apollo.web.resources.ViewHelper
import org.fusesource.hawtbuf.Buffer
import FileSupport._
import org.apache.activemq.apollo.broker.store.leveldb.LevelDBClient._
import org.apache.activemq.apollo.broker.store.QueueRecord

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object LevelDBStore extends Log {
  val DATABASE_LOCKED_WAIT_DELAY = 10 * 1000;
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class LevelDBStore(val config: LevelDBStoreDTO) extends DelayingStoreSupport {

  var next_queue_key = new AtomicLong(1)
  var next_msg_key = new AtomicLong(1)

  var write_executor: ExecutorService = _
  var read_executor: ExecutorService = _

  var client: LevelDBClient = _

  def create_client = new LevelDBClient(this)


  def store_kind = "leveldb"

  override def toString = store_kind + " store at " + config.directory

  override protected def locator_based = true

  def flush_delay = config.flush_delay.getOrElse(500)

  protected def get_next_msg_key = next_msg_key.getAndIncrement


  override def on_store_requested(mr: MessageRecord) = {
    if( client.snappy_compress_logs && mr.compressed==null ) {
      val compressed = Snappy.compress(mr.buffer)
      if (compressed.length < mr.buffer.length) {
        mr.compressed = compressed
      }
    }
  }

  protected def store(uows: Seq[DelayableUOW])(callback: => Unit) = write_executor {
    try {
      client.store(uows)
    } catch {
      case e =>
      warn(e, "Failure occured while storing units of work: "+e)
    } finally {
      dispatch_queue {
        callback
      }
    }
  }

  protected def _start(on_completed: Task) = {
    try {
      client = create_client
      write_executor = Executors.newFixedThreadPool(1, new ThreadFactory() {
        def newThread(r: Runnable) = {
          val rc = new Thread(r, store_kind + " store io write")
          rc.setDaemon(true)
          rc
        }
      })
      read_executor = Executors.newFixedThreadPool(config.read_threads.getOrElse(10), new ThreadFactory() {
        def newThread(r: Runnable) = {
          val rc = new Thread(r, store_kind + " store io read")
          rc.setDaemon(true)
          rc
        }
      })
      schedule_reoccurring(1, TimeUnit.SECONDS) {
        poll_stats
      }
      schedule_reoccurring(10, TimeUnit.SECONDS) {
        write_executor {
          client.gc
        }
      }
      write_executor {
        try {
          client.start()
          next_msg_key.set(client.getLastMessageKey + 1)
          next_queue_key.set(client.get_last_queue_key + 1)
        } catch {
          case e: Throwable =>
            _service_failure = e
            LevelDBStore.error(e, "Store startup failure: " + e)
        } finally {
          on_completed.run
        }
      }
    }
    catch {
      case e: Throwable =>
        e.printStackTrace()
        LevelDBStore.error(e, "Store startup failure: " + e)
    }
  }

  protected def _stop(on_completed: Task) = {
    new Thread() {
      override def run = {
        write_executor.shutdown
        write_executor.awaitTermination(60, TimeUnit.SECONDS)
        write_executor = null
        read_executor.shutdown
        read_executor.awaitTermination(60, TimeUnit.SECONDS)
        read_executor = null
        client.stop
        on_completed.run
      }
    }.start
  }

  private def keep_polling = {
    val ss = service_state
    ss.is_starting || ss.is_started
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the Store interface
  //
  /////////////////////////////////////////////////////////////////////

  /**
   * Deletes all stored data from the store.
   */
  def purge(callback: => Unit) = {
    write_executor {
      client.purge()
      next_queue_key.set(1)
      next_msg_key.set(1)
      callback
    }
  }


  def get_map_entry(key: Buffer)(callback: (Option[Buffer]) => Unit) = {
    read_executor {
      callback(client.get(key))
    }
  }

  def get_prefixed_map_entries(prefix: Buffer)(callback: Seq[(Buffer, Buffer)] => Unit) = {
    read_executor {
      callback(client.get_prefixed_map_entries(prefix))
    }
  }

  /**
   * Ges the last queue key identifier stored.
   */
  def get_last_queue_key(callback: (Option[Long]) => Unit): Unit = {
    write_executor {
      callback(Some(client.get_last_queue_key))
    }
  }

  def add_queue(record: QueueRecord)(callback: (Boolean) => Unit) = {
    write_executor {
      client.add_queue(record, ^ {
        callback(true)
      })
    }
  }

  def remove_queue(queueKey: Long)(callback: (Boolean) => Unit) = {
    write_executor {
      client.remove_queue(queueKey, ^ {
        callback(true)
      })
    }
  }

  def get_queue(queueKey: Long)(callback: (Option[QueueRecord]) => Unit) = {
    write_executor {
      callback(client.get_queue(queueKey))
    }
  }

  def list_queues(callback: (Seq[Long]) => Unit) = {
    write_executor {
      callback(client.list_queues)
    }
  }

  val load_source = createSource(new ListEventAggregator[(Long, AtomicReference[Object], (Option[MessageRecord]) => Unit)](), dispatch_queue)
  load_source.setEventHandler(^ {
    drain_loads
  });
  load_source.resume


  def load_message(messageKey: Long, locator: AtomicReference[Object])(callback: (Option[MessageRecord]) => Unit) = {
    message_load_latency_counter.start {
      end =>
        load_source.merge((messageKey, locator, {
          (result) =>
            end()
            callback(result)
        }))
    }
  }

  def drain_loads = {
    var data = load_source.getData
    message_load_batch_size_counter += data.size
    read_executor ^ {
      client.loadMessages(data)
    }
  }

  def list_queue_entry_ranges(queueKey: Long, limit: Int)(callback: (Seq[QueueEntryRange]) => Unit) = {
    write_executor ^ {
      callback(client.listQueueEntryGroups(queueKey, limit))
    }
  }

  def list_queue_entries(queueKey: Long, firstSeq: Long, lastSeq: Long)(callback: (Seq[QueueEntryRecord]) => Unit) = {
    write_executor ^ {
      callback(client.getQueueEntries(queueKey, firstSeq, lastSeq))
    }
  }

  def poll_stats: Unit = {
    flush_latency = flush_latency_counter(true)
    message_load_latency = message_load_latency_counter(true)
    //        client.metric_journal_append = client.metric_journal_append_counter(true)
    //        client.metric_index_update = client.metric_index_update_counter(true)
    close_latency = close_latency_counter(true)
    message_load_batch_size = message_load_batch_size_counter(true)
  }

  def kind = "LevelDB"
  def location = config.directory.toString

  def get_store_status(callback: (StoreStatusDTO) => Unit) = dispatch_queue {
    val rc = new LevelDBStoreStatusDTO
    fill_store_status(rc)
    for( file <- config.directory.recursive_list ) {
      if(!file.isDirectory) {
        rc.disk_usage += file.length()
      }
    }
    rc.message_load_batch_size = message_load_batch_size
    import collection.JavaConversions._
    val pending_status = detailed_pending_status
    write_executor {
      client.using_index {
        rc.index_stats = client.index.getProperty("leveldb.stats")
        rc.log_append_pos = client.log.appender_limit
        rc.index_snapshot_pos = client.last_index_snapshot_pos
        rc.log_stats = {
          import collection.JavaConversions._
          var row_layout = "%-20s | %-10s | %-10s\n"
          row_layout.format("Log File", "Msg Refs", "File Size") +
            client.log.log_infos.map {
              case (id, info) => id -> client.log_refs.get(id).map(_.get)
            }.toSeq.sortWith{case (a,b)=> a._1 < b._1}.flatMap {
              case (id, refs) =>
                try {
                  val file = LevelDBClient.create_sequence_file(client.directory, id, LevelDBClient.LOG_SUFFIX)
                  val size = file.length()
                  Some(row_layout.format(
                    file.getName,
                    refs.getOrElse(0L).toString,
                    ViewHelper.memory(size)
                  ))
                } catch {
                  case e: Throwable =>
                    None
                }
            }.mkString("")
        }
      }
      rc.log_stats += pending_status
      callback(rc)
    }
  }

  /**
   * Exports the contents of the store to the provided streams.  Each stream should contain
   * a list of framed protobuf objects with the corresponding object types.
   */
  def export_data(os: OutputStream, cb: (Option[String]) => Unit) = write_executor {
    cb(client.export_data(os))
  }

  /**
   * Imports a previously exported set of streams.  This deletes any previous data
   * in the store.
   */
  def import_data(is: InputStream, cb: (Option[String]) => Unit) = write_executor {
    cb(client.import_data(is))
  }

  /**
   * Compacts the data in the store.
   */
  override def compact(callback: => Unit) = write_executor {
    info("Compacting '%s'", toString)
    client.index.compact_needed = true
    client.gc
    info("'%s' compaction completed", toString)
    callback
  }
}
