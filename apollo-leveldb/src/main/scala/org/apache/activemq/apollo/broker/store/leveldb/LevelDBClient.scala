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

import java.{lang => jl}
import java.{util => ju}

import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.broker.store._
import java.io._
import java.util.concurrent.TimeUnit
import org.apache.activemq.apollo.util._
import java.util.concurrent.locks.ReentrantReadWriteLock
import org.apache.activemq.apollo.util.{TreeMap => ApolloTreeMap}
import collection.immutable.TreeMap
import org.fusesource.leveldbjni.internal.Util
import org.apache.activemq.apollo.broker.Broker
import org.apache.activemq.apollo.util.ProcessSupport._
import collection.mutable.{HashMap, ListBuffer}
import org.apache.activemq.apollo.dto.JsonCodec
import org.iq80.leveldb._
import org.apache.activemq.apollo.broker.store.leveldb.RecordLog.LogInfo
import org.apache.activemq.apollo.broker.store.PBSupport
import java.util.concurrent.atomic.AtomicReference
import org.fusesource.hawtbuf.{DataByteArrayInputStream, Buffer}
import language.implicitConversions;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object LevelDBClient extends Log {

  final val STORE_SCHEMA_PREFIX = "leveldb_store:"
  final val STORE_SCHEMA_VERSION = 3

  final val queue_prefix = 'q'.toByte
  final val queue_entry_prefix = 'e'.toByte
  final val map_prefix = 'p'.toByte
  final val tmp_prefix = 't'.toByte

  final val queue_prefix_array = Array(queue_prefix)
  final val map_prefix_array = Array(map_prefix)
  final val queue_entry_prefix_array = Array(queue_entry_prefix)

  final val dirty_index_key = bytes(":dirty")
  final val log_refs_index_key = bytes(":log-refs")
  final val logs_index_key = bytes(":logs")
  final val TRUE = bytes("true")
  final val FALSE = bytes("false")

  final val LOG_ADD_QUEUE = 1.toByte
  final val LOG_REMOVE_QUEUE = 2.toByte
  final val LOG_ADD_MESSAGE = 3.toByte
  final val LOG_ADD_QUEUE_ENTRY = 5.toByte
  final val LOG_REMOVE_QUEUE_ENTRY = 6.toByte
  final val LOG_MAP_ENTRY = 7.toByte

  final val LOG_SUFFIX = ".log"
  final val INDEX_SUFFIX = ".index"
  var auto_compaction_ratio = 100

  def bytes(value: String) = value.getBytes("UTF-8")

  import FileSupport._

  def create_sequence_file(directory: File, id: Long, suffix: String) = directory / ("%016x%s".format(id, suffix))

  def find_sequence_files(directory: File, suffix: String): TreeMap[Long, File] = {
    TreeMap((directory.list_files.flatMap {
      f =>
        if (f.getName.endsWith(suffix)) {
          try {
            val base = f.getName.stripSuffix(suffix)
            val position = java.lang.Long.parseLong(base, 16);
            Some(position -> f)
          } catch {
            case e: NumberFormatException => None
          }
        } else {
          None
        }
    }): _*)
  }

  val on_windows = System.getProperty("os.name").toLowerCase().startsWith("windows")
  var link = (source:File, target:File) => jniLinkStrategy(source, target)

  def jniLinkStrategy(source: File, target: File) {
    // We first try to link via a native system call.
    try {
      Util.link(source, target)
    } catch {
      case e: IOException => throw e
      case e: Throwable =>
        // Fallback.. to a slower impl..
        debug("Native link system call not available")
        link = (source:File, target:File) => jnaLinkStrategy(source, target)
        link(source, target)
    }
  }

  def jnaLinkStrategy(source: File, target: File) {
    // Next try JNA (might not be in classpath)
    try {
      IOHelper.hardlink(source, target)
    } catch {
      case e: IOException => throw e
      case e: Throwable =>
        // Fallback.. to a slower impl..
        debug("JNA based hard link system call not available")
        link = if( on_windows ) {
          (source:File, target:File) => windowsCliLinkStrategy(source, target)
        } else {
          (source:File, target:File) => unixCliLinkStrategy(source, target)
        }
        link(source, target)
    }
  }

  def unixCliLinkStrategy(source: File, target: File) {
    system("ln", source.getCanonicalPath, target.getCanonicalPath) match {
      case (0, _, _) => // Success
      case (_, out, err) => None
      // TODO: we might want to look at the out/err to see why it failed
      // to avoid falling back to the slower strategy.
      debug("ln OS command not available either")
      link = (source:File, target:File) => copyLinkStrategy(source, target)
      link(source, target)
    }
  }

  def windowsCliLinkStrategy(source: File, target: File) {
    // Next try JNA (might not be in classpath)
    system("fsutil", "hardlink", "create", target.getCanonicalPath, source.getCanonicalPath) match {
      case (0, _, _) => // Success
      case (_, out, err) =>
        // TODO: we might want to look at the out/err to see why it failed
        // to avoid falling back to the slower strategy.
        debug("fsutil OS command not available either")
        link = (source:File, target:File) => copyLinkStrategy(source, target)
        link(source, target)
    }
  }

  def copyLinkStrategy(source: File, target: File) {
    // this final strategy is slow but sure to work.
    source.copy_to(target)
  }

  def copy_index(from:File, to:File) = {
    for( file <- from.list_files ) {
      val name: String = file.getName
      if( name.endsWith(".sst") ) {
        // SST files don't change once created, safe to hard link.
        link(file, to / name)
      } else if(name == "LOCK")  {
        // No need to copy the lock file.
      } else {
        /// These might not be append only files, so avoid hard linking just to be safe.
        file.copy_to(to/name)
      }
    }
  }

}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class LevelDBClient(store: LevelDBStore) {

  import HelperTrait._
  import LevelDBClient._
  import FileSupport._

  def dispatchQueue = store.dispatch_queue

  implicit def toByteArray(buf: Buffer): Array[Byte] = buf.toByteArray

  implicit def toBuffer(buf: Array[Byte]): Buffer = new Buffer(buf)

  /////////////////////////////////////////////////////////////////////
  //
  // Helpers
  //
  /////////////////////////////////////////////////////////////////////

  def config = store.config

  def directory = config.directory

  /////////////////////////////////////////////////////////////////////
  //
  // Public interface used by the LevelDBStore
  //
  /////////////////////////////////////////////////////////////////////

  var sync = false;
  var verify_checksums = false;

  var log: RecordLog = _

  var snappy_compress_logs = false
  var index: RichDB = _
  var index_options: Options = _

  var last_index_snapshot_ts = System.currentTimeMillis()
  var last_index_snapshot_pos: Long = _
  val snapshot_rw_lock = new ReentrantReadWriteLock(true)

  var factory: DBFactory = _
  val log_refs = HashMap[Long, LongCounter]()
  var recovery_logs:java.util.TreeMap[Long, Void] = _

  def dirty_index_file = directory / ("dirty" + INDEX_SUFFIX)

  def temp_index_file = directory / ("temp" + INDEX_SUFFIX)

  def snapshot_index_file(id: Long) = create_sequence_file(directory, id, INDEX_SUFFIX)

  def create_log: RecordLog = {
    new RecordLog(directory, LOG_SUFFIX)
  }

  def log_size = {
    Option(config.log_size).map(MemoryPropertyEditor.parse(_)).getOrElse(1024 * 1024 * 100L)
  }

  def paranoid_checks = OptionSupport(config.paranoid_checks).getOrElse(false)

  def start() = {
    import OptionSupport._
    directory.mkdirs()

//    val factory_names = Option(config.index_factory).getOrElse("org.iq80.leveldb.impl.Iq80DBFactory")
    val factory_names = Option(config.index_factory).getOrElse("org.fusesource.leveldbjni.JniDBFactory, org.iq80.leveldb.impl.Iq80DBFactory")
    factory = factory_names.split("""(,|\s)+""").map(_.trim()).flatMap {
      name =>
        try {
          Some(Broker.class_loader.loadClass(name).newInstance().asInstanceOf[DBFactory])
        } catch {
          case x: Throwable =>
            None
        }
    }.headOption.getOrElse(throw new Exception("Could not load any of the index factory classes: " + factory_names))

    if (factory.getClass.getName == "org.iq80.leveldb.impl.Iq80DBFactory") {
      warn("Using the pure java LevelDB implementation which is still experimental.  If the JNI version is not available for your platform, please switch to the BDB store instead. http://activemq.apache.org/apollo/documentation/user-manual.html#BDB_Store")
    }

    sync = config.sync.getOrElse(true);
    verify_checksums = config.verify_checksums.getOrElse(false);

    index_options = new Options();
    index_options.createIfMissing(true);


    auto_compaction_ratio = OptionSupport(config.auto_compaction_ratio).getOrElse(100)
    config.index_max_open_files.foreach(index_options.maxOpenFiles(_))
    config.index_block_restart_interval.foreach(index_options.blockRestartInterval(_))
    index_options.paranoidChecks(paranoid_checks)
    Option(config.index_write_buffer_size).map(MemoryPropertyEditor.parse(_).toInt).foreach(index_options.writeBufferSize(_))
    Option(config.index_block_size).map(MemoryPropertyEditor.parse(_).toInt).foreach(index_options.blockSize(_))
    Option(config.index_compression).foreach(x => index_options.compressionType(x match {
      case "snappy" => CompressionType.SNAPPY
      case "none" => CompressionType.NONE
      case _ => CompressionType.SNAPPY
    }))

    if (Option(config.log_compression).map(_.toLowerCase).getOrElse("snappy") == "snappy" && Snappy != null) {
      snappy_compress_logs = true
    }

    index_options.cacheSize(Option(config.index_cache_size).map(MemoryPropertyEditor.parse(_).toLong).getOrElse(1024 * 1024 * 256L))
    index_options.logger(new Logger() {
      def log(msg: String) = trace(msg.stripSuffix("\n"))
    })

    log = create_log
    log.sync = sync
    log.logSize = log_size
    log.verify_checksums = verify_checksums
    log.on_log_rotate = () => {
      // lets queue a request to checkpoint when
      // the logs rotate.. queue it on the GC thread since GC's lock
      // the index for a long time.
      store.write_executor {
        snapshot_index
      }
    }

    lock_file = new LockFile(directory / "lock", true)
    def time[T](func: => T): Long = {
      val start = System.nanoTime()
      func
      System.nanoTime() - start
    }

    // Lock before we open anything..
    lock_store

    // Lets check store compatibility...
    val version_file = directory / "store-version.txt"
    if (version_file.exists()) {
      val ver = try {
        var tmp: String = version_file.read_text().trim()
        if (tmp.startsWith(STORE_SCHEMA_PREFIX)) {
          tmp.stripPrefix(STORE_SCHEMA_PREFIX).toInt
        } else {
          -1
        }
      } catch {
        case e:Throwable => throw new Exception("Unexpected version file format: " + version_file)
      }
      ver match {
        case STORE_SCHEMA_VERSION => // All is good.
        case _ => throw new Exception("Cannot open the store.  It's schema version is not supported.")
      }
    }
    version_file.write_text(STORE_SCHEMA_PREFIX + STORE_SCHEMA_VERSION)

    val log_open_duration = time {
      retry {
        log.open
      }
    }
    info("Opening the log file took: %.2f ms", (log_open_duration / TimeUnit.MILLISECONDS.toNanos(1).toFloat))

    // Find out what was the last snapshot.
    val snapshots = find_sequence_files(directory, INDEX_SUFFIX)
    var last_snapshot_index = snapshots.lastOption
    last_index_snapshot_pos = last_snapshot_index.map(_._1).getOrElse(0)

    // Only keep the last snapshot..
    snapshots.filterNot(_._1 == last_index_snapshot_pos).foreach(_._2.recursive_delete)
    temp_index_file.recursive_delete // usually does not exist.

    var reportedFailure:Throwable = null
    retry {

      // Delete the dirty indexes
      dirty_index_file.recursive_delete
      dirty_index_file.mkdirs()

      for( (id, file) <- last_snapshot_index) {
        try {
          copy_index(file, dirty_index_file)
        } catch {
          case e: Exception =>
            warn(e, "Could not recover snapshot of the index: " + e)
            last_snapshot_index = None
        }
      }

      def recover = {
        index = new RichDB(factory.open(dirty_index_file, index_options))
        if (paranoid_checks) {
          for(value <- index.get(dirty_index_key) ) {
            if( java.util.Arrays.equals(value, TRUE) ) {
              warn("Recovering from a dirty index.")
            }
          }
        }
        index.put(dirty_index_key, TRUE)

        load_log_refs

        if (paranoid_checks) {
          check_index_integrity(index)
        }

        // Update the index /w what was stored on the logs..
        var pos = last_index_snapshot_pos;

        var last_reported_at = System.currentTimeMillis();
        var showing_progress = false
        var last_reported_pos = 0L

        def remaining(eta: Double) = {
          if (eta > 60 * 60) {
            "%.2f hrs".format(eta / (60 * 60))
          } else if (eta > 60) {
            "%.2f mins".format(eta / 60)
          } else {
            "%.0f secs".format(eta)
          }
        }

        var replay_operations = 0
        val log_replay_duration = time {
          while (pos < log.appender_limit) {

            val now = System.currentTimeMillis();
            if (now > last_reported_at + 1000) {
              val at = pos - last_index_snapshot_pos
              val total = log.appender_limit - last_index_snapshot_pos
              val rate = (pos - last_reported_pos) * 1000.0 / (now - last_reported_at)
              val eta = (total - at) / rate

              System.out.print("Replaying recovery log: %.2f%% done (%,d/%,d bytes) @ %,.2f kb/s, %s remaining.     \r".format(
                at * 100.0 / total, at, total, rate / 1024, remaining(eta)))
              showing_progress = true;
              last_reported_at = now
              last_reported_pos = pos
            }

            log.read(pos).map {
              case (kind, data, next_pos) =>
                kind match {
                  case LOG_ADD_QUEUE_ENTRY =>
                    replay_operations += 1
                    val record = QueueEntryPB.FACTORY.parseUnframed(data)

                    val index_record = record.copy()
                    index_record.clearQueueKey()
                    index_record.clearQueueSeq()
                    index.put(encode_key(queue_entry_prefix, record.getQueueKey, record.getQueueSeq), index_record.freeze().toUnframedBuffer)

                    log_ref_increment(decode_vlong(record.getMessageLocator))

                  case LOG_REMOVE_QUEUE_ENTRY =>
                    replay_operations += 1
                    index.get(data, new ReadOptions).foreach {
                      value =>
                        val record = QueueEntryPB.FACTORY.parseUnframed(value)
                        val pos = decode_vlong(record.getMessageLocator)
                        pos.foreach(log_ref_decrement(_))
                        index.delete(data)
                    }

                  case LOG_ADD_QUEUE =>
                    replay_operations += 1
                    val record = QueuePB.FACTORY.parseUnframed(data)
                    index.put(encode_key(queue_prefix, record.getKey), data)

                  case LOG_REMOVE_QUEUE =>
                    replay_operations += 1
                    val ro = new ReadOptions
                    ro.fillCache(false)
                    ro.verifyChecksums(verify_checksums)
                    val queue_key = decode_vlong(data)
                    index.delete(encode_key(queue_prefix, queue_key))
                    index.cursor_prefixed(encode_key(queue_entry_prefix, queue_key), ro) {
                      (key, value) =>
                        index.delete(key)

                        // Figure out what log file that message entry was in so we can,
                        // decrement the log file reference.
                        val record = QueueEntryPB.FACTORY.parseUnframed(value)
                        val pos = decode_vlong(record.getMessageLocator)
                        log_ref_decrement(pos)
                        true
                    }

                  case LOG_MAP_ENTRY =>
                    replay_operations += 1
                    val entry = MapEntryPB.FACTORY.parseUnframed(data)
                    if (entry.getValue == null) {
                      index.delete(encode_key(map_prefix, entry.getKey))
                    } else {
                      index.put(encode_key(map_prefix, entry.getKey), entry.getValue.toByteArray)
                    }
                  case _ =>
                  // Skip records which don't require index updates.
                }
                pos = next_pos
            }
          }
          if (replay_operations > 0) {
            snapshot_index
          }
        }

        if (showing_progress) {
          System.out.println("Replaying recovery log: done. %d operations recovered in %s".format(replay_operations, log_replay_duration.toDouble / TimeUnit.SECONDS.toNanos(1)));
        }

        // Access the last queue just to see if we need to compact the index (checks for slow access).
        for( queue <- list_queues.lastOption ) {
          listQueueEntryGroups(queue, 100000)
        }
        // delete obsolete files..
        gc
      }

      try {
        recover

      } catch {
        case e: Throwable =>
          if( reportedFailure == null ) {
            reportedFailure = e
            // replay failed.. good thing we are in a retry block...
            if( index!=null) {
              index.close
              index = null
            }
            // Lets do a repair for shits and giggles..
            factory.repair(dirty_index_file, index_options)
            throw e;
          } else {
            reportedFailure = e
          }
      } finally {
        recovery_logs = null
      }
    }

    if( reportedFailure!=null ) {
      throw reportedFailure;
    }
  }

  def check_index_integrity(index: RichDB) = {
    val actual_log_refs = HashMap[Long, LongCounter]()
    var referenced_queues = Set[Long]()

    // Lets find out what the queue entries are..
    var fixed_records = 0
    index.cursor_prefixed(queue_entry_prefix_array) { (key, value) =>
      try {
        val (_, queue_key, seq_key) = decode_long_long_key(key)
        val record = QueueEntryPB.FACTORY.parseUnframed(value)
        val (pos, len) = decode_locator(record.getMessageLocator)
        for( key <- log_ref_key(pos) ) {
          actual_log_refs.getOrElseUpdate(key, new LongCounter()).incrementAndGet()
        }
        referenced_queues += queue_key
      } catch {
        case e:Throwable =>
          warn(e, "invalid queue entry record: %s, error: %s", new Buffer(key), e)
          fixed_records += 1
          // Invalid record.
          index.delete(key)
      }
      true
    }

    // Lets cross check the queues.
    index.cursor_prefixed(queue_prefix_array) {
      (key, value) =>
        try {
          val (_, queue_key) = decode_long_key(key)
          val record = QueuePB.FACTORY.parseUnframed(value)
          if (record.getKey != queue_key) {
            throw new IOException("key mismatch")
          }
          referenced_queues -= queue_key
        } catch {
          case e:Throwable =>
            trace("invalid queue record: %s, error: %s", new Buffer(key), e)
            fixed_records += 1
            // Invalid record.
            index.delete(key)
        }
        true
    }

    referenced_queues.foreach {
      queue_key =>
      // We have queue entries for a queue that does not exist..
        index.cursor_prefixed(encode_key(queue_entry_prefix, queue_key)) {
          (key, value) =>
            trace("invalid queue entry record: %s, error: queue key does not exits %s", new Buffer(key), queue_key)
            fixed_records += 1
            index.delete(key)
            val record = QueueEntryPB.FACTORY.parseUnframed(value)
            val pos = decode_vlong(record.getMessageLocator)
            for( key <- log_ref_key(pos)) {
              for( counter <- actual_log_refs.get(key) ) {
                if (counter.decrementAndGet() == 0) {
                  actual_log_refs.remove(key)
                }
              }
            }
            true
        }
    }

    if (actual_log_refs != log_refs) {
      debug("expected != actual log references. expected: %s, actual %s", log_refs, actual_log_refs)
      log_refs.clear()
      log_refs ++= actual_log_refs
    }

    if (fixed_records > 0) {
      warn("Fixed %d invalid index enties in the leveldb store", fixed_records)
    }
  }

  var lock_file: LockFile = _

  def lock_store = {
    import OptionSupport._
    if (config.fail_if_locked.getOrElse(false)) {
      lock_file.lock()
    } else {
      retry {
        lock_file.lock()
      }
    }
  }

  def unlock_store = {
    lock_file.unlock()
  }

  private def store_log_refs = {
    import collection.JavaConversions.mapAsJavaMap
    index.put(log_refs_index_key, JsonCodec.encode(mapAsJavaMap(log_refs.mapValues(_.get()))).toByteArray)
    index.put(logs_index_key, JsonCodec.encode(log.log_file_positions).toByteArray)
  }

  private def load_log_refs = {
    import collection.JavaConversions._
    log_refs.clear()
    index.get(log_refs_index_key, new ReadOptions).foreach {
      value =>
        for( (k, v) <- JsonCodec.decode(new Buffer(value), classOf[java.util.Map[String, Object]]) ) {
            log_refs.put(k.toLong, new LongCounter(v.asInstanceOf[Number].longValue()))
        }
    }

    index.get(logs_index_key, new ReadOptions).map { value =>
      recovery_logs = new java.util.TreeMap[Long, Void]()
      for( v <- JsonCodec.decode(new Buffer(value), classOf[java.util.List[Object]]) ) {
        recovery_logs.put(v.asInstanceOf[Number].longValue(), null)
      }
    }
  }

  def stop() = {
    // this blocks until all io completes..
    snapshot_rw_lock.writeLock().lock()
    store_log_refs
    index.put(dirty_index_key, FALSE, new WriteOptions().sync(true))
    index.close
    log.close
    copy_dirty_index_to_snapshot
    log = null
    unlock_store
  }

  def using_index[T](func: => T): T = {
    val lock = snapshot_rw_lock.readLock();
    lock.lock()
    try {
      func
    } finally {
      lock.unlock()
    }
  }

  def retry_using_index[T](func: => T): T = retry(using_index(func))

  /**
   * TODO: expose this via management APIs, handy if you want to
   * do a file system level snapshot and want the data to be consistent.
   */
  def suspend() = {
    // Make sure we are the only ones accessing the index. since
    // we will be closing it to create a consistent snapshot.
    snapshot_rw_lock.writeLock().lock()

    store_log_refs
    index.put(dirty_index_key, FALSE, new WriteOptions().sync(true))
    // Suspend the index so that it's files are not changed async on us.
    index.db.suspendCompactions
  }

  /**
   * TODO: expose this via management APIs, handy if you want to
   * do a file system level snapshot and want the data to be consistent.
   */
  def resume() = {
    // re=open it..
    retry {
      index.db.resumeCompactions
      index.put(dirty_index_key, TRUE)
    }
    snapshot_rw_lock.writeLock().unlock()
  }

  def copy_dirty_index_to_snapshot {
    if (log.appender_limit == last_index_snapshot_pos) {
      // no need to snapshot again...
      return
    }

    // Where we start copying files into.  Delete this on
    // restart.
    val tmp_dir = temp_index_file
    tmp_dir.mkdirs()

    try {

      // Copy/Hard link all the index files.
      copy_index(dirty_index_file, tmp_dir)

      // Rename to signal that the snapshot is complete.
      val new_snapshot_index_pos = log.appender_limit
      tmp_dir.renameTo(snapshot_index_file(new_snapshot_index_pos))

      snapshot_index_file(last_index_snapshot_pos).recursive_delete
      last_index_snapshot_pos = new_snapshot_index_pos
      last_index_snapshot_ts = System.currentTimeMillis()

    } catch {
      case e: Exception =>
        // if we could not snapshot for any reason, delete it as we don't
        // want a partial check point..
        warn(e, "Could not snapshot the index: " + e)
        tmp_dir.recursive_delete
    }
  }

  def snapshot_index: Unit = {
    if (log.appender_limit == last_index_snapshot_pos) {
      // no need to snapshot again...
      return
    }
//    if (paranoid_checks) {
//      check_index_integrity(index)
//    }
    suspend()
    try {
      copy_dirty_index_to_snapshot
    } finally {
      resume()
    }
  }

  def retry[T](func: => T): T = {
    var error: Throwable = null
    var rc: Option[T] = None

    // We will loop until the tx succeeds.  Perhaps it's
    // failing due to a temporary condition like low disk space.
    while (!rc.isDefined) {

      try {
        rc = Some(func)
      } catch {
        case e: Throwable =>
          if (error == null) {
            warn(e, "DB operation failed. (entering recovery mode): " + e)
          }
          error = e
      }

      if (!rc.isDefined) {
        // We may need to give up if the store is being stopped.
        if (!store.service_state.is_starting_or_started) {
          throw error
        }
        Thread.sleep(1000)
      }
    }

    if (error != null) {
      info("DB recovered from failure.")
    }
    rc.get
  }

  def purge() = {
    snapshot_rw_lock.writeLock().lock()
    try {
      log.close
      index.close
      directory.list_files.foreach(_.recursive_delete)
      log_refs.clear()
    } finally {
      retry {
        index = new RichDB(factory.open(dirty_index_file, index_options))
        log.open
      }
      snapshot_rw_lock.writeLock().unlock()
    }
  }

  def add_queue(record: QueueRecord, callback: Runnable) = {
    retry_using_index {
      log.appender {
        appender =>
          val value: Buffer = PBSupport.to_pb(record).freeze().toUnframedBuffer
          appender.append(LOG_ADD_QUEUE, value)
          index.put(encode_key(queue_prefix, record.key), value)
      }
    }
    callback.run
  }

  def log_ref_decrement(pos: Long, log_info: LogInfo = null) = this.synchronized {
    for( key <- log_ref_key(pos, log_info) ) {
      for( counter<- log_refs.get(key) ) {
        if (counter.decrementAndGet() == 0) {
          log_refs.remove(key)
        }
      }
    }
  }

  def log_ref_increment(pos: Long, log_info: LogInfo = null) = this.synchronized {
    for( key <- log_ref_key(pos, log_info) ) {
      log_refs.getOrElseUpdate(key, new LongCounter()).incrementAndGet()
    }
  }


  def log_ref_key(pos: Long, log_info: RecordLog.LogInfo=null): Option[Long] = {
    if( log_info!=null ) {
      Some(log_info.position)
    } else {
      val rc = if( recovery_logs !=null ) {
        Option(recovery_logs.floorKey(pos))
      } else {
        log.log_info(pos).map(_.position)
      }
      if( !rc.isDefined ) {
        warn("Invalid log position: " + pos)
      }
      rc
    }
  }

  def remove_queue(queue_key: Long, callback: Runnable) = {
    retry_using_index {
      log.appender {
        appender =>
          val ro = new ReadOptions
          ro.fillCache(false)
          ro.verifyChecksums(verify_checksums)
          appender.append(LOG_REMOVE_QUEUE, encode_vlong(queue_key))
          index.delete(encode_key(queue_prefix, queue_key))
          index.cursor_prefixed(encode_key(queue_entry_prefix, queue_key), ro) {
            (key, value) =>
              index.delete(key)

              // Figure out what log file that message entry was in so we can,
              // decrement the log file reference.
              val record = QueueEntryPB.FACTORY.parseUnframed(value)
              val pos = decode_vlong(record.getMessageLocator)
              log_ref_decrement(pos)
              true
          }
      }
    }
    callback.run
  }

  def store(uows: Seq[LevelDBStore#DelayableUOW]) {
    retry_using_index {
      log.appender {
        appender =>

          var sync_needed = false
          index.write() {
            batch =>
              uows.foreach {
                uow =>

                  for ((key, value) <- uow.map_actions) {
                    val entry = new MapEntryPB.Bean()
                    entry.setKey(key)
                    if (value == null) {
                      batch.delete(encode_key(map_prefix, key))
                    } else {
                      entry.setValue(value)
                      batch.put(encode_key(map_prefix, key), value.toByteArray)
                    }
                    var log_data = entry.freeze().toUnframedBuffer

                    appender.append(LOG_MAP_ENTRY, log_data)
                  }

                  uow.actions.foreach {
                    case (msg, action) =>
                      val message_record = action.message_record
                      var locator: (Long, Int) = null
                      var log_info: LogInfo = null

                      if (message_record != null) {

                        val pb = new MessagePB.Bean
                        pb.setCodec(message_record.codec)

                        val body = if(message_record.compressed!=null) {
                          pb.setCompression(1)
                          message_record.compressed
                        } else {
                          message_record.buffer
                        }
                        var header = pb.freeze().toFramedBuffer

                        val (pos, log_info) = appender.append(LOG_ADD_MESSAGE, header, body)
                        locator = (pos, header.length + body.length)
                        message_record.locator.set(locator);
                      }

                      action.dequeues.foreach {
                        entry =>
                          if (locator == null) {
                            locator = entry.message_locator.get().asInstanceOf[(Long, Int)]
                          }
                          assert(locator != null)
                          val (pos, len) = locator
                          val key = encode_key(queue_entry_prefix, entry.queue_key, entry.entry_seq)

                          appender.append(LOG_REMOVE_QUEUE_ENTRY, key)
                          batch.delete(key)
                          log_ref_decrement(pos, log_info)
                      }

                      var locator_buffer: Buffer = null
                      action.enqueues.foreach {
                        entry =>
                          if (locator == null) {
                            locator = entry.message_locator.get().asInstanceOf[(Long, Int)]
                          }
                          assert(locator != null)
                          val (pos, len) = locator
                          if (locator_buffer == null) {
                            locator_buffer = encode_locator(pos, len)
                          }

                          entry.message_locator.set(locator)

                          val log_record = new QueueEntryPB.Bean
                          // TODO: perhaps we should normalize the sender to make the index entries more compact.
                          if( entry.sender!=null ) {
                            entry.sender.foreach(log_record.addSender(_))
                          }
                          log_record.setMessageLocator(locator_buffer)
                          log_record.setQueueKey(entry.queue_key)
                          log_record.setQueueSeq(entry.entry_seq)
                          log_record.setSize(entry.size)
                          if (entry.expiration != 0)
                            log_record.setExpiration(entry.expiration)
                          if (entry.redeliveries != 0)
                            log_record.setRedeliveries(entry.redeliveries)

                          appender.append(LOG_ADD_QUEUE_ENTRY, log_record.freeze().toUnframedBuffer)

                          // Slim down the index record, the smaller it is the cheaper the compactions
                          // will be and the more we can cache in mem.
                          val index_record = log_record.copy()
                          index_record.clearQueueKey()
                          index_record.clearQueueSeq()
                          batch.put(encode_key(queue_entry_prefix, entry.queue_key, entry.entry_seq), index_record.freeze().toUnframedBuffer)

                          // Increment it.
                          log_ref_increment(pos, log_info)

                      }
                  }
                  if (uow.flush_sync) {
                    sync_needed = true
                  }
              }
          }
          if (sync_needed && sync) {
            appender.flush
            appender.force
          }
      }
    }
  }

  val metric_load_from_index_counter = new TimeCounter
  var metric_load_from_index = metric_load_from_index_counter(false)

  def loadMessages(requests: ListBuffer[(Long, AtomicReference[Object], (Option[MessageRecord]) => Unit)]): Unit = {

    val ro = new ReadOptions
    ro.verifyChecksums(verify_checksums)
    ro.fillCache(true)

    val missing = retry_using_index {
      index.snapshot {
        snapshot =>
          ro.snapshot(snapshot)
          requests.flatMap {
            x =>
              val (_, locator, callback) = x
              val record = metric_load_from_index_counter.time {
                val (pos, len) = locator.get().asInstanceOf[(Long, Int)]
                log.read(pos, len).map { data =>
                  val is = new DataByteArrayInputStream(data)
                  val pb = MessagePB.FACTORY.parseFramed(is)
                  val rc = PBSupport.from_pb(pb)
                  rc.buffer = is.readBuffer(is.available())
                  rc.locator = locator
                  if(pb.getCompression == 1) {
                    rc.buffer = Snappy.uncompress(rc.buffer)
                  }
                  rc
                }
              }
              if (record.isDefined) {
                callback(record)
                None
              } else {
                Some(x)
              }
          }
      }
    }

    if (missing.isEmpty)
      return

    // There's a small chance that a message was missing, perhaps we started a read tx, before the
    // write tx completed.  Lets try again..
    retry_using_index {
      index.snapshot {
        snapshot =>
          ro.snapshot(snapshot)
          missing.foreach {
            x =>
              val (_, locator, callback) = x
              val record: Option[MessageRecord] = metric_load_from_index_counter.time {
                val (pos, len) = locator.get().asInstanceOf[(Long, Int)]
                log.read(pos, len).map { data=>
                  val is = new DataByteArrayInputStream(data)
                  val pb = MessagePB.FACTORY.parseFramed(is)
                  val rc = PBSupport.from_pb(pb)
                  rc.buffer = is.readBuffer(is.available())
                  rc.locator = locator
                  if(pb.getCompression == 1) {
                    rc.buffer = Snappy.uncompress(rc.buffer)
                  }
                  rc
                }
              }
              callback(record)
          }
      }
    }
  }

  def list_queues: Seq[Long] = {
    val rc = ListBuffer[Long]()
    using_index {
      val ro = new ReadOptions
      ro.verifyChecksums(verify_checksums)
      ro.fillCache(false)
      index.cursor_keys_prefixed(queue_prefix_array, ro) {
        key =>
          rc += decode_long_key(key)._2
          true // to continue cursoring.
      }
    }
    rc
  }

  def get_queue(queue_key: Long): Option[QueueRecord] = {
    retry_using_index {
      val ro = new ReadOptions
      ro.fillCache(false)
      ro.verifyChecksums(verify_checksums)
      index.get(encode_key(queue_prefix, queue_key), ro).map {
        x =>
          PBSupport.from_pb(QueuePB.FACTORY.parseUnframed(x))
      }
    }
  }

  def listQueueEntryGroups(queue_key: Long, limit: Int): Seq[QueueEntryRange] = {
    var rc = ListBuffer[QueueEntryRange]()
    val ro = new ReadOptions
    ro.verifyChecksums(verify_checksums)
    ro.fillCache(false)
    retry_using_index {
      index.snapshot {
        snapshot =>
          ro.snapshot(snapshot)

          var group: QueueEntryRange = null
          index.cursor_prefixed(encode_key(queue_entry_prefix, queue_key), ro) {
            (key, value) =>

              val (_, _, current_key) = decode_long_long_key(key)
              if (group == null) {
                group = new QueueEntryRange
                group.first_entry_seq = current_key
              }

              val entry = QueueEntryPB.FACTORY.parseUnframed(value)
              val pos = decode_vlong(entry.getMessageLocator)

              group.last_entry_seq = current_key
              group.count += 1
              group.size += entry.getSize

              if (group.expiration == 0) {
                group.expiration = entry.getExpiration
              } else {
                if (entry.getExpiration != 0) {
                  group.expiration = entry.getExpiration.min(group.expiration)
                }
              }

              if (group.count == limit) {
                rc += group
                group = null
              }

              true // to continue cursoring.
          }
          if (group != null) {
            rc += group
          }
      }
    }
    rc
  }

  def getQueueEntries(queue_key: Long, firstSeq: Long, lastSeq: Long): Seq[QueueEntryRecord] = {
    var rc = ListBuffer[QueueEntryRecord]()
    val ro = new ReadOptions
    ro.verifyChecksums(verify_checksums)
    ro.fillCache(true)
    retry_using_index {
      index.snapshot {
        snapshot =>
          ro.snapshot(snapshot)
          val start = encode_key(queue_entry_prefix, queue_key, firstSeq)
          val end = encode_key(queue_entry_prefix, queue_key, lastSeq + 1)
          index.cursor_range(start, end, ro) {
            (key, value) =>
              val (_, _, queue_seq) = decode_long_long_key(key)
              val record = QueueEntryPB.FACTORY.parseUnframed(value)
              val entry = PBSupport.from_pb(record)
              entry.queue_key = queue_key
              entry.entry_seq = queue_seq
              entry.message_locator = new AtomicReference[Object](decode_locator(record.getMessageLocator))
              rc += entry
              true
          }
      }
    }
    rc
  }

  def getLastMessageKey: Long = 0

  def get(key: Buffer): Option[Buffer] = {
    retry_using_index {
      index.get(encode_key(map_prefix, key)).map(new Buffer(_))
    }
  }

  def get_prefixed_map_entries(prefix: Buffer): Seq[(Buffer, Buffer)] = {
    val rc = ListBuffer[(Buffer, Buffer)]()
    retry_using_index {
      index.cursor_prefixed(encode_key(map_prefix, prefix)) {
        (key, value) =>
          rc += new Buffer(key) -> new Buffer(value)
          true
      }
    }
    rc
  }

  def get_last_queue_key: Long = {
    retry_using_index {
      index.last_key(queue_prefix_array).map(decode_long_key(_)._2).getOrElse(0)
    }
  }

  // APLO-245: lets try to detect when leveldb needs a compaction..
  def detect_if_compact_needed:Unit = {

    // auto compaction might be disabled...
    if ( auto_compaction_ratio <= 0 ) {
      return
    }

    // How much space is the dirty index using??
    var index_usage = 0L
    for( file <- dirty_index_file.recursive_list ) {
      if(!file.isDirectory && file.getName.endsWith(".sst") ) {
        index_usage += file.length()
      }
    }
    // Lets use the log_refs to get a rough estimate on how many entries are store in leveldb.
    var index_queue_entries=0L
    for ( (_, count) <- log_refs ) {
      index_queue_entries += count.get()
    }

    // Don't force compactions until level 0 is full.
    val SSL_FILE_SIZE = 1024*1024*4L
    if( index_usage > SSL_FILE_SIZE*10 ) {
      if ( index_queue_entries > 0 ) {
        val ratio = (index_usage*1.0f/index_queue_entries)
        // println("usage: index_usage:%d, index_queue_entries:%d, ratio: %f".format(index_usage, index_queue_entries, ratio))

        // After running some load we empirically found that a healthy ratio is between 12 and 25 bytes per entry.
        // lets compact if we go way over the healthy ratio.
        if( ratio > auto_compaction_ratio ) {
          index.compact_needed = true
        }
      } else {
        // at most the index should have 1 full level file.
        index.compact_needed = true
      }
    }
  }

  def gc: Unit = {

    // TODO:
    // Perhaps we should snapshot_index if the current snapshot is old.
    //
    import collection.JavaConversions._

    detect_if_compact_needed

    // Lets compact the leveldb index if it looks like we need to.
    if( index.compact_needed ) {
      debug("Compacting the leveldb index at: %s", dirty_index_file)
      val start = System.nanoTime()
      index.compact
      val duration = System.nanoTime() - start;
      info("Compacted the leveldb index at: %s in %.2f ms", dirty_index_file, (duration / 1000000.0))
    }
    val empty_journals = log.log_infos.keySet.toSet -- log_refs.keySet

    // We don't want to delete any journals that the index has not snapshot'ed or
    // the the
    val delete_limit = log_ref_key(last_index_snapshot_pos).
      getOrElse(last_index_snapshot_pos).min(log.appender_start)

    empty_journals.foreach {
      id =>
        if (id < delete_limit) {
          log.delete(id)
        }
    }
  }

  case class UsageCounter(info: LogInfo) {
    var count = 0L
    var size = 0L
    var first_reference_queue: QueueRecord = _

    def increment(value: Int) = {
      count += 1
      size += value
    }
  }

  //
  // Collects detailed usage information about the journal like who's referencing it.
  //
  //  def get_log_usage_details = {
  //
  //    val usage_map = new ApolloTreeMap[Long,UsageCounter]()
  //    log.log_mutex.synchronized {
  //      log.log_infos.foreach(entry=> usage_map.put(entry._1, UsageCounter(entry._2)) )
  //    }
  //
  //    def lookup_usage(pos: Long) = {
  //      var entry = usage_map.floorEntry(pos)
  //      if (entry != null) {
  //        val usage = entry.getValue()
  //        if (pos < usage.info.limit) {
  //          Some(usage)
  //        } else {
  //          None
  //        }
  //      } else {
  //        None
  //      }
  //    }
  //
  //    val ro = new ReadOptions()
  //    ro.fillCache(false)
  //    ro.verifyChecksums(verify_checksums)
  //
  //    retry_using_index {
  //      index.snapshot { snapshot =>
  //        ro.snapshot(snapshot)
  //
  //        // Figure out which journal files are still in use by which queues.
  //        index.cursor_prefixed(queue_entry_prefix_array, ro) { (_,value) =>
  //
  //          val entry_record:QueueEntryRecord = value
  //          val pos = if(entry_record.message_locator!=null) {
  //            Some(decode_locator(entry_record.message_locator)._1)
  //          } else {
  //            index.get(encode_key(message_prefix, entry_record.message_key)).map(decode_locator(_)._1)
  //          }
  //
  //          pos.flatMap(lookup_usage(_)).foreach { usage =>
  //            if( usage.first_reference_queue == null ) {
  //              usage.first_reference_queue = index.get(encode_key(queue_prefix, entry_record.queue_key), ro).map( x=> decode_queue_record(x) ).getOrElse(null)
  //            }
  //            usage.increment(entry_record.size)
  //          }
  //
  //          true
  //        }
  //      }
  //    }
  //
  //    import collection.JavaConversions._
  //    usage_map.values.toSeq.toArray
  //  }


  def export_data(os: OutputStream): Option[String] = {
    try {
      val manager = ExportStreamManager(os, 1)

      retry_using_index {

        // Delete all the tmp keys..
        index.cursor_keys_prefixed(Array(tmp_prefix)) {
          key =>
            index.delete(key)
            true
        }

        index.snapshot {
          snapshot =>
            val nocache = new ReadOptions
            nocache.snapshot(snapshot)
            nocache.verifyChecksums(verify_checksums)
            nocache.fillCache(false)

            val cache = new ReadOptions
            nocache.snapshot(snapshot)
            nocache.verifyChecksums(false)
            nocache.fillCache(false)

            // Build a temp table of all references messages by the queues
            // Remember 2 queues could reference the same message.
            index.cursor_prefixed(queue_entry_prefix_array, cache) {
              (_, value) =>
                val record = QueueEntryPB.FACTORY.parseUnframed(value)
                val (pos, len) = decode_locator(record.getMessageLocator)
                index.put(encode_key(tmp_prefix, pos), encode_vlong(len))
                true
            }

            // Use the temp table to export all the referenced messages. Use
            // the log position as the message key.
            index.cursor_prefixed(Array(tmp_prefix)) {
              (key, value) =>
                val (_, pos) = decode_long_key(key)
                val len = decode_vlong(value).toInt
                log.read(pos, len).foreach { data =>
                  val is = new DataByteArrayInputStream(data)
                  val record = MessagePB.FACTORY.parseFramed(is).copy()
                  var buffer = is.readBuffer(is.available())
                  if(record.getCompression == 1) {
                    buffer = Snappy.uncompress(buffer)
                  }
                  record.setMessageKey(pos)
                  record.setValue(buffer)
                  manager.store_message(record)
                }
                true
            }

            // Now export the queue entries
            index.cursor_prefixed(queue_entry_prefix_array, nocache) {
              (key, value) =>
                val (_, queue_key, queue_seq) = decode_long_long_key(key)
                val record = QueueEntryPB.FACTORY.parseUnframed(value).copy()
                val (pos, len) = decode_locator(record.getMessageLocator)
                record.setQueueKey(queue_key)
                record.setQueueSeq(queue_seq)
                record.setMessageKey(pos)
                manager.store_queue_entry(record)
                true
            }

            index.cursor_prefixed(queue_prefix_array) {
              (_, value) =>
                val record = QueuePB.FACTORY.parseUnframed(value)
                manager.store_queue(record)
                true
            }

            index.cursor_prefixed(map_prefix_array, nocache) {
              (key, value) =>
                val key_buffer = new Buffer(key)
                key_buffer.moveHead(1)
                val record = new MapEntryPB.Bean
                record.setKey(key_buffer)
                record.setValue(new Buffer(value))
                manager.store_map_entry(record)
                true
            }

        }

        // Delete all the tmp keys..
        index.cursor_keys_prefixed(Array(tmp_prefix)) {
          key =>
            index.delete(key)
            true
        }

      }
      manager.finish

      None
    } catch {
      case x: Exception =>
        debug(x, "Export failed")
        x.printStackTrace()
        Some(x.getMessage)
    }
  }

  def import_data(is: InputStream): Option[String] = {
    try {
      val manager = ImportStreamManager(is)
      if (manager.version != 1) {
        return Some("Cannot import from an export file of version: " + manager.version)
      }

      purge

      retry_using_index {
        log.appender {
          appender =>
            while (manager.getNext match {

              case record: MessagePB.Buffer =>
                val pb = new MessagePB.Bean
                pb.setCodec(record.getCodec)
                val body = if(snappy_compress_logs) {
                  val compressed = Snappy.compress(record.getValue)
                  if (compressed.length < record.getValue.length) {
                    pb.setCompression(1)
                    compressed
                  } else {
                    record.getValue
                  }
                } else {
                  record.getValue
                }
                var header = pb.freeze().toFramedBuffer
                val (pos, log_info) = appender.append(LOG_ADD_MESSAGE, header, body)
                index.put(encode_key(tmp_prefix, record.getMessageKey), encode_locator(pos, header.length+body.length))
                true

              case record: QueueEntryPB.Buffer =>
                val copy = record.copy();
                var original_msg_key: Long = record.getMessageKey
                index.get(encode_key(tmp_prefix, original_msg_key)) match {
                  case Some(locator) =>
                    val (pos, len) = decode_locator(locator)
                    copy.setMessageLocator(locator)
                    index.put(encode_key(queue_entry_prefix, record.getQueueKey, record.getQueueSeq), copy.freeze().toUnframedBuffer)
                    for(key <- log_ref_key(pos)) {
                      log_refs.getOrElseUpdate(key, new LongCounter()).incrementAndGet()
                    }
                  case None =>
                    println("Invalid queue entry, references message that was not in the export: " + original_msg_key)
                }
                true

              case record: QueuePB.Buffer =>
                index.put(encode_key(queue_prefix, record.getKey), record.toUnframedBuffer)
                true

              case record: MapEntryPB.Buffer =>
                index.put(encode_key(map_prefix, record.getKey), record.getValue)
                true

              case null =>
                false
            }) {
              // keep looping
            }

        }
      }

      store_log_refs
      // Delete all the tmp keys..
      index.cursor_keys_prefixed(Array(tmp_prefix)) {
        key =>
          index.delete(key)
          true
      }

      snapshot_index
      None

    } catch {
      case x: Exception =>
        debug(x, "Import failed")
        Some(x.getMessage)
    }
  }
}
