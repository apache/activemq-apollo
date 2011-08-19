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
package org.apache.activemq.apollo.broker.store.jdbm2

import dto.JDBM2StoreDTO
import java.{lang=>jl}
import java.{util=>ju}

import collection.mutable.ListBuffer
import org.apache.activemq.apollo.broker.store._
import org.apache.activemq.apollo.util._
import jdbm._
import btree.BTree
import htree.HTree
import java.util.Comparator
import jdbm.helper._
import PBSupport._
import org.fusesource.hawtbuf.proto.PBMessageFactory
import java.io._
import org.fusesource.hawtbuf.Buffer

object JDBM2Client extends Log {

  object MessageRecordSerializer extends Serializer[MessagePB.Buffer] {
    def serialize(out: SerializerOutput, v: MessagePB.Buffer) = v.writeUnframed(out)
    def deserialize(in: SerializerInput) = MessagePB.FACTORY.parseUnframed(in).freeze
  }

  object QueueRecordSerializer extends Serializer[QueueRecord] {
    def serialize(out: SerializerOutput, v: QueueRecord) = encode_queue_record(out, v)
    def deserialize(in: SerializerInput) = decode_queue_record(in)
  }

  object QueueEntryRecordSerializer extends Serializer[QueueEntryRecord] {
    def serialize(out: SerializerOutput, v: QueueEntryRecord) = encode_queue_entry_record(out, v)
    def deserialize(in: SerializerInput) = decode_queue_entry_record(in)
  }

  object ZeroCopyValueSerializer extends Serializer[(Int, Long, Int)] {
    def serialize(out: SerializerOutput, v: (Int,Long, Int)) = {
      out.writePackedInt(v._1)
      out.writePackedLong(v._2)
      out.writePackedInt(v._3)
    }

    def deserialize(in: SerializerInput) = {
      (in.readPackedInt, in.readPackedLong, in.readPackedInt)
    }
  }

  object BufferSerializer extends Serializer[Buffer] {
    def serialize(out: SerializerOutput, v: Buffer) = {
      out.writePackedInt(v.length())
      out.write(v.data, v.offset, v.length)
    }

    def deserialize(in: SerializerInput) = {
      val rc = new Buffer(in.readPackedInt());
      in.readFully(rc.data)
      rc
    }
  }

  object QueueEntryKeySerializer extends Serializer[(Long,Long)] {
    def serialize(out: SerializerOutput, v: (Long,Long)) = {
      out.writePackedLong(v._1)
      out.writePackedLong(v._2)
    }

    def deserialize(in: SerializerInput) = {
      (in.readPackedLong, in.readPackedLong)
    }
  }

  class QueueEntryKeyComparator extends Comparator[(Long,Long)] with Serializable {
    def compare(o1: (Long, Long), o2: (Long, Long)) = {
      val rc = o1._1.compareTo(o2._1)
      if( rc==0 ) {
        o1._2.compareTo(o2._2)
      } else {
        rc
      }
    }
  }

  val QueueEntryKeyComparator = new QueueEntryKeyComparator

  final class RichBTree[K,V](val self: BTree[K,V]) {

    def cursor(func: (K,V) => Boolean): Unit = {
      val browser = self.browse
      val entry = new Tuple[K,V]
      while( browser.getNext(entry) && func(entry.getKey, entry.getValue) ) {
      }
    }

    def cursor_range(start_inclusive:K, end_exclusive:K)(func: (K,V)=>Unit): Unit = {
      val browser = self.browse(start_inclusive)
      val entry = new Tuple[K,V]
      def in_range = self.getComparator.compare(entry.getKey,end_exclusive) < 0
      while( browser.getNext(entry) && in_range ) {
        func(entry.getKey, entry.getValue)
      }
    }

  }

  implicit def btree_to_rich_btree[K,V](x: BTree[K,V]) = new RichBTree[K,V](x)

  final class RichHTree[K,V](val self: HTree[K,V]) {

    def cursor(func: (K,V) => Boolean): Unit = {
      val i = self.keys
      var keep_going = true
      while( keep_going && i.hasNext() ) {
        val key = i.next
        val value = self.find(key)
        keep_going = func(key, value)
      }
    }

  }

  implicit def htree_to_rich_btree[K,V](x: HTree[K,V]) = new RichHTree[K,V](x)

}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class JDBM2Client(store: JDBM2Store) {

  import JDBM2Client._


  def dispatchQueue = store.dispatch_queue

  /////////////////////////////////////////////////////////////////////
  //
  // Public interface used by the BDBStore
  //
  /////////////////////////////////////////////////////////////////////

  var config: JDBM2StoreDTO = null

  var recman:RecordManager = _

  var queues_db:HTree[Long, QueueRecord] = _
  var entries_db:BTree[(Long,Long), QueueEntryRecord] = _
  var messages_db:HTree[Long, MessagePB.Buffer] = _
  var zerocp_db:HTree[Long, (Int, Long, Int)] = _
  var message_refs_db:HTree[Long, java.lang.Integer] = _
  var map_db:HTree[Buffer, Buffer] = _

  var last_message_key = 0L
  var last_queue_key = 0L

  var zero_copy_buffer_allocator: FileZeroCopyBufferAllocator = _

  def zero_copy_dir = {
    import FileSupport._
    config.directory / "zerocp"
  }

  def start() = {

    Thread.currentThread.setContextClassLoader(getClass.getClassLoader)
    import FileSupport._

    config.directory.mkdirs

    if( Option(config.zero_copy).map(_.booleanValue).getOrElse(false) ) {
      zero_copy_buffer_allocator = new FileZeroCopyBufferAllocator(zero_copy_dir)
      zero_copy_buffer_allocator.start
    }

    recman = RecordManagerFactory.createRecordManager((config.directory / "jdbm2").getCanonicalPath)

    def init_btree[K, V](name: String, key_comparator:Comparator[K]=ComparableComparator.INSTANCE.asInstanceOf[Comparator[K]], key_serializer:Serializer[K]=null, value_serializer:Serializer[V]=null) = {
      val recid = recman.getNamedObject(name)
      var rc: BTree[K, V] = if (recid == 0) {
        val rc = BTree.createInstance[K, V](recman, key_comparator, key_serializer, value_serializer);
        recman.setNamedObject(name, rc.getRecid)
        rc
      } else {
        BTree.load[K, V](recman, recid)
      }
      rc.setKeySerializer(key_serializer)
      rc.setValueSerializer(value_serializer)
      rc
    }

    def init_htree[K, V](name: String, key_serializer:Serializer[K]=null, value_serializer:Serializer[V]=null) = {
      val recid = recman.getNamedObject(name)
      var rc: HTree[K, V] = if (recid == 0) {
        val rc = HTree.createInstance[K, V](recman, key_serializer, value_serializer);
        recman.setNamedObject(name, rc.getRecid)
        rc
      } else {
        HTree.load[K, V](recman, recid, key_serializer, value_serializer)
      }
      rc
    }

    transaction {
      messages_db = init_htree("messages", value_serializer = MessageRecordSerializer)
      map_db = init_htree("map", value_serializer = BufferSerializer, key_serializer = BufferSerializer)
      zerocp_db = init_htree("lobs", value_serializer = ZeroCopyValueSerializer)
      message_refs_db = init_htree("message_refs")
      queues_db = init_htree("queues", value_serializer = QueueRecordSerializer)
      entries_db = init_btree("enttries", new QueueEntryKeyComparator, QueueEntryKeySerializer, QueueEntryRecordSerializer)

      last_message_key = Option(recman.getNamedObject("last_message_key")).map(_.longValue).getOrElse(0L)
      last_queue_key = Option(recman.getNamedObject("last_queue_key")).map(_.longValue).getOrElse(0L)

      if( zero_copy_buffer_allocator!=null ) {
        zerocp_db.cursor { (_,v)=>
          zero_copy_buffer_allocator.alloc_at(v._1, v._2, v._3)
          true
        }
      }
    }

  }

  def stop() = {
    recman.close
    recman = null;
    if( zero_copy_buffer_allocator!=null ) {
      zero_copy_buffer_allocator.stop
      zero_copy_buffer_allocator = null
    }
  }

  def transaction[T](func: => T): T = {
    var ok = false
    try {
      val rc = func
      ok = true
      rc
    } finally {
      if(ok){
        recman.commit
      } else {
        recman.rollback
      }
    }
  }


  def purge() = {
    def delete_files = {
      if( config.directory.isDirectory ) {
        config.directory.listFiles.filter(_.getName.startsWith("jdbm2.")).foreach(_.delete)
      }
      if( zero_copy_dir.isDirectory ) {
        zero_copy_dir.listFiles.foreach(_.delete)
      }
    }
    if( recman!=null ) {
      stop
      delete_files
      start
    } else {
      delete_files
    }
  }

  def addQueue(record: QueueRecord, callback:Runnable) = {
    transaction {
      if( record.key > last_queue_key ) {
        last_queue_key = record.key
        recman.setNamedObject("last_queue_key", last_queue_key)
      }
      queues_db.put(record.key, record)
    }
    callback.run
  }

  def add_and_get[K](db:HTree[K,java.lang.Integer], key:K, amount:Int):Int = {
    db.find(key) match {
      case null =>
        if( amount!=0 ) {
          db.put(key, amount)
        }
        amount

      case value =>
        val counter = value.intValue
        val update = counter + amount
        if( update == 0 ) {
          db.remove(key)
        } else {
          db.put(key, update)
        }
        update
    }
  }

  def compact = {
    val gc = ListBuffer[Long]()
    message_refs_db.cursor { (key, refs)=>
      if( refs == 0 ) {
        gc += key
      }
      true
    }
    transaction {
      gc.foreach { key=>
        message_refs_db.remove(key)
        messages_db.remove(key)
        if( zero_copy_buffer_allocator!=null ){
          val location = zerocp_db.find(key)
          if( location!=null ) {
            zero_copy_buffer_allocator.free(location._1, location._2, location._3)
            zerocp_db.remove(key)
          }
        }
      }
    }
    recman.defrag
  }

  def add_message_reference(key:Long)={
    message_refs_db.find(key) match {
      case null =>
        message_refs_db.put(key, 1)
      case value =>
        message_refs_db.put(key, value.intValue+1)
    }
  }

  def remove_message_reference(key:Long)={
    message_refs_db.find(key) match {
      case null =>
        message_refs_db.put(key, 0)
      case value =>
        message_refs_db.put(key, value.intValue-1)
    }
  }

  def removeQueue(queue_key: Long, callback:Runnable) = {
    transaction {
      queues_db.remove(queue_key)
      entries_db.cursor_range( (queue_key, 0L), (queue_key+1, 0L)) { (key,value)=>
        entries_db.remove(key)
        val queue_entry:QueueEntryRecord = value
        remove_message_reference(queue_entry.message_key)
      }
    }
    callback.run
  }

  def store(uows: Seq[JDBM2Store#DelayableUOW], callback:Runnable) {
    transaction {
      var zcp_files_to_sync = Set[Int]()
      uows.foreach { uow =>

        for((key,value) <- uow.map_actions) {
          if( value==null ) {
            map_db.remove(key)
          } else {
            map_db.put(key, value)
          }
        }

        uow.actions.foreach { case (msg, action) =>

          val message_record = action.message_record
          if (message_record != null) {

            val pb = if( message_record.zero_copy_buffer != null ) {
              val r = to_pb(action.message_record).copy
              val buffer = zero_copy_buffer_allocator.to_alloc_buffer(message_record.zero_copy_buffer)
              r.setZcpFile(buffer.file)
              r.setZcpOffset(buffer.offset)
              r.setZcpSize(buffer.size)
              zerocp_db.put(message_record.key, (buffer.file, buffer.offset, buffer.size))
              zcp_files_to_sync += buffer.file
              r.freeze
            } else {
              to_pb(action.message_record)
            }

            messages_db.put(action.message_record.key, pb)
            if( action.message_record.key > last_message_key ) {
              last_message_key = action.message_record.key
              recman.setNamedObject("last_message_key", last_message_key)
            }
          }

          action.enqueues.foreach { queue_entry =>
            entries_db.insert((queue_entry.queue_key, queue_entry.entry_seq), queue_entry, true)
            add_message_reference(queue_entry.message_key)
          }

          action.dequeues.foreach { queue_entry =>
            entries_db.remove((queue_entry.queue_key, queue_entry.entry_seq))
            remove_message_reference(queue_entry.message_key)
          }

        }
      }
      if( zero_copy_buffer_allocator!=null ) {
        zcp_files_to_sync.foreach(zero_copy_buffer_allocator.sync(_))
      }
    }
    callback.run
  }

  def listQueues: Seq[Long] = {
    val rc = ListBuffer[Long]()
    queues_db.cursor { (key, _) =>
      rc += key
      true // to continue cursoring.
    }
    rc
  }

  def getQueue(queue_key: Long): Option[QueueRecord] = {
    Option(queues_db.find(queue_key))
  }

  def listQueueEntryGroups(queue_key: Long, limit: Int) : Seq[QueueEntryRange] = {
    var rc = ListBuffer[QueueEntryRange]()
    var group:QueueEntryRange = null

    entries_db.cursor_range( (queue_key, 0L), (queue_key+1, 0L)) { (key,entry)=>

      val seq = key._2

      if( group == null ) {
        group = new QueueEntryRange
        group.first_entry_seq = seq
      }

      group.last_entry_seq = seq
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
    }

    if( group!=null ) {
      rc += group
    }
    rc
  }

  def getQueueEntries(queue_key: Long, firstSeq:Long, lastSeq:Long): Seq[QueueEntryRecord] = {
    var rc = ListBuffer[QueueEntryRecord]()
    entries_db.cursor_range( (queue_key, firstSeq), (queue_key, lastSeq+1)) { (_,entry)=>
      rc += entry
    }
    rc
  }

  val metric_load_from_index_counter = new TimeCounter
  var metric_load_from_index = metric_load_from_index_counter(false)

  def loadMessages(requests: ListBuffer[(Long, (Option[MessageRecord])=>Unit)]) = {
    requests.foreach { case (message_key, callback)=>
      val record = metric_load_from_index_counter.time {
        Option(messages_db.find(message_key)).map{ pb=>
          val rc = from_pb(pb)
          if( pb.hasZcpFile ) {
            rc.zero_copy_buffer = zero_copy_buffer_allocator.view_buffer(pb.getZcpFile, pb.getZcpOffset, pb.getZcpSize)
          }
          rc
        }
      }
      callback(record)
    }
  }


  def getLastMessageKey:Long = last_message_key

  def getLastQueueKey:Long = last_queue_key

  def get(key: Buffer):Option[Buffer] = {
    Option(map_db.find(key))
  }

  def export_pb(streams:StreamManager[OutputStream]):Result[Zilch,String] = {
    try {
      import PBSupport._

      streams.using_map_stream { stream=>
        map_db.cursor { (key, value) =>
          val record = new MapEntryPB.Bean
          record.setKey(key)
          record.setValue(value)
          record.freeze().writeFramed(stream)
          true
        }
      }

      streams.using_queue_stream { queue_stream=>
        queues_db.cursor { (_, value) =>
          val record:QueueRecord = value
          record.writeFramed(queue_stream)
          true
        }
      }
      streams.using_message_stream { message_stream=>
        messages_db.cursor { (_, pb) =>
          if( pb.hasZcpFile ) {
            val zcpb = zero_copy_buffer_allocator.view_buffer(pb.getZcpFile, pb.getZcpOffset, pb.getZcpSize)
            var data = pb.copy
            data.clearZcpFile
            data.clearZcpFile
            // write the pb frame and then the direct buffer data..
            data.freeze.writeFramed(message_stream)
            zcpb.read(message_stream)
          } else {
            pb.writeFramed(message_stream)
          }
          true
        }
      }

      streams.using_queue_entry_stream { queue_entry_stream=>
        entries_db.cursor { (_, value) =>
          val record:QueueEntryRecord = value
          record.writeFramed(queue_entry_stream)
          true
        }
      }
      Success(Zilch)

    } catch {
      case x:Exception=>
        Failure(x.getMessage)
    }
  }

  def import_pb(streams:StreamManager[InputStream]):Result[Zilch,String] = {
    try {
      purge

      var size =0
      def check_flush(incr:Int, max:Int) = {
        size += incr
        if( size > max ) {
          recman.commit
          size = 0
        }
      }

      transaction {

        def foreach[B] (stream:InputStream, fact:PBMessageFactory[_,_])(func: (B)=>Unit):Unit = {
          var done = false
          do {
            try {
              func(fact.parseFramed(stream).asInstanceOf[B])
            } catch {
              case x:EOFException =>
                done = true
            }
          } while( !done )
        }


        import PBSupport._

        streams.using_map_stream { stream=>
          foreach[MapEntryPB.Buffer](stream, MapEntryPB.FACTORY) { pb =>
            map_db.put(pb.getKey, pb.getValue)
            check_flush(1, 10000)
          }
        }

        streams.using_queue_stream { queue_stream=>
          foreach[QueuePB.Buffer](queue_stream, QueuePB.FACTORY) { pb =>
            val record:QueueRecord = pb
            queues_db.put(record.key, record)
            check_flush(1, 10000)
          }
        }

        recman.commit

        var zcp_counter = 0
        val max_ctx = zero_copy_buffer_allocator.contexts.size

        streams.using_message_stream { message_stream=>
          foreach[MessagePB.Buffer](message_stream, MessagePB.FACTORY) { pb=>

            val record:MessagePB.Buffer = if( pb.hasZcpSize ) {
              val cp = pb.copy
              val zcpb = zero_copy_buffer_allocator.contexts(zcp_counter % max_ctx).alloc(cp.getZcpSize)
              cp.setZcpFile(zcpb.file)
              cp.setZcpOffset(zcpb.offset)

              zcp_counter += 1
              zcpb.write(message_stream)

              zerocp_db.put(pb.getMessageKey, (zcpb.file, zcpb.offset, zcpb.size))
              cp.freeze
            } else {
              pb
            }

            messages_db.put(record.getMessageKey, record)
            check_flush(record.getSize, 1024*124*10)
          }
        }

        recman.commit

        streams.using_queue_entry_stream { queue_entry_stream=>
          foreach[QueueEntryPB.Buffer](queue_entry_stream, QueueEntryPB.FACTORY) { pb=>
            val record:QueueEntryRecord = pb
            entries_db.insert((record.queue_key, record.entry_seq), record, true)
            add_message_reference(record.message_key)
            check_flush(1, 10000)
          }
        }

      }
      Success(Zilch)

    } catch {
      case x:Exception=>
        Failure(x.getMessage)
    }
  }
}
