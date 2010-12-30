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
import java.io.Serializable
import jdbm.helper._
import PBSupport._

object JDBM2Client extends Log {

  object MessageRecordSerializer extends Serializer[MessageRecord] {
    def serialize(out: SerializerOutput, v: MessageRecord) = encode_message_record(out, v)
    def deserialize(in: SerializerInput) = decode_message_record(in)
  }

  object QueueRecordSerializer extends Serializer[QueueRecord] {
    def serialize(out: SerializerOutput, v: QueueRecord) = encode_queue_record(out, v)
    def deserialize(in: SerializerInput) = decode_queue_record(in)
  }

  object QueueEntryRecordSerializer extends Serializer[QueueEntryRecord] {
    def serialize(out: SerializerOutput, v: QueueEntryRecord) = encode_queue_entry_record(out, v)
    def deserialize(in: SerializerInput) = decode_queue_entry_record(in)
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

  implicit def to_rich_btree[K,V](x: BTree[K,V]) = new RichBTree[K,V](x)

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

  implicit def to_rich_btree[K,V](x: HTree[K,V]) = new RichHTree[K,V](x)

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
  var messages_db:HTree[Long, MessageRecord] = _
  var message_refs_db:HTree[Long, java.lang.Integer] = _

  var last_message_key = 0L
  var last_queue_key = 0L

  def start() = {

    config.directory.mkdirs

    import FileSupport._
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
      message_refs_db = init_htree("message_refs")
      queues_db = init_htree("queues", value_serializer = QueueRecordSerializer)
      entries_db = init_btree("enttries", new QueueEntryKeyComparator, QueueEntryKeySerializer, QueueEntryRecordSerializer)

      last_message_key = Option(recman.getNamedObject("last_message_key")).map(_.longValue).getOrElse(0L)
      last_queue_key = Option(recman.getNamedObject("last_queue_key")).map(_.longValue).getOrElse(0L)
    }

  }

  def stop() = {
    recman.close
    recman = null;
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
      uows.foreach { uow =>
        uow.actions.foreach { case (msg, action) =>

          if (action.messageRecord != null) {
            messages_db.put(action.messageRecord.key, action.messageRecord)
            if( action.messageRecord.key > last_message_key ) {
              last_message_key = action.messageRecord.key
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
    val records =  requests.flatMap { case (message_key, callback)=>
      val record = metric_load_from_index_counter.time {
        Option(messages_db.find(message_key))
      }
      record match {
        case None =>
        debug("Message not indexed: %s", message_key)
        callback(None)
        None
        case Some(x) => Some((record, callback))
      }
    }

    records.foreach { case (record, callback)=>
      callback( record )
    }

  }


  def getLastMessageKey:Long = last_message_key

  def getLastQueueKey:Long = last_queue_key

}
