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

import java.util.Comparator
import com.sleepycat.je._
import java.io.Serializable
import org.apache.activemq.apollo.broker.store._
import PBSupport._
import org.fusesource.hawtbuf._
import language.implicitConversions

object HelperTrait {

  implicit def to_message_buffer(entry: DatabaseEntry): MessagePB.Buffer = MessagePB.FACTORY.parseFramed(entry.getData)
  implicit def to_database_entry(v: MessagePB.Buffer): DatabaseEntry = new DatabaseEntry(v.toFramedByteArray)

  implicit def to_queue_entry_record(entry: DatabaseEntry): QueueEntryRecord = entry.getData
  implicit def to_database_entry(v: QueueEntryRecord): DatabaseEntry = new DatabaseEntry(v)

  implicit def to_queue_record(entry: DatabaseEntry): QueueRecord = entry.getData
  implicit def to_database_entry(v: QueueRecord): DatabaseEntry = new DatabaseEntry(v)

  implicit def to_buffer(entry: DatabaseEntry): Buffer = new Buffer(entry.getData)
  implicit def to_database_entry(v: Buffer): DatabaseEntry = new DatabaseEntry(v.toByteArray)

  implicit def decode_lob_value(entry: DatabaseEntry): (Long,Int) = {
    val in = new DataByteArrayInputStream(entry.getData)
    (in.readVarLong(), in.readVarInt())
  }
  implicit def encode_zcp_value(v: (Long,Int)): DatabaseEntry = {
    val out = new DataByteArrayOutputStream(
      AbstractVarIntSupport.computeVarLongSize(v._1) +
      AbstractVarIntSupport.computeVarIntSize(v._2)
    )
    out.writeVarLong(v._1)
    out.writeVarInt(v._2)
    new DatabaseEntry(out.toBuffer.data)
  }

  implicit def to_bytes(l:Long):Array[Byte] = {
    val out = new DataByteArrayOutputStream(AbstractVarIntSupport.computeVarLongSize(l))
    out.writeVarLong(l)
    out.toBuffer.data
  }
  implicit def to_long(bytes:Array[Byte]):Long = {
    val in = new DataByteArrayInputStream(bytes)
    in.readVarLong()
  }

  implicit def to_database_entry(l:Long):DatabaseEntry = new DatabaseEntry(to_bytes(l))
  implicit def to_long(value:DatabaseEntry):Long = to_long(value.getData)

  implicit def to_bytes(l:(Long, Long)):Array[Byte] = {
    val out = new DataByteArrayOutputStream(
      AbstractVarIntSupport.computeVarLongSize(l._1)+
      AbstractVarIntSupport.computeVarLongSize(l._2)
    )
    out.writeVarLong(l._1)
    out.writeVarLong(l._2)
    out.toBuffer.data
  }

  implicit def to_long_long(bytes:Array[Byte]):(Long,Long) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readVarLong(), in.readVarLong())
  }

  implicit def to_database_entry(l:(Long,Long)):DatabaseEntry = new DatabaseEntry(to_bytes(l))
  implicit def to_long_long(value:DatabaseEntry):(Long,Long) = to_long_long(value.getData)

  implicit def to_bytes(l:Int):Array[Byte] = {
    val out = new DataByteArrayOutputStream(AbstractVarIntSupport.computeVarIntSize(l))
    out.writeVarInt(l)
    out.toBuffer.data
  }

  implicit def to_int(bytes:Array[Byte]):Int = {
    val in = new DataByteArrayInputStream(bytes)
    in.readVarInt()
  }

  implicit def to_database_entry(l:Int):DatabaseEntry = new DatabaseEntry(to_bytes(l))
  implicit def to_int(value:DatabaseEntry):Int = to_int(value.getData)


  @SerialVersionUID(1)
  class LongComparator extends Comparator[Array[Byte]] with Serializable {

    def compare(o1: Array[Byte], o2: Array[Byte]) = {
        val v1 = to_long(o1)
        val v2 = to_long(o2)
        v1.compareTo(v2)
    }
    
  }

  @SerialVersionUID(2)
  class LongLongComparator extends Comparator[Array[Byte]] with Serializable {

    def compare(o1: Array[Byte], o2: Array[Byte]) = {
      val v1 = to_long_long(o1)
      val v2 = to_long_long(o2)
      val rc = v1._1.compareTo(v2._1)
      if (rc==0) {
        v1._2.compareTo(v2._2)
      } else {
        rc
      }
    }

  }

  val buffer_key_conf = new DatabaseConfig();
  buffer_key_conf.setAllowCreate(true)
  buffer_key_conf.setTransactional(true);
  buffer_key_conf.setSortedDuplicates(false);

  val long_key_conf = new DatabaseConfig();
  long_key_conf.setAllowCreate(true)
  long_key_conf.setTransactional(true);
  long_key_conf.setBtreeComparator(new LongComparator)
  long_key_conf.setSortedDuplicates(false);

  val long_long_key_conf = new DatabaseConfig();
  long_long_key_conf.setAllowCreate(true)
  long_long_key_conf.setTransactional(true);
  long_long_key_conf.setBtreeComparator(new LongLongComparator)
  long_long_key_conf.setSortedDuplicates(false);

  final class RichDatabase(val db: Database) extends Proxy {
    def self: Any = db

    def with_cursor[T](tx:Transaction)(func: (Cursor) => T): T = {
      val cursor = db.openCursor(tx, null)
      try {
        func(cursor)
      } finally {
        cursor.close
      }
    }

    def cursor(tx:Transaction)(func: (DatabaseEntry,DatabaseEntry) => Boolean): Unit = {
      with_cursor(tx) { cursor=>
        val key = new DatabaseEntry();
        val data = new DatabaseEntry();
        while ( cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS && func(key, data) ) {
        }
      }
    }

    def cursor_from(tx:Transaction, start:DatabaseEntry)(func: (DatabaseEntry,DatabaseEntry) => Boolean): Unit = {
      with_cursor(tx) { cursor=>
        val key = new DatabaseEntry(start.getData)
        val data = new DatabaseEntry();
        if (cursor.getSearchKeyRange(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS && func(key, data) ) {
          while (cursor.getNext(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS && func(key, data)) {
          }
        }
      }
    }

    def cursor_prefixed(tx:Transaction, prefix:Buffer)(func: (DatabaseEntry,DatabaseEntry) => Boolean): Unit = {
      cursor_from(tx, prefix) { (key, value) =>
        if( key.startsWith(prefix) ) {
          func(key, value)
        } else {
          false
        }
      }
    }

    def get(tx:Transaction, key:DatabaseEntry):Option[DatabaseEntry] = {
      val value = new DatabaseEntry()
      if( db.get(tx, key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS ) {
        Some(value)
      } else {
        None
      }
    }

    def last_key(tx:Transaction): Option[DatabaseEntry] = {
      with_cursor(tx) { cursor=>
        val key = new DatabaseEntry()
        val data = new DatabaseEntry();
        if (cursor.getPrev(key, data, LockMode.DEFAULT) == OperationStatus.SUCCESS ) {
          Some(key)
        } else {
          None

        }
      }
    }
  }

  implicit def to_rich_database(x: Database) = new RichDatabase(x)


  def entries_db_name(queue_key: Long): String =  "entries-" + queue_key

}
