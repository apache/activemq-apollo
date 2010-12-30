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
import java.nio.ByteBuffer
import com.sleepycat.je._
import java.io.Serializable
import org.apache.activemq.apollo.broker.store.{PBSupport, MessageRecord, QueueRecord, QueueEntryRecord}
import PBSupport._

object HelperTrait {

  implicit def to_message_record(entry: DatabaseEntry): MessageRecord = entry.getData
  implicit def to_database_entry(v: MessageRecord): DatabaseEntry = new DatabaseEntry(v)

  implicit def to_queue_entry_record(entry: DatabaseEntry): QueueEntryRecord = entry.getData
  implicit def to_database_entry(v: QueueEntryRecord): DatabaseEntry = new DatabaseEntry(v)

  implicit def to_queue_record(entry: DatabaseEntry): QueueRecord = entry.getData
  implicit def to_database_entry(v: QueueRecord): DatabaseEntry = new DatabaseEntry(v)


  implicit def to_bytes(l:Long):Array[Byte] = ByteBuffer.wrap(new Array[Byte](8)).putLong(l).array()
  implicit def to_long(bytes:Array[Byte]):Long = ByteBuffer.wrap(bytes).getLong()
  implicit def to_database_entry(l:Long):DatabaseEntry = new DatabaseEntry(to_bytes(l))
  implicit def to_long(value:DatabaseEntry):Long = to_long(value.getData)

  implicit def to_bytes(l:Int):Array[Byte] = ByteBuffer.wrap(new Array[Byte](4)).putInt(l).array()
  implicit def to_int(bytes:Array[Byte]):Int = ByteBuffer.wrap(bytes).getInt()
  implicit def to_database_entry(l:Int):DatabaseEntry = new DatabaseEntry(to_bytes(l))
  implicit def to_int(value:DatabaseEntry):Int = to_int(value.getData)



  class LongComparator extends Comparator[Array[Byte]] with Serializable {

    def compare(o1: Array[Byte], o2: Array[Byte]) = {
        val v1:java.lang.Long = to_long(o1)
        val v2:java.lang.Long = to_long(o2)
        v1.compareTo(v2)
    }
    
  }

  val long_key_conf = new DatabaseConfig();
  long_key_conf.setAllowCreate(true)
  long_key_conf.setTransactional(true);
  long_key_conf.setBtreeComparator(new LongComparator)

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
