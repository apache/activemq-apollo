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

import org.fusesource.hawtbuf._
import org.iq80.leveldb._
import java.io.DataOutput
import org.fusesource.leveldbjni.internal.JniDB

object HelperTrait {

  def encode_locator(pos: Long, len: Int): Array[Byte] = {
    val out = new DataByteArrayOutputStream(
      AbstractVarIntSupport.computeVarLongSize(pos) +
        AbstractVarIntSupport.computeVarIntSize(len)
    )
    out.writeVarLong(pos)
    out.writeVarInt(len)
    out.getData
  }

  def decode_locator(bytes: Array[Byte]): (Long, Int) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readVarLong(), in.readVarInt())
  }

  def decode_locator(bytes: Buffer): (Long, Int) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readVarLong(), in.readVarInt())
  }

  def encode_vlong(a1: Long): Array[Byte] = {
    val out = new DataByteArrayOutputStream(
      AbstractVarIntSupport.computeVarLongSize(a1)
    )
    out.writeVarLong(a1)
    out.getData
  }

  def decode_vlong(bytes: Array[Byte]): Long = {
    val in = new DataByteArrayInputStream(bytes)
    in.readVarLong()
  }

  def encode_key(a1: Byte, a2: Long): Array[Byte] = {
    val out = new DataByteArrayOutputStream(9)
    out.writeByte(a1.toInt)
    out.writeLong(a2)
    out.getData
  }

  def encode_key(a1: Byte, a2: Buffer): Array[Byte] = {
    val out = new DataByteArrayOutputStream(1 + a2.length)
    out.writeByte(a1.toInt)
    a2.writeTo(out.asInstanceOf[DataOutput])
    out.getData
  }

  def decode_long_key(bytes: Array[Byte]): (Byte, Long) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readByte(), in.readLong())
  }

  def encode_key(a1: Byte, a2: Long, a3: Long): Array[Byte] = {
    val out = new DataByteArrayOutputStream(17)
    out.writeByte(a1)
    out.writeLong(a2)
    out.writeLong(a3)
    out.getData
  }

  def decode_long_long_key(bytes: Array[Byte]): (Byte, Long, Long) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readByte(), in.readLong(), in.readLong())
  }

  def encode(a1: Byte, a2: Int): Array[Byte] = {
    val out = new DataByteArrayOutputStream(5)
    out.writeByte(a1)
    out.writeInt(a2)
    out.getData
  }

  def decode_int_key(bytes: Array[Byte]): (Byte, Int) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readByte(), in.readInt())
  }

  final class RichDB(val db: DB) {

    val is_pure_java_version = db.getClass.getName == "org.iq80.leveldb.impl.DbImpl"

    def getProperty(name: String) = db.getProperty(name)

    def getApproximateSizes(ranges: Range*) = db.getApproximateSizes(ranges: _*)

    def get(key: Array[Byte], ro: ReadOptions = new ReadOptions): Option[Array[Byte]] = {
      Option(db.get(key, ro))
    }

    def close: Unit = db.close()

    def delete(key: Array[Byte], wo: WriteOptions = new WriteOptions): Unit = {
      db.delete(key, wo)
    }

    def put(key: Array[Byte], value: Array[Byte], wo: WriteOptions = new WriteOptions): Unit = {
      db.put(key, value, wo)
    }

    def write[T](wo: WriteOptions = new WriteOptions)(func: WriteBatch => T): T = {
      val updates = db.createWriteBatch()
      try {

        val rc = Some(func(updates))
        might_trigger_compaction(db.write(updates, wo))
        return rc.get
      } finally {
        updates.close();
      }
    }

    def snapshot[T](func: Snapshot => T): T = {
      val snapshot = db.getSnapshot
      try {
        func(snapshot)
      } finally {
        snapshot.close()
      }
    }

    def cursor_keys(ro: ReadOptions = new ReadOptions)(func: Array[Byte] => Boolean): Unit = {
      val iterator = db.iterator(ro)
      iterator.seekToFirst();
      try {
        while (iterator.hasNext && func(iterator.peekNext.getKey)) {
          iterator.next()
        }
      } finally {
        iterator.close();
      }
    }

    def compact = {
      compact_needed = false
      db match {
        case db:JniDB =>
          db.compactRange(null, null)
//        case db:DbImpl =>
//          val start = new Slice(Array[Byte]('a'.toByte))
//          val end = new Slice(Array[Byte]('z'.toByte))
//          db.compactRange(2, start, end)
        case _ =>
      }
    }

    private def might_trigger_compaction[T](func: => T): T = {
      val start = System.nanoTime()
      try {
        func
      } finally {
        val duration = System.nanoTime() - start
        // If it takes longer than 100 ms..
        if( duration > 1000000*100 ) {
          compact_needed = true
        }
      }
    }

    @volatile
    var compact_needed = false

    def cursor_keys_prefixed(prefix: Array[Byte], ro: ReadOptions = new ReadOptions)(func: Array[Byte] => Boolean): Unit = {
      val iterator = db.iterator(ro)
      might_trigger_compaction(iterator.seek(prefix))
      try {
        def check(key: Array[Byte]) = {
          key.startsWith(prefix) && func(key)
        }
        while (iterator.hasNext && check(iterator.peekNext.getKey)) {
          iterator.next()
        }
      } finally {
        iterator.close();
      }
    }

    def cursor_prefixed(prefix: Array[Byte], ro: ReadOptions = new ReadOptions)(func: (Array[Byte], Array[Byte]) => Boolean): Unit = {
      val iterator = db.iterator(ro)
      might_trigger_compaction( iterator.seek(prefix) )
      try {
        def check(key: Array[Byte]) = {
          key.startsWith(prefix) && func(key, iterator.peekNext.getValue)
        }
        while (iterator.hasNext && check(iterator.peekNext.getKey)) {
          iterator.next()
        }
      } finally {
        iterator.close();
      }
    }

    def compare(a1: Array[Byte], a2: Array[Byte]): Int = {
      new Buffer(a1).compareTo(new Buffer(a2))
    }

    def cursor_range_keys(start_included: Array[Byte], end_excluded: Array[Byte], ro: ReadOptions = new ReadOptions)(func: Array[Byte] => Boolean): Unit = {
      val iterator = db.iterator(ro)
      might_trigger_compaction(iterator.seek(start_included))
      try {
        def check(key: Array[Byte]) = {
          if (compare(key, end_excluded) < 0) {
            func(key)
          } else {
            false
          }
        }
        while (iterator.hasNext && check(iterator.peekNext.getKey)) {
          iterator.next()
        }
      } finally {
        iterator.close();
      }
    }

    def cursor_range(start_included: Array[Byte], end_excluded: Array[Byte], ro: ReadOptions = new ReadOptions)(func: (Array[Byte], Array[Byte]) => Boolean): Unit = {
      val iterator = db.iterator(ro)
      might_trigger_compaction(iterator.seek(start_included))
      try {
        def check(key: Array[Byte]) = {
          (compare(key, end_excluded) < 0) && func(key, iterator.peekNext.getValue)
        }
        while (iterator.hasNext && check(iterator.peekNext.getKey)) {
          iterator.next()
        }
      } finally {
        iterator.close();
      }
    }

    def last_key(prefix: Array[Byte], ro: ReadOptions = new ReadOptions): Option[Array[Byte]] = {
      val last = new Buffer(prefix).deepCopy().data
      if (last.length > 0) {
        val pos = last.length - 1
        last(pos) = (last(pos) + 1).toByte
      }

      if (is_pure_java_version) {
        // The pure java version of LevelDB does not support backward iteration.
        var rc: Option[Array[Byte]] = None
        cursor_range_keys(prefix, last) {
          key =>
            rc = Some(key)
            true
        }
        rc
      } else {
        val iterator = db.iterator(ro)
        try {

          might_trigger_compaction(iterator.seek(last))
          if (iterator.hasPrev) {
            iterator.prev()
          } else {
            iterator.seekToLast()
          }

          if (iterator.hasNext) {
            val key = iterator.peekNext.getKey
            if (key.startsWith(prefix)) {
              Some(key)
            } else {
              None
            }
          } else {
            None
          }
        } finally {
          iterator.close();
        }
      }
    }
  }

}
