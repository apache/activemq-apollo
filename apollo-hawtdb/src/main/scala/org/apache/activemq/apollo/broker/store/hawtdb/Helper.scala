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
package org.apache.activemq.apollo.broker.store.hawtdb

import org.fusesource.hawtbuf._
import codec._
import java.io.DataOutput
import org.fusesource.hawtdb.api.{SortedIndex, BTreeIndexFactory}

object Helper {

  def encode_long(a1:Long) = {
    val out = new DataByteArrayOutputStream(
      AbstractVarIntSupport.computeVarLongSize(a1)
    )
    out.writeVarLong(a1)
    out.toBuffer
  }

  def decode_long(bytes:Buffer):Long = {
    val in = new DataByteArrayInputStream(bytes)
    in.readVarLong()
  }

  def encode(a1:Byte, a2:Long) = {
    val out = new DataByteArrayOutputStream(9)
    out.writeByte(a1.toInt)
    out.writeLong(a2)
    out.toBuffer
  }

  def encode(a1:Byte, a2:Buffer) = {
    val out = new DataByteArrayOutputStream(1+a2.length)
    out.writeByte(a1.toInt)
    a2.writeTo(out.asInstanceOf[DataOutput])
    out.toBuffer
  }

  def decode_long_key(bytes:Buffer):(Byte, Long) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readByte(), in.readLong())
  }

  def encode(a1:Byte, a2:Long, a3:Long) = {
    val out = new DataByteArrayOutputStream(17)
    out.writeByte(a1)
    out.writeLong(a2)
    out.writeLong(a3)
    out.toBuffer
  }

  def decode_long_long_key(bytes:Buffer):(Byte,Long,Long) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readByte(), in.readLong(), in.readLong())
  }

  def encode(a1:Byte, a2:Int) = {
    val out = new DataByteArrayOutputStream(5)
    out.writeByte(a1)
    out.writeInt(a2)
    out.toBuffer
  }

  def decode_int_key(bytes:Buffer):(Byte,Int) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readByte(), in.readInt())
  }

  val INDEX_FACTORY = new BTreeIndexFactory[Buffer, Buffer]();
  INDEX_FACTORY.setKeyCodec(BufferCodec.INSTANCE);
  INDEX_FACTORY.setValueCodec(BufferCodec.INSTANCE);
  INDEX_FACTORY.setDeferredEncoding(true);

  final class RichBTreeIndex(val db: SortedIndex[Buffer,Buffer]) {

    def get(key:Buffer):Option[Buffer] = Option(db.get(key))
    def delete(key:Buffer) = db.remove(key)
    def put(key:Buffer, value:Buffer) = Option(db.put(key, value))

    def cursor_keys(func: Buffer => Boolean): Unit = {
      val iterator = db.iterator()
      while( iterator.hasNext && func(iterator.next().getKey) ) {
      }
    }

    def cursor_range_keys(start_included:Buffer, end_excluded:Buffer)(func:Buffer => Boolean): Unit = {
      import org.fusesource.hawtdb.api.Predicates._
      val iterator = db.iterator(and(gte(start_included), lt(end_excluded)))
      while( iterator.hasNext && func(iterator.next().getKey) ) {
      }
    }

    def cursor_range(start_included:Buffer, end_excluded:Buffer)(func: (Buffer,Buffer) => Boolean): Unit = {
      def call(entry:java.util.Map.Entry[Buffer,Buffer]) = func(entry.getKey, entry.getValue)
      import org.fusesource.hawtdb.api.Predicates._
      val iterator = db.iterator(and(gte(start_included), lt(end_excluded)))
      while( iterator.hasNext && call(iterator.next()) ) {
      }
    }

    def last_key(prefix:Buffer): Option[Buffer] = {
      var rc:Option[Buffer] = None
      cursor_keys_prefixed(prefix) { key =>
        rc = Some(key)
        true
      }
      rc
    }

    def cursor_prefixed(prefix:Buffer)(func: (Buffer,Buffer) => Boolean): Unit = {
      val iterator = db.iterator(prefix)
      def check(entry:java.util.Map.Entry[Buffer,Buffer]) = {
        entry.getKey.startsWith(prefix) && func(entry.getKey, entry.getValue)
      }
      while( iterator.hasNext && check(iterator.next()) ) {
      }
    }

    def cursor_keys_prefixed(prefix:Buffer)(func: Buffer => Boolean): Unit = {
      val iterator = db.iterator(prefix)
      def check(entry:java.util.Map.Entry[Buffer,Buffer]) = {
        entry.getKey.startsWith(prefix) && func(entry.getKey)
      }
      while( iterator.hasNext && check(iterator.next()) ) {
      }
    }
  }

}
