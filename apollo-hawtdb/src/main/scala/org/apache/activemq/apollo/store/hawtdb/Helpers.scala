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
package org.apache.activemq.apollo.store.hawtdb

import model._
import model.Type.TypeCreatable
import org.fusesource.hawtbuf.codec._
import org.fusesource.hawtbuf.{UTF8Buffer, AsciiBuffer, Buffer}
import java.io.{IOException, DataInput, DataOutput}
import org.fusesource.hawtdb.internal.journal.{LocationCodec, Location}
import org.fusesource.hawtdb.api._
import org.fusesource.hawtbuf.proto.{MessageBuffer, PBMessage}
import org.apache.activemq.apollo.store.{MessageRecord, QueueRecord, QueueEntryRecord}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Helpers {

  implicit def toMessageRecord(pb: AddMessage.Getter): MessageRecord = {
    val rc = new MessageRecord
    rc.key = pb.getMessageKey
    rc.protocol = pb.getProtocol
    rc.size = pb.getSize
    rc.buffer = pb.getValue
    rc.expiration = pb.getExpiration
    rc
  }

  implicit def fromMessageRecord(v: MessageRecord): AddMessage.Bean = {
    val pb = new AddMessage.Bean
    pb.setMessageKey(v.key)
    pb.setProtocol(v.protocol)
    pb.setSize(v.size)
    pb.setValue(v.buffer)
    pb.setExpiration(v.expiration)
    pb
  }

  implicit def toQueueEntryRecord(pb: AddQueueEntry.Getter): QueueEntryRecord = {
    val rc = new QueueEntryRecord
    rc.queueKey = pb.getQueueKey
    rc.queueSeq = pb.getQueueSeq
    rc.messageKey = pb.getMessageKey
    rc.attachment = pb.getAttachment
    rc.size = pb.getSize
    rc.redeliveries = pb.getRedeliveries.toShort
    rc
  }

  implicit def fromQueueEntryRecord(v: QueueEntryRecord): AddQueueEntry.Bean = {
    val pb = new AddQueueEntry.Bean
    pb.setQueueKey(v.queueKey)
    pb.setQueueSeq(v.queueSeq)
    pb.setMessageKey(v.messageKey)
    pb.setAttachment(v.attachment)
    pb.setSize(v.size)
    pb.setRedeliveries(v.redeliveries)
    pb
  }

  implicit def toLocation(value: Long): Location = {
    val temp = new Buffer(8)
    val editor = temp.bigEndianEditor
    editor.writeLong(value)
    temp.reset
    new Location(editor.readInt(), editor.readInt())
  }
  
  implicit def fromLocation(value: Location):Long = {
    val temp = new Buffer(8)
    val editor = temp.bigEndianEditor
    editor.writeInt(value.getDataFileId)
    editor.writeInt(value.getOffset)
    temp.reset
    editor.readLong
  }

  implicit def toAsciiBuffer(value:String):AsciiBuffer = new AsciiBuffer(value)
  implicit def toUTF8Buffer(value:String):UTF8Buffer = new UTF8Buffer(value)

  type PB = PBMessage[_ <: PBMessage[_, _], _ <: MessageBuffer[_, _]]
  implicit def toPBMessage(value: TypeCreatable): PB = value.asInstanceOf[PB]


  val DATABASE_ROOT_RECORD_ACCESSOR = new CodecPagedAccessor[DatabaseRootRecord.Buffer](DatabaseRootRecord.FRAMED_CODEC);

  def decode(location: Location, updateType: Int, value: Buffer) = {
    val t = Type.valueOf(updateType);
    if (t == null) {
      throw new IOException("Could not load journal record. Invalid type at location: " + location);
    }
    t.parseFramed(value).asInstanceOf[TypeCreatable]
  }

  //
  // Index factories...
  //

  import java.{lang => jl}

  // maps message key -> Journal Location
  val MESSAGE_KEY_INDEX_FACTORY = new BTreeIndexFactory[jl.Long, Location]();
  MESSAGE_KEY_INDEX_FACTORY.setKeyCodec(LongCodec.INSTANCE);
  MESSAGE_KEY_INDEX_FACTORY.setValueCodec(LocationCodec.INSTANCE);
  MESSAGE_KEY_INDEX_FACTORY.setDeferredEncoding(true);

  // maps Journal Data File Id -> Ref Counter
  val DATA_FILE_REF_INDEX_FACTORY = new BTreeIndexFactory[jl.Integer, jl.Integer]();
  DATA_FILE_REF_INDEX_FACTORY.setKeyCodec(VarIntegerCodec.INSTANCE);
  DATA_FILE_REF_INDEX_FACTORY.setValueCodec(VarIntegerCodec.INSTANCE);
  DATA_FILE_REF_INDEX_FACTORY.setDeferredEncoding(true);

  // maps message key -> Ref Counter
  val MESSAGE_REFS_INDEX_FACTORY = new BTreeIndexFactory[jl.Long, jl.Integer]();
  MESSAGE_REFS_INDEX_FACTORY.setKeyCodec(LongCodec.INSTANCE);
  MESSAGE_REFS_INDEX_FACTORY.setValueCodec(VarIntegerCodec.INSTANCE);
  MESSAGE_REFS_INDEX_FACTORY.setDeferredEncoding(true);

  // maps queue key -> QueueRootRecord
  val QUEUE_INDEX_FACTORY = new BTreeIndexFactory[jl.Long, QueueRootRecord.Buffer]();
  QUEUE_INDEX_FACTORY.setKeyCodec(VarLongCodec.INSTANCE);
  QUEUE_INDEX_FACTORY.setValueCodec(QueueRootRecord.FRAMED_CODEC);
  QUEUE_INDEX_FACTORY.setDeferredEncoding(true);

  // maps queue seq -> AddQueueEntry
  val QUEUE_ENTRY_INDEX_FACTORY = new BTreeIndexFactory[jl.Long, AddQueueEntry.Buffer]();
  QUEUE_ENTRY_INDEX_FACTORY.setKeyCodec(VarLongCodec.INSTANCE);
  QUEUE_ENTRY_INDEX_FACTORY.setValueCodec(AddQueueEntry.FRAMED_CODEC);
  QUEUE_ENTRY_INDEX_FACTORY.setDeferredEncoding(true);

  // maps message key -> queue seq
  val QUEUE_TRACKING_INDEX_FACTORY = new BTreeIndexFactory[jl.Long, jl.Long]();
  QUEUE_TRACKING_INDEX_FACTORY.setKeyCodec(LongCodec.INSTANCE);
  QUEUE_TRACKING_INDEX_FACTORY.setValueCodec(VarLongCodec.INSTANCE);
  QUEUE_TRACKING_INDEX_FACTORY.setDeferredEncoding(true);

  val SUBSCRIPTIONS_INDEX_FACTORY = new BTreeIndexFactory[AsciiBuffer, AddSubscription.Buffer]();
  SUBSCRIPTIONS_INDEX_FACTORY.setKeyCodec(AsciiBufferCodec.INSTANCE);
  SUBSCRIPTIONS_INDEX_FACTORY.setValueCodec(AddSubscription.FRAMED_CODEC);
  SUBSCRIPTIONS_INDEX_FACTORY.setDeferredEncoding(true);

}