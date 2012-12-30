package org.apache.activemq.apollo.broker.store

import java.io.{OutputStream, InputStream}
import org.fusesource.hawtbuf.Buffer
import org.apache.activemq.apollo.broker.DestinationAddress
import collection.mutable.ListBuffer
import language.implicitConversions;

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
/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object PBSupport {

  implicit def to_pb(v: MessageRecord):MessagePB.Bean = {
    val pb = new MessagePB.Bean
    pb.setMessageKey(v.key)
    pb.setCodec(v.codec)
    pb.setValue(v.buffer)
    pb
  }

  implicit def from_pb(pb: MessagePB.Getter):MessageRecord = {
    val rc = new MessageRecord
    rc.key = pb.getMessageKey
    rc.codec = pb.getCodec
    rc.buffer = pb.getValue
    rc
  }

  def encode_message_record(out: OutputStream, v: MessageRecord) = to_pb(v).freeze.writeFramed(out)
  def decode_message_record(in: InputStream):MessageRecord = MessagePB.FACTORY.parseFramed(in)

  implicit def encode_message_record(v: MessageRecord):Array[Byte] = to_pb(v).freeze.toFramedByteArray
  implicit def decode_message_record(data: Array[Byte]):MessageRecord = MessagePB.FACTORY.parseFramed(data)

  implicit def encode_message_record_buffer(v: MessageRecord) = to_pb(v).freeze.toFramedBuffer
  implicit def decode_message_record_buffer(data: Buffer):MessageRecord = MessagePB.FACTORY.parseFramed(data)


  implicit def to_pb(v: QueueRecord):QueuePB.Bean = {
    val pb = new QueuePB.Bean
    pb.setKey(v.key)
    pb.setBindingData(v.binding_data)
    pb.setBindingKind(v.binding_kind)
    pb
  }

  implicit def from_pb(pb: QueuePB.Getter):QueueRecord = {
    QueueRecord(pb.getKey, pb.getBindingKind, pb.getBindingData)
  }

  def encode_queue_record(out: OutputStream, v: QueueRecord) = to_pb(v).freeze.writeFramed(out)
  def decode_queue_record(in: InputStream):QueueRecord = QueuePB.FACTORY.parseFramed(in)

  implicit def encode_queue_record(v: QueueRecord) = to_pb(v).freeze.toFramedByteArray
  implicit def decode_queue_record(data: Array[Byte]):QueueRecord = QueuePB.FACTORY.parseFramed(data)

  implicit def encode_queue_record_buffer(v: QueueRecord) = to_pb(v).freeze.toFramedBuffer
  implicit def decode_queue_record_buffer(data: Buffer):QueueRecord = QueuePB.FACTORY.parseFramed(data)

  implicit def to_pb(v: QueueEntryRecord):QueueEntryPB.Bean = {
    val pb = new QueueEntryPB.Bean
    pb.setQueueKey(v.queue_key)
    pb.setQueueSeq(v.entry_seq)
    pb.setMessageKey(v.message_key)
    pb.setAttachment(v.attachment)
    pb.setSize(v.size)
    if(v.expiration!=0)
      pb.setExpiration(v.expiration)
    if(v.redeliveries!=0)
      pb.setRedeliveries(v.redeliveries)
    if ( v.sender!=null ) {
      v.sender.foreach(pb.addSender(_))
    }
    pb
  }
  implicit def from_pb(pb: QueueEntryPB.Getter):QueueEntryRecord = {
    import collection.JavaConversions._
    val rc = new QueueEntryRecord
    rc.queue_key = pb.getQueueKey
    rc.entry_seq = pb.getQueueSeq
    rc.message_key = pb.getMessageKey
    rc.attachment = pb.getAttachment
    rc.size = pb.getSize
    rc.expiration = pb.getExpiration
    rc.redeliveries = pb.getRedeliveries.toShort
    var senderList = pb.getSenderList
    if( senderList!=null ) {
      rc.sender = senderList.toList
    } else {
      rc.sender = List()
    }
    rc
  }

  def encode_queue_entry_record(out: OutputStream, v: QueueEntryRecord) = to_pb(v).freeze.writeFramed(out)
  def decode_queue_entry_record(in: InputStream):QueueEntryRecord = QueueEntryPB.FACTORY.parseFramed(in)

  implicit def encode_queue_entry_record(v: QueueEntryRecord) = to_pb(v).freeze.toFramedByteArray
  implicit def decode_queue_entry_record(data: Array[Byte]):QueueEntryRecord = QueueEntryPB.FACTORY.parseFramed(data)

  implicit def encode_queue_entry_record_buffer(v: QueueEntryRecord) = to_pb(v).freeze.toFramedBuffer
  implicit def decode_queue_entry_record_buffer(data: Buffer):QueueEntryRecord = QueueEntryPB.FACTORY.parseFramed(data)

}