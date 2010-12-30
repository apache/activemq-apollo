package org.apache.activemq.apollo.broker.store

import java.io.{OutputStream, InputStream}

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

  implicit def to_pb(v: MessageRecord):MessagePB.Buffer = {
    val pb = new MessagePB.Bean
    pb.setMessageKey(v.key)
    pb.setProtocol(v.protocol)
    pb.setSize(v.size)
    pb.setValue(v.buffer)
    pb.setExpiration(v.expiration)
    pb.freeze
  }

  implicit def from_pb(pb: MessagePB.Getter):MessageRecord = {
    val rc = new MessageRecord
    rc.key = pb.getMessageKey
    rc.protocol = pb.getProtocol
    rc.size = pb.getSize
    rc.buffer = pb.getValue
    rc.expiration = pb.getExpiration
    rc
  }

  def encode_message_record(out: OutputStream, v: MessageRecord) = to_pb(v).writeUnframed(out)
  def decode_message_record(in: InputStream):MessageRecord = MessagePB.FACTORY.parseUnframed(in)

  implicit def encode_message_record(v: MessageRecord) = to_pb(v).toUnframedByteArray
  implicit def decode_message_record(data: Array[Byte]):MessageRecord = MessagePB.FACTORY.parseUnframed(data)



  implicit def to_pb(v: QueueRecord):QueuePB.Buffer = {
    val pb = new QueuePB.Bean
    pb.setKey(v.key)
    pb.setBindingData(v.binding_data)
    pb.setBindingKind(v.binding_kind)
    pb.freeze
  }

  implicit def from_pb(pb: QueuePB.Getter):QueueRecord = {
    val rc = new QueueRecord
    rc.key = pb.getKey
    rc.binding_data = pb.getBindingData
    rc.binding_kind = pb.getBindingKind
    rc
  }

  def encode_queue_record(out: OutputStream, v: QueueRecord) = to_pb(v).writeUnframed(out)
  def decode_queue_record(in: InputStream):QueueRecord = QueuePB.FACTORY.parseUnframed(in)

  implicit def encode_queue_record(v: QueueRecord) = to_pb(v).toUnframedByteArray
  implicit def decode_queue_record(data: Array[Byte]):QueueRecord = QueuePB.FACTORY.parseUnframed(data)


  implicit def to_pb(v: QueueEntryRecord):QueueEntryPB.Buffer = {
    val pb = new QueueEntryPB.Bean
    pb.setQueueKey(v.queue_key)
    pb.setQueueSeq(v.entry_seq)
    pb.setMessageKey(v.message_key)
    pb.setAttachment(v.attachment)
    pb.setSize(v.size)
    pb.setRedeliveries(v.redeliveries)
    pb.freeze
  }

  implicit def from_pb(pb: QueueEntryPB.Getter):QueueEntryRecord = {
    val rc = new QueueEntryRecord
    rc.queue_key = pb.getQueueKey
    rc.entry_seq = pb.getQueueSeq
    rc.message_key = pb.getMessageKey
    rc.attachment = pb.getAttachment
    rc.size = pb.getSize
    rc.redeliveries = pb.getRedeliveries.toShort
    rc
  }

  def encode_queue_entry_record(out: OutputStream, v: QueueEntryRecord) = to_pb(v).writeUnframed(out)
  def decode_queue_entry_record(in: InputStream):QueueEntryRecord = QueueEntryPB.FACTORY.parseUnframed(in)

  implicit def encode_queue_entry_record(v: QueueEntryRecord) = to_pb(v).toUnframedByteArray
  implicit def decode_queue_entry_record(data: Array[Byte]):QueueEntryRecord = QueueEntryPB.FACTORY.parseUnframed(data)

}