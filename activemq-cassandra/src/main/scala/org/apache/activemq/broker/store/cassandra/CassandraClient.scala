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
package org.apache.activemq.broker.store.cassandra

import org.apache.activemq.apollo.broker.{Logging, BaseService}
import com.shorrockin.cascal.session._
import com.shorrockin.cascal.utils.Conversions._
import java.util.{HashMap}
import org.fusesource.hawtbuf.AsciiBuffer._
import org.fusesource.hawtbuf.{AsciiBuffer, DataByteArrayInputStream, DataByteArrayOutputStream, Buffer}
import org.apache.activemq.apollo.store.{QueueEntryRecord, QueueStatus, MessageRecord, QueueRecord}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class CassandraClient() {
  var schema: Schema = new Schema("ActiveMQ")
  var hosts = Host("127.0.0.1", 9160, 3000) :: Nil

  implicit def toByteArray(buffer: Buffer) = buffer.toByteArray

  protected var pool: SessionPool = null

  def start() = {
    val params = new PoolParams(20, ExhaustionPolicy.Fail, 500L, 6, 2)
    pool = new SessionPool(hosts, params, Consistency.One)
  }

  def stop() = {
    pool.close
  }

  protected def withSession[E](block: Session => E): E = {
    val session = pool.checkout
    try {
      block(session)
    } finally {
      pool.checkin(session)
    }
  }

  implicit def decodeMessageRecord(v: Array[Byte]): MessageRecord = {
    import PBMessageRecord._
    val pb = PBMessageRecord.FACTORY.parseUnframed(v)
    val rc = new MessageRecord
    rc.protocol = pb.getProtocol
    rc.size = pb.getSize
    rc.value = pb.getValue
    rc.stream = pb.getStream
    rc.expiration = pb.getExpiration
    rc
  }

  implicit def encodeMessageRecord(v: MessageRecord): Array[Byte] = {
    val pb = new PBMessageRecord.Bean
    pb.setProtocol(v.protocol)
    pb.setSize(v.size)
    pb.setValue(v.value)
    pb.setStream(v.stream)
    pb.setExpiration(v.expiration)
    pb.freeze.toUnframedByteArray
  }
  
  implicit def decodeQueueEntryRecord(v: Array[Byte]): QueueEntryRecord = {
    import PBQueueEntryRecord._
    val pb = PBQueueEntryRecord.FACTORY.parseUnframed(v)
    val rc = new QueueEntryRecord
    rc.messageKey = pb.getMessageKey
    rc.attachment = pb.getAttachment
    rc.size = pb.getSize
    rc.redeliveries = pb.getRedeliveries.toShort
    rc
  }

  implicit def encodeQueueEntryRecord(v: QueueEntryRecord): Array[Byte] = {
    val pb = new PBQueueEntryRecord.Bean
    pb.setMessageKey(v.messageKey)
    pb.setAttachment(v.attachment)
    pb.setSize(v.size)
    pb.setRedeliveries(v.redeliveries)
    pb.freeze.toUnframedByteArray
  }

  def purge() = {
    withSession {
      session =>
        session.list(schema.queue_name).map { x =>
          val qid: Long = x.name
          session.remove(schema.entries \ qid)
        }
        session.remove(schema.queue_name)
        session.remove(schema.message_data)
    }
  }

  def addQueue(record: QueueRecord) = {
    withSession {
      session =>
        session.insert(schema.queue_name \ (record.key, record.name))
    }
  }

  def listQueues: Seq[Long] = {
    withSession {
      session =>
        session.list(schema.queue_name).map {
          x =>
            val id: Long = x.name
            id
        }
    }
  }

  def getQueueStatus(id: Long): Option[QueueStatus] = {
    withSession {
      session =>
        session.get(schema.queue_name \ id) match {
          case Some(x) =>

            val rc = new QueueStatus
            rc.record = new QueueRecord
            rc.record.key = id
            rc.record.name = new AsciiBuffer(x.value)

//            rc.count = session.count( schema.entries \ id )
            
            // TODO
            //          rc.count =
            //          rc.first =
            //          rc.last =

            Some(rc)
          case None =>
            None
        }
    }
  }


  def store(txs:Seq[CassandraStore#CassandraBatch]) {
    withSession {
      session =>
        var operations = List[Operation]()
        txs.foreach {
          tx =>
            tx.actions.foreach {
              case (msg, action) =>
                var rc =
                if (action.store != null) {
                  operations ::= Insert( schema.message_data \ (msg, action.store) )
                }
                action.enqueues.foreach {
                  queueEntry =>
                    val qid = queueEntry.queueKey
                    val seq = queueEntry.queueSeq
                    operations ::= Insert( schema.entries \ qid \ (seq, queueEntry) )
                }
                action.dequeues.foreach {
                  queueEntry =>
                    val qid = queueEntry.queueKey
                    val seq = queueEntry.queueSeq
                    operations ::= Delete( schema.entries \ qid, ColumnPredicate(seq :: Nil) )
                }
            }
        }
        session.batch(operations)
    }
  }

  def loadMessage(id: Long): Option[MessageRecord] = {
    withSession {
      session =>
        session.get(schema.message_data \ id) match {
          case Some(x) =>
            val rc: MessageRecord = x.value
            rc.key = id
            Some(rc)
          case None =>
            None
        }
    }
  }

  def getQueueEntries(qid: Long): Seq[QueueEntryRecord] = {
    withSession {
      session =>
        session.list(schema.entries \ qid).map { x=>
          val rc:QueueEntryRecord = x.value
          rc.queueKey = qid
          rc.queueSeq = x.name
          rc
        }
    }
  }
}