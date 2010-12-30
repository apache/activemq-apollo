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
package org.apache.activemq.apollo.broker.store.cassandra

import com.shorrockin.cascal.session._
import com.shorrockin.cascal.utils.Conversions._
import org.fusesource.hawtbuf.Buffer
import org.apache.activemq.apollo.broker.store._
import collection.mutable.ListBuffer
import org.apache.activemq.apollo.broker.store.PBSupport._

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
        session.insert(schema.queue_name \ (record.key, record))
    }
  }

  def removeQueue(queue_key: Long):Boolean = {
    withSession {
      session =>
        session.remove(schema.entries \ queue_key)
        session.remove(schema.queue_name \ queue_key)
    }
    true
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

  def getQueue(id: Long): Option[QueueRecord] = {
    withSession {
      session =>
        session.get(schema.queue_name \ id) match {
          case Some(x) =>
            val record:QueueRecord = x.value
            Some(record)

          case None =>
            None
        }
    }
  }


  def store(txs:Seq[DelayingStoreSupport#DelayableUOW]) {
    withSession {
      session =>
        var operations = List[Operation]()
        txs.foreach {
          tx =>
            tx.actions.foreach {
              case (msg, action) =>
                var rc =
                if (action.messageRecord != null) {
                  operations ::= Insert( schema.message_data \ (msg, action.messageRecord ) )
                }
                action.enqueues.foreach {
                  queueEntry =>
                    val qid = queueEntry.queue_key
                    val seq = queueEntry.entry_seq
                    operations ::= Insert( schema.entries \ qid \ (seq, queueEntry) )
                }
                action.dequeues.foreach {
                  queueEntry =>
                    val qid = queueEntry.queue_key
                    val seq = queueEntry.entry_seq
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
            Some(rc)
          case None =>
            None
        }
    }
  }

  def listQueueEntryGroups(queue_key: Long, limit: Int): Seq[QueueEntryRange] = {
    withSession {
      session =>
        var rc = ListBuffer[QueueEntryRange]()
        var group:QueueEntryRange = null

        // TODO: this is going to bring back lots of entries.. not good.
        session.list(schema.entries \ queue_key).foreach { x=>

          val record:QueueEntryRecord = x.value

          if( group == null ) {
            group = new QueueEntryRange
            group.first_entry_seq = record.entry_seq
          }
          group.last_entry_seq = record.entry_seq
          group.count += 1
          group.size += record.size
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
  }

  def getQueueEntries(queue_key: Long, firstSeq:Long, lastSeq:Long): Seq[QueueEntryRecord] = {
    withSession {
      session =>
        session.list(schema.entries \ queue_key, RangePredicate(firstSeq, lastSeq)).map { x=>
          val rc:QueueEntryRecord = x.value
          rc
        }
    }
  }
}
