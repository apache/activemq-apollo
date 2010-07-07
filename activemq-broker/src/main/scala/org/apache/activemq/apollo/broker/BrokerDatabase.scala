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
package org.apache.activemq.apollo.broker

import _root_.java.lang.{String}
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import org.fusesource.hawtbuf._
import org.apache.activemq.util.TreeMap
import java.util.concurrent.atomic.{AtomicLong}
import java.util.{HashSet}
import collection.JavaConversions

class Record
case class QueueRecord(val id:Long, val name:AsciiBuffer, val parent:AsciiBuffer, val config:String) extends Record
class MessageRecord(val id:Long, val msgId:AsciiBuffer, val encoding: AsciiBuffer, val message:Buffer)  extends Record
class QueueEntryRecord(val queue:Long, val seqId:Long, val msgId:Long) extends Record
class SubscriptionRecord(val id:Long, val pk:AsciiBuffer, val selector:AsciiBuffer, val destination:AsciiBuffer, val durable:Boolean, val tte:Long, val attachment:Buffer) extends Record
class Action
case class CreateRecord(record:Record) extends Action
case class UpdateRecord(record:Record) extends Action
case class DeleteRecord(id:Long) extends Action

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class BrokerDatabase(host:VirtualHost) {

  def start() ={
  }

  def stop() = {
  }

  private object messages {

    val dispatchQueue = createQueue("MessagesTable")
    val messages = new TreeMap[Long, MessageRecord]
    val inprogress = new HashSet[Long]()

    def add(record:MessageRecord) = {
      val id= record.id

      // the inprogress list protects the message from being
      // gced too soon.  Protection ends once StoredMessageRef
      // is disposed..
      val ref = new StoredMessageRef(id) {
        override def dispose = ^{
          inprogress.remove(id)
        } >>: dispatchQueue
      }

      using(ref) {
        inprogress.add(id)
        messages.put(record.id, record)
      } >>: dispatchQueue

      ref
    }

    def get(id:Long, cb:(MessageRecord)=>Unit) = reply(cb) {
      messages.get(id)
    } >>: dispatchQueue

  }

  private val msg_id_generator = new AtomicLong
  def createMessageRecord(msgId:AsciiBuffer, encoding:AsciiBuffer, message:Buffer) = {
    val record = new MessageRecord(msg_id_generator.incrementAndGet, msgId, encoding, message)
    messages.add(record)
  }



  case class QueueData(val record:QueueRecord) {
    var messges:List[Long] = Nil
  }

  private object queues {
    var _next_id = 0L;
    def next_id = {
      val rc = _next_id
      _next_id += 1
      rc
    }

    val dispatchQueue = createQueue("QueuesTable")
    val records = new TreeMap[Long, QueueData]
  }

  case class QueueInfo(record:QueueRecord, first:Long, last:Long, count:Int)

  def listQueues(cb: (Seq[Long])=>Unit ) = reply(cb) {
    JavaConversions.asSet(queues.records.keySet).toSeq
  } >>: queues.dispatchQueue

  def getQueueInfo(id:Long)(cb:(Option[QueueInfo])=>Unit ) = reply(cb) {
    val qd = queues.records.get(id)
    if( qd == null ) {
      None
    } else {
      Some(
        if( qd.messges.isEmpty ) {
          QueueInfo(qd.record, -1, -1, 0)
        } else {
          QueueInfo(qd.record, qd.messges.head, qd.messges.last, qd.messges.size)
        }
      )
    }
  } >>: queues.dispatchQueue


  def addQueue(record:QueueRecord)(cb:(Option[Long])=>Unit):Unit = reply(cb) {
    val id = queues.next_id
    if( queues.records.containsKey(id) ) {
      None
    } else {
      queues.records.put(id, QueueData(record))
      Some(id)
    }
  } >>: queues.dispatchQueue

}
