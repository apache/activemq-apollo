/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
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
package org.apache.activemq.apollo.amqp


import _root_.org.fusesource.hawtbuf._
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.broker.store.MessageRecord
import org.apache.activemq.apollo.broker.Message

object AmqpCodec extends Log {

  val PROTOCOL = "amqp"
  val PROTOCOL_ID = Buffer.ascii(PROTOCOL)
  val PROTOCOL_MAGIC = new Buffer(Array[Byte]('A', 'M', 'Q', 'P'))

  val EMPTY_BUFFER = new Buffer(0)
  var max_command_length = 20


  def encode(message: Message):MessageRecord = {
    message match {
      case message:AMQPMessage =>
        val rc = new MessageRecord
        rc.protocol = PROTOCOL_ID
        rc.buffer = message.payload
        rc
      case _ => throw new RuntimeException("Invalid message type");
    }
  }

  def decode(message: MessageRecord) = {
    AMQPMessage(message.buffer)
  }

}