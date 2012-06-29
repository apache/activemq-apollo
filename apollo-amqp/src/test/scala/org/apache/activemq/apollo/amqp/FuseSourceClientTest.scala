package org.apache.activemq.apollo.amqp

import org.fusesource.amqp.blocking.AMQP
import org.fusesource.amqp.types._
import org.fusesource.amqp._

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

class FuseSourceClientTest extends AmqpTestSupport {

  test("broker") {

//    val port = 5672
//    val queue = "testqueue"

    val queue = "/queue/fstestqueue"

    val nMsgs = 1
//    val qos = QoS.AT_MOST_ONCE
    
    try {
      val connect_options = new AMQPClientOptions
      connect_options.setHost("127.0.0.1", port)
      connect_options.setContainerId("client")
      connect_options.setIdleTimeout(-1)
      connect_options.setMaxFrameSize(1024*4)
//      connect_options.setListener(new AMQPConnection.Listener())
      val connection = AMQP.open(connect_options);
      {
        var data = "x" * 10 // 1024*20

        var session = connection.createSession(10, 10)
        val sender_options = new AMQPSenderOptions
        sender_options.setQoS(AMQPQoS.AT_MOST_ONCE)
        sender_options.setTarget(queue);
        var p = AMQP.createSender(sender_options)
        p.attach(session);

        for (i <- 0 until nMsgs) {
          var s = "Message #" + (i + 1)
          println("Sending " + s)
          p.send(MessageSupport.message(s+", data: "+data))
        }

        p.close()
        session.close()
      }
      {
        var session = connection.createSession(10, 10)

        val receiver_options = new AMQPReceiverOptions
        receiver_options.setQoS(AMQPQoS.AT_MOST_ONCE)
        receiver_options.setSource(queue);
        var c = AMQP.createReceiver(receiver_options)
        c.attach(session);

        // Receive messages non-transacted
        for (i <- 0 until nMsgs) {
          val msg = c.receive();
          if (msg == null)

          msg.getMessage().getData match {
            case value:AMQPString =>
              println("Received: " + value.getValue());
          }
//          if (!msg.isSettled())
//            msg.accept();
        }
        c.close()
        session.close()
      }
      connection.close()
    } catch {
      case e: Exception => {
        e.printStackTrace
      }
    }

  }

}