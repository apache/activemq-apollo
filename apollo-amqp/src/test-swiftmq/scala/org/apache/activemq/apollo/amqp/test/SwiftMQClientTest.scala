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

package org.apache.activemq.apollo.amqp.test

import com.swiftmq.amqp.AMQPContext
import com.swiftmq.amqp.v100.client.Connection
import com.swiftmq.amqp.v100.client.QoS
import com.swiftmq.amqp.v100.generated.messaging.message_format.AmqpValue
import com.swiftmq.amqp.v100.types.AMQPString
import com.swiftmq.amqp.v100.client.ExceptionListener

class SwiftMQClientTest extends AmqpTestSupport {

  test("broker") {

//    val port = 5672
//    val queue = "testqueue"

    val queue = "/queue/testqueue"

    val nMsgs = 1
    val qos = QoS.AT_MOST_ONCE
    val ctx = new AMQPContext(AMQPContext.CLIENT);

    try {

      val connection = new Connection(ctx, "127.0.0.1", port, false)
      connection.setContainerId("client")
      connection.setIdleTimeout(-1)
      connection.setMaxFrameSize(1024*4)
      connection.setExceptionListener(new ExceptionListener(){
        def onException(e: Exception) {
          e.printStackTrace();
        }
      })
      connection.connect;
      {
        var data = "x" * 10 // 1024*20

        var session = connection.createSession(10, 10)
        var p = {
          session.createProducer(queue, qos)
        }
        for (i <- 0 until nMsgs) {
          var msg = new com.swiftmq.amqp.v100.messaging.AMQPMessage
          var s = "Message #" + (i + 1)
          println("Sending " + s)
          msg.setAmqpValue(new AmqpValue(new AMQPString(s+", data: "+data)))
          p.send(msg)
        }
        p.close()
        session.close()
      }
      {
        var session = connection.createSession(10, 10)
        val c = session.createConsumer(queue, 100, qos, true, null);

        // Receive messages non-transacted
        for (i <- 0 until nMsgs)
        {
          val msg = c.receive();
          if (msg == null)

          msg.getAmqpValue().getValue match {
            case value:AMQPString =>
              println("Received: " + value.getValue());
          }
          if (!msg.isSettled())
            msg.accept();
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
