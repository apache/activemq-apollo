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
package org.apache.activemq.apollo.openwire

import javax.jms.{TextMessage, Session}

class QueueTest extends OpenwireTestSupport {

  test("Queue order preserved") {

    connect()

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue("example"))
    def put(id:Int) {
      producer.send(session.createTextMessage("message:"+id))
    }

    List(1,2,3).foreach(put _)

    val consumer = session.createConsumer(queue("example"))

    def get(id:Int) {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(queue("example"))
      m.getText should equal ("message:"+id)
    }

    List(1,2,3).foreach(get _)
  }

  test("Test that messages are consumed ") {

    connect()
    var session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val queue = session.createQueue("test");
    val producer = session.createProducer(queue);
    producer.send(session.createTextMessage("Hello"));

    // Consume the message...
    var consumer = session.createConsumer(queue)
    var msg = consumer.receive(1000)
    assert(msg != null)

    Thread.sleep(1000)

    session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    // Attempt to Consume the message...check if message was acknowledge
    consumer = session.createConsumer(queue)
    msg = consumer.receive(1000)
    assert(msg == null)

    session.close()
  }

  test("Queue and a selector") {
    connect()
    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue("example"))

    def put(id:Int, color:String) {
      val message = session.createTextMessage("message:" + id)
      message.setStringProperty("color", color)
      producer.send(message)
    }

    List((1, "red"), (2, "blue"), (3, "red")).foreach {
      case (id, color) => put(id, color)
    }

    val consumer = session.createConsumer(queue("example"), "color='red'")

    def get(id:Int) {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(queue("example"))
      m.getText should equal ("message:"+id)
    }

    get(1)
    get(3)
  }
}