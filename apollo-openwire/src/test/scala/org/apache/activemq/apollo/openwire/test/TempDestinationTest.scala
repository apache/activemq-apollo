package org.apache.activemq.apollo.openwire.test

import javax.jms._

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class TempDestinationTest extends OpenwireTestSupport {

  test("Temp Queue Destinations") {
    test_temp_destination((session: Session) => session.createTemporaryQueue())
  }

  test("Temp Topic Destinations") {
    test_temp_destination((session: Session) => session.createTemporaryTopic())
  }

  def test_temp_destination(func: (Session) => Destination) = {
    connect()

    val connection2 = connect("", true)
    val session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val dest = func(session);
    val consumer = session.createConsumer(dest)

    val producer2 = session2.createProducer(dest)

    def put(id: Int) = producer2.send(session.createTextMessage("message:" + id))
    def get(id: Int) = {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(dest)
      m.getText should equal("message:" + id)
    }

    Thread.sleep(1000);
    List(1, 2, 3).foreach(put _)
    List(1, 2, 3).foreach(get _)

    // A different connection should not be able to consume from it.
    try {
      session2.createConsumer(dest)
      fail("expected jms exception")
    } catch {
      case e: JMSException => println(e)
    }

    // delete the temporary destination.
    consumer.close()
    dest match {
      case dest: TemporaryQueue => dest.delete()
      case dest: TemporaryTopic => dest.delete()
    }

    // The producer should no longer be able to send to it.
    Thread.sleep(1000);
    try {
      put(4)
      fail("expected jms exception")
    } catch {
      case e: JMSException => println(e)
    }

  }

}