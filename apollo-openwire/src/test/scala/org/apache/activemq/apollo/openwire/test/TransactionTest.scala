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
package org.apache.activemq.apollo.openwire.test

import javax.jms.{TextMessage, Message, MessageListener, Session}


/**
 *
 */
class TransactionTest extends OpenwireTestSupport {

  test("Simple JMS Transaction Test") {
    connect()
    val producer_session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = producer_session.createProducer(queue("example"))

    val messages = List(producer_session.createTextMessage("one"), producer_session.createTextMessage("two"), producer_session.createTextMessage("three"))

    messages.foreach(producer.send(_))

    val consumer_session = default_connection.createSession(true, Session.SESSION_TRANSACTED)
    val consumer = consumer_session.createConsumer(queue("example"))

    val m = consumer.receive(1000).asInstanceOf[TextMessage]
    m should not be (null)
    m.getText should equal(messages(0).getText)
    consumer_session.commit

    var m2 = consumer.receive(1000).asInstanceOf[TextMessage]
    m2 should not be (null)
    m2.getText should equal(messages(1).getText)
    consumer_session.rollback

    m2 = consumer.receive(2000).asInstanceOf[TextMessage]
    m2 should not be (null)
    m2.getText should equal(messages(1).getText)
    consumer_session.commit

    val m3 = consumer.receive(1000).asInstanceOf[TextMessage]
    m3 should not be (null)
    m3.getText should equal(messages(2).getText)
    consumer_session.commit
  }

}