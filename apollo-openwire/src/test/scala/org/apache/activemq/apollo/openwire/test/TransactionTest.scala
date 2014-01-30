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

import javax.jms._


/**
 *
 */
class TransactionTest extends OpenwireTestSupport {

  test("Simple JMS Consumer Transaction Test") {
    connect()
    val dest = queue(next_id("example"))

    val producer_session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = producer_session.createProducer(dest)

    val messages = List(producer_session.createTextMessage("one"), producer_session.createTextMessage("two"), producer_session.createTextMessage("three"))

    messages.foreach(producer.send(_))

    val consumer_session = default_connection.createSession(true, Session.SESSION_TRANSACTED)
    val consumer = consumer_session.createConsumer(dest)

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

  test("Simple JMS Producer Transaction Test"){
    connect()
    val dest = queue(next_id("example"))

    val producer_session = default_connection.createSession(true, Session.SESSION_TRANSACTED)
    val producer = producer_session.createProducer(dest)

    val messages = List(producer_session.createTextMessage("one"), producer_session.createTextMessage("two"), producer_session.createTextMessage("three"))

    producer.send(messages(0))

    val consumer_session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val consumer = consumer_session.createConsumer(dest)

    // should not have anything here
    var m = consumer.receive(1000).asInstanceOf[TextMessage]
    m should be (null)

    // commit so consumer can see it
    producer_session.commit()

    m = consumer.receive(1000).asInstanceOf[TextMessage]
    m should not be (null)
    m.getText should equal(messages(0).getText)

    producer.send(messages(1))
    producer_session.rollback()
    producer.send(messages(2))
    producer_session.commit()

    val m3 = consumer.receive(1000).asInstanceOf[TextMessage]
    m3 should not be (null)
    m3.getText should equal(messages(2).getText)

  }
}

class OpenwireLevelDBTransactionTest extends TransactionTest {
  override def broker_config_uri = "xml:classpath:apollo-openwire-leveldb.xml"

  test("Large Transaction Test"){
//    for( i <- 1 to 1000 ) {
    connect()
    val dest = queue(next_id("example"))
    val message_count = 100
    val producer_session = default_connection.createSession(true, Session.SESSION_TRANSACTED)
    val producer = producer_session.createProducer(dest)
    producer.setDeliveryMode(DeliveryMode.PERSISTENT)

    for( i <- 1 to message_count) {
      val x = producer_session.createTextMessage("x" * (1024*64))
      x.setIntProperty("i", i)
      producer.send(x)
    }

    // commit so consumer can see it
    producer_session.commit()

    val consumer_session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val consumer = consumer_session.createConsumer(dest)

    for( i <- 1 to message_count) {
      val m = consumer.receive(1000).asInstanceOf[TextMessage]
      m should not be (null)
      m.getIntProperty("i") should be (i)
    }
//    disconnect() }
  }

  ignore("APLO-342: Test memory usage"){
    connect()
    val dest = queue(next_id("example"))
    val message_count = 1000000
    val producer_session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = producer_session.createProducer(dest)
    producer.setDeliveryMode(DeliveryMode.PERSISTENT)

    val consumer_session = default_connection.createSession(true, Session.SESSION_TRANSACTED)
    val consumer = consumer_session.createConsumer(dest)

    for( i <- 1 to message_count) {
      if( (i % (message_count/100)) == 0) {
        println("On message: %d, jvm heap: %.2f".format(i, getJVMHeapUsage/(1024*1024.0)))
      }
      val x = producer_session.createTextMessage("x" * (1024*64))
      x.setIntProperty("i", i)
      producer.send(x)

      val m = consumer.receive(1000).asInstanceOf[TextMessage]
      m should not be (null)
      m.getIntProperty("i") should be (i)
      consumer_session.commit()
    }
  }


}

