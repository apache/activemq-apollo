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

import javax.jms.{Session, JMSException, MessageProducer}
import org.apache.activemq.apollo.broker.BrokerParallelTestExecution

class SecurityTest extends OpenwireTestSupport with BrokerParallelTestExecution {

  override def is_parallel_test_class = false

  override val broker_config_uri: String = "xml:classpath:apollo-openwire-secure.xml"

  override def beforeAll = {
    try {
      val login_file = new java.io.File(getClass.getClassLoader.getResource("login.config").getFile())
      System.setProperty("java.security.auth.login.config", login_file.getCanonicalPath)
    } catch {
      case x: Throwable => x.printStackTrace
    }
    super.beforeAll
  }

  test("Connect with valid id password but can't connect") {
    intercept[JMSException] {
      connect(user="can_not_connect", password="can_not_connect")
    }
  }

  test("Connect with no id password") {
    intercept[JMSException] {
      connect()
    }
  }

  test("Connect with invalid id password") {
    intercept[JMSException] {
      connect(user="foo", password="bar")
    }
  }

  test("Connect with valid id password that can connect") {
    try {
      connect("?jms.alwaysSyncSend=true", user="can_only_connect", password="can_only_connect")
    } catch {
      case e:Throwable =>
        e.printStackTrace()
        fail("Should not have thrown an exception ")
    }

  }

  test("Send not authorized") {
    val connection = connect(user="can_only_connect", password="can_only_connect")

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue("secure"))

    intercept[JMSException] {
      producer.send(session.createTextMessage("Test Message"))
    }
  }

  test("Send authorized but not create") {

    val connection = connect(user="can_send_queue", password="can_send_queue")

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue("secure"))

    intercept[JMSException] {
      producer.send(session.createTextMessage("Test Message"))
    }
  }

  test("Consume authorized but not create") {

    val connection = connect(user="can_consume_queue", password="can_consume_queue")

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    intercept[JMSException] {
      val consumer = session.createConsumer(queue("secure"))
      consumer.receive();
    }
  }

  test("Send and create authorized") {
    create_and_send(next_id("secure"))
  }


  def create_and_send(dest:String) {
    val connection = connect(user="can_send_create_queue", password="can_send_create_queue")

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue(dest))

    try {
      producer.send(session.createTextMessage("Test Message"))
    } catch {
      case e:Throwable => fail("Should not have thrown an exception")
    }
  }

  test("Can send and once created") {
    val dest = next_id("secure")
    val connection = connect(user="can_send_queue", password="can_send_queue")

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue(dest))

    try {
      producer.send(session.createTextMessage("Test Message"))
      fail("Should have thrown an exception since dest is not created.")
    } catch {
      case e:Throwable =>
    }

    // Now actually create it...
    create_and_send(dest)

    try {
      producer.send(session.createTextMessage("Test Message"))
    } catch {
      case e:Throwable =>
        e.printStackTrace()
        fail("Should not have thrown an exception since it was created")
    }

  }

  test("Consume not authorized") {

    val connection = connect(user="can_only_connect", password="can_only_connect")

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    intercept[JMSException] {
      val consumer = session.createConsumer(queue("secure"))
      consumer.receive();
    }
  }

  test("APLO-213: The JMSXUserID is set to be the authenticated user") {

    val dest = queue(next_id("JMSXUserID"));

    val producer_connection = connect(user="can_send_create_queue", password="can_send_create_queue")

    val producer_session = producer_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = producer_session.createProducer(dest)
    try {
      producer.send(producer_session.createTextMessage("Test Message"))
    } catch {
      case e:Throwable => fail("Should not have thrown an exception")
    }

    val consumer_connection = connect(user="can_consume_queue", password="can_consume_queue")
    val consumer_session = consumer_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val consumer = consumer_session.createConsumer(dest)

    val msg = consumer.receive(receive_timeout)
    msg.getStringProperty("JMSXUserID") should equal("can_send_create_queue")

  }


}
