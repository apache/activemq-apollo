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

abstract class SecurityTest extends OpenwireTestSupport {

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
}

class ConnectionFailureWithValidCredentials extends SecurityTest {

  test("Connect with valid id password but can't connect") {

    val factory = create_connection_factory()
    val connection = factory.createConnection("can_not_connect", "can_not_connect")

    intercept[JMSException] {
      connection.start()
    }
  }
}

class CoonectionFailsWhenNoCredentialsGiven extends SecurityTest {

  test("Connect with no id password") {

    val factory = create_connection_factory()
    val connection = factory.createConnection()

    intercept[JMSException] {
      connection.start()
    }
  }
}

class ConnectionFailsWhenCredentialsAreInvlaid extends SecurityTest {

  test("Connect with invalid id password") {
    val factory = create_connection_factory()
    val connection = factory.createConnection("foo", "bar")

    intercept[JMSException] {
      connection.start()
    }
  }
}

class ConnectionSucceedsWithValidCredentials extends SecurityTest {
  test("Connect with valid id password that can connect") {

    val factory = create_connection_factory("?jms.alwaysSyncSend=true")
    val connection = factory.createConnection("can_only_connect", "can_only_connect")

    try {
      connection.start()
    } catch {
      case e => fail("Should not have thrown an exception")
    }

  }
}

class SendFailsWhenNotAuthorized extends SecurityTest {
  test("Send not authorized") {
    val factory = create_connection_factory()
    val connection = factory.createConnection("can_only_connect", "can_only_connect")

    try {
      connection.start()
    } catch {
      case e => fail("Should not have thrown an exception")
    }

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue("secure"))

    intercept[JMSException] {
      producer.send(session.createTextMessage("Test Message"))
    }
  }
}

class SendFailsWhenNotAuthorizedToCreateQueues extends SecurityTest {

  test("Send authorized but not create") {

    val factory = create_connection_factory()
    val connection = factory.createConnection("can_send_queue", "can_send_queue")

    try {
      connection.start()
    } catch {
      case e => fail("Should not have thrown an exception")
    }

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue("secure"))

    intercept[JMSException] {
      producer.send(session.createTextMessage("Test Message"))
    }
  }
}

class ConsumeFailsWhenNotAuthroizedToCreateQueue extends SecurityTest {

  test("Consume authorized but not create") {

    val factory = create_connection_factory()
    val connection = factory.createConnection("can_consume_queue", "can_consume_queue")

    try {
      connection.start()
    } catch {
      case e => fail("Should not have thrown an exception")
    }

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    intercept[JMSException] {
      val consumer = session.createConsumer(queue("secure"))
      consumer.receive();
    }
  }
}

class SendSucceedsWhenCreateQueueAthorized extends SecurityTest {
  test("Send and create authorized") {
    val factory = create_connection_factory()
    val connection = factory.createConnection("can_send_create_queue", "can_send_create_queue")

    try {
      connection.start()
    } catch {
      case e => fail("Should not have thrown an exception")
    }

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue("secure"))

    try {
      producer.send(session.createTextMessage("Test Message"))
    } catch {
      case e => fail("Should not have thrown an exception")
    }
  }

  test("Can send and once created") {

    val factory = create_connection_factory()
    val connection = factory.createConnection("can_send_queue", "can_send_queue")

    try {
      connection.start()
    } catch {
      case e => fail("Should not have thrown an exception")
    }

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue("secure"))

    try {
      producer.send(session.createTextMessage("Test Message"))
    } catch {
      case e => fail("Should not have thrown an exception")
    }
  }
}

class SubscribeFailsForConnectionOnlyAuthorization extends SecurityTest {

  test("Consume not authorized") {

    val factory = create_connection_factory()
    val connection = factory.createConnection("can_only_connect", "can_only_connect")

    try {
      connection.start()
    } catch {
      case e => fail("Should not have thrown an exception")
    }

    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    intercept[JMSException] {
      val consumer = session.createConsumer(queue("secure"))
      consumer.receive();
    }
  }
}
