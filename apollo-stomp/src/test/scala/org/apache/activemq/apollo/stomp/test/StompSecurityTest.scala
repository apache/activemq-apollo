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
package org.apache.activemq.apollo.stomp.test

import java.lang.String

class StompSecurityTest extends StompTestSupport {

  override def broker_config_uri: String = "xml:classpath:apollo-stomp-secure.xml"

  override def is_parallel_test_class: Boolean = false

  override def beforeAll = {
    try {
      println("before: " + testName)
      val login_file = new java.io.File(getClass.getClassLoader.getResource("login.config").getFile())
      System.setProperty("java.security.auth.login.config", login_file.getCanonicalPath)
    } catch {
      case x: Throwable => x.printStackTrace
    }
    super.beforeAll
  }

  test("Connect with valid id password but can't connect") {

    val frame = connect_request("1.1", client,
      "login:can_not_connect\n" +
              "passcode:can_not_connect\n")
    frame should startWith("ERROR\n")
    frame should include("message:Not authorized to connect")

  }

  test("Connect with no id password") {
    val frame = connect_request("1.1", client)
    frame should startWith("ERROR\n")
    frame should include("message:Authentication failed.")
  }

  test("Connect with invalid id password") {
    val frame = connect_request("1.1", client,
      "login:foo\n" +
              "passcode:bar\n")
    frame should startWith("ERROR\n")
    frame should include("message:Authentication failed.")

  }

  test("Connect with valid id password that can connect") {
    connect("1.1", client,
      "login:can_only_connect\n" +
              "passcode:can_only_connect\n")

  }

  test("Connector restricted user on the right connector") {
    connect("1.1", client,
      "login:connector_restricted\n" +
              "passcode:connector_restricted\n", "tcp2")
  }

  test("Connector restricted user on the wrong connector") {
    val frame = connect_request("1.1", client,
      "login:connector_restricted\n" +
              "passcode:connector_restricted\n", "tcp")
    frame should startWith("ERROR\n")
    frame should include("message:Not authorized to connect to connector 'tcp'.")
  }

  test("Send not authorized") {
    connect("1.1", client,
      "login:can_only_connect\n" +
              "passcode:can_only_connect\n")

    client.write(
      "SEND\n" +
              "destination:/queue/secure\n" +
              "receipt:0\n" +
              "\n" +
              "Hello Wolrd\n")

    val frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include("message:Not authorized to create the queue")
  }

  test("Send authorized but not create") {
    connect("1.1", client,
      "login:can_send_queue\n" +
              "passcode:can_send_queue\n")

    client.write(
      "SEND\n" +
              "destination:/queue/secure\n" +
              "receipt:0\n" +
              "\n" +
              "Hello Wolrd\n")

    val frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include("message:Not authorized to create the queue")

  }

  test("Consume authorized but not create") {
    connect("1.1", client,
      "login:can_consume_queue\n" +
              "passcode:can_consume_queue\n")

    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/secure\n" +
              "id:0\n" +
              "receipt:0\n" +
              "\n")

    val frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include("message:Not authorized to create the queue")
  }

  test("Send and create authorized") {
    connect("1.1", client,
      "login:can_send_create_queue\n" +
              "passcode:can_send_create_queue\n")

    client.write(
      "SEND\n" +
              "destination:/queue/secure\n" +
              "receipt:0\n" +
              "\n" +
              "Hello Wolrd\n")

    wait_for_receipt("0")

  }

  test("Send and create authorized via id_regex") {
    connect("1.1", client,
      "login:guest\n" +
              "passcode:guest\n")

    client.write(
      "SEND\n" +
              "destination:/queue/testblah\n" +
              "receipt:0\n" +
              "\n" +
              "Hello Wolrd\n")

    wait_for_receipt("0")

    client.write(
      "SEND\n" +
              "destination:/queue/notmatch\n" +
              "receipt:1\n" +
              "\n" +
              "Hello Wolrd\n")

    val frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include("message:Not authorized to create the queue")
  }

  test("Can send and once created") {

    // Now try sending with the lower access id.
    connect("1.1", client,
      "login:can_send_queue\n" +
              "passcode:can_send_queue\n")

    client.write(
      "SEND\n" +
              "destination:/queue/secure\n" +
              "receipt:0\n" +
              "\n" +
              "Hello Wolrd\n")

    wait_for_receipt("0")

  }

  test("Consume not authorized") {
    connect("1.1", client,
      "login:can_only_connect\n" +
              "passcode:can_only_connect\n")

    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/secure\n" +
              "id:0\n" +
              "receipt:0\n" +
              "\n")

    val frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include("message:Not authorized to consume from the queue")
  }

  test("Consume authorized and JMSXUserID is set on message") {
    connect("1.1", client,
      "login:can_send_create_consume_queue\n" +
              "passcode:can_send_create_consume_queue\n")

    subscribe("0", "/queue/sendsid")
    async_send("/queue/sendsid", "hello")

    val frame = client.receive()
    frame should startWith("MESSAGE\n")
    frame should include("JMSXUserID:can_send_create_consume_queue\n")
    frame should include("sender-ip:127.0.0.1\n")
  }
}
