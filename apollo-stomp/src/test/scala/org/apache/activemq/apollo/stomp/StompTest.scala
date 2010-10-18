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
package org.apache.activemq.apollo.stomp

import org.scalatest.matchers.ShouldMatchers
import org.apache.activemq.apollo.util.FunSuiteSupport
import org.apache.activemq.apollo.broker.{Broker, BrokerFactory}

class StompTestSupport extends FunSuiteSupport with ShouldMatchers {
  var broker: Broker = null

  override protected def beforeAll() = {
    val uri = "xml:classpath:activemq-stomp.xml"
    info("Loading broker configuration from the classpath with URI: " + uri)
    broker = BrokerFactory.createBroker(uri, true)
    Thread.sleep(1000); //TODO implement waitUntilStarted
  }

  override protected def afterAll() = {
    broker.stop
  }


}

class Stomp10ConnectTest extends StompTestSupport {

  test("Stomp 1.0 CONNECT") {
    val client = new StompClient
    client.open("localhost", 61613)

    client.send(
      "CONNECT\n" +
      "\n")
    val frame = client.receive()
    frame should startWith("CONNECTED\n")
    frame should include regex("""session:.+?\n""")
    frame should include("version:1.0\n")
  }

}

class Stomp11ConnectTest extends StompTestSupport {

  test("Stomp 1.1 CONNECT") {
    val client = new StompClient
    client.open("localhost", 61613)

    client.send(
      "CONNECT\n" +
      "accept-version:1.0,1.1\n" +
      "host:default\n" +
      "\n")
    val frame = client.receive()
    frame should startWith("CONNECTED\n")
    frame should include regex("""session:.+?\n""")
    frame should include("version:1.1\n")
  }

  test("Stomp 1.1 CONNECT /w STOMP Action") {
    val client = new StompClient
    client.open("localhost", 61613)

    client.send(
      "STOMP\n" +
      "accept-version:1.0,1.1\n" +
      "host:default\n" +
      "\n")
    val frame = client.receive()
    frame should startWith("CONNECTED\n")
    frame should include regex("""session:.+?\n""")
    frame should include("version:1.1\n")
  }

  test("Stomp 1.1 CONNECT /w valid version fallback") {
    val client = new StompClient
    client.open("localhost", 61613)

    client.send(
      "CONNECT\n" +
      "accept-version:1.0,10.0\n" +
      "host:default\n" +
      "\n")
    val frame = client.receive()
    frame should startWith("CONNECTED\n")
    frame should include regex("""session:.+?\n""")
    frame should include("version:1.0\n")
  }

  test("Stomp 1.1 CONNECT /w invalid version fallback") {
    val client = new StompClient
    client.open("localhost", 61613)

    client.send(
      "CONNECT\n" +
      "accept-version:9.0,10.0\n" +
      "host:default\n" +
      "\n")
    val frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include regex("""version:.+?\n""")
    frame should include regex("""message:.+?\n""")
  }

  test("Stomp CONNECT /w invalid virtual host") {
    val client = new StompClient
    client.open("localhost", 61613)

    client.send(
      "CONNECT\n" +
      "accept-version:1.0,1.1\n" +
      "host:invalid\n" +
      "\n")
    val frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include regex("""message:.+?\n""")
  }

}

class Stomp11HeartBeatTest extends StompTestSupport {

  test("Stomp 1.1 Broker sends heart-beat") {
    val client = new StompClient
    client.open("localhost", 61613)

    client.send(
      "CONNECT\n" +
      "accept-version:1.1\n" +
      "host:default\n" +
      "heart-beat:0,1000\n" +
      "\n")
    val frame = client.receive()
    frame should startWith("CONNECTED\n")
    frame should include regex("""heart-beat:.+?\n""")

    def heart_beat_after(time:Long) {
      var start = System.currentTimeMillis
      val c = client.in.read()
      c should be === (Stomp.NEWLINE)
      var end = System.currentTimeMillis
      (end - start) should be >= time
    }
    client.in.read()
    heart_beat_after(900)
    heart_beat_after(900)
  }


  test("Stomp 1.1 Broker times out idle connection") {
    StompProtocolHandler.inbound_heartbeat = 1000L
    try {
      val client = new StompClient
      client.open("localhost", 61613)

      client.send(
        "CONNECT\n" +
        "accept-version:1.1\n" +
        "host:default\n" +
        "heart-beat:1000,0\n" +
        "\n")

      var frame = client.receive()
      frame should startWith("CONNECTED\n")
      frame should include regex("""heart-beat:.+?\n""")

      var start = System.currentTimeMillis

      frame = client.receive()
      frame should startWith("ERROR\n")
      frame should include regex("""message:.+?\n""")
      
      var end = System.currentTimeMillis
      (end - start) should be >= 1000L

    } finally {
      StompProtocolHandler.inbound_heartbeat = StompProtocolHandler.DEFAULT_INBOUND_HEARTBEAT
    }
  }

}