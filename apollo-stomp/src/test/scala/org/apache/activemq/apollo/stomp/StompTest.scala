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
import org.scalatest.BeforeAndAfterEach

class StompTestSupport extends FunSuiteSupport with ShouldMatchers with BeforeAndAfterEach {
  var broker: Broker = null

  override protected def beforeAll() = {
    val uri = "xml:classpath:activemq-stomp.xml"
    info("Loading broker configuration from the classpath with URI: " + uri)
    broker = BrokerFactory.createBroker(uri, true)
    Thread.sleep(1000); //TODO implement waitUntilStarted
  }

  var client = new StompClient

  override protected def afterAll() = {
    broker.stop
  }

  override protected def afterEach() = {
    super.afterEach
    client.close
  }

  def connect(version:String, c: StompClient = client) = {
    c.open("localhost", 61613)
    version match {
      case "1.0"=>
        c.write(
          "CONNECT\n" +
          "\n")
      case "1.1"=>
        c.write(
          "CONNECT\n" +
          "accept-version:1.1\n" +
          "host:default\n" +
          "\n")
      case x=> throw new RuntimeException("invalid version: %f".format(x))
    }
    val frame = c.receive()
    frame should startWith("CONNECTED\n")
    frame should include regex("""session:.+?\n""")
    frame should include("version:"+version+"\n")
    c
  }

  def wait_for_receipt(id:String, c: StompClient = client): Unit = {
    val frame = c.receive()
    frame should startWith("RECEIPT\n")
    frame should include("receipt-id:"+id+"\n")
  }

}

class Stomp10ConnectTest extends StompTestSupport {

  test("Stomp 1.0 CONNECT") {
    connect("1.0")
  }

}

class Stomp11ConnectTest extends StompTestSupport {

  test("Stomp 1.1 CONNECT") {
    connect("1.1")
  }

  test("Stomp 1.1 CONNECT /w STOMP Action") {

    client.open("localhost", 61613)

    client.write(
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

    client.open("localhost", 61613)

    client.write(
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

    client.open("localhost", 61613)

    client.write(
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

    client.open("localhost", 61613)

    client.write(
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

    client.open("localhost", 61613)

    client.write(
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
  
      client.open("localhost", 61613)

      client.write(
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

class StompDestinationTest extends StompTestSupport {

  test("Queue order preserved") {
    connect("1.1")

    def put(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/queue/example\n" +
        "\n" +
        "message:"+id+"\n")
    }
    put(1)
    put(2)
    put(3)

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/example\n" +
      "id:0\n" +
      "\n")

    def get(id:Int) = {
      val frame = client.receive()
      println(frame)
      frame should startWith("MESSAGE\n")
      frame should include ("subscription:0\n")
      frame should endWith regex("\n\nmessage:"+id+"\n")
    }
    get(1)
    get(2)
    get(3)
  }

  test("Topic drops messages sent before before subscription is established") {
    connect("1.1")

    def put(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/topic/updates\n" +
        "\n" +
        "message:"+id+"\n")
    }
    put(1)

    client.write(
      "SUBSCRIBE\n" +
      "destination:/topic/updates\n" +
      "id:0\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0")

    put(2)
    put(3)

    def get(id:Int) = {
      val frame = client.receive()
      println(frame)
      frame should startWith("MESSAGE\n")
      frame should include ("subscription:0\n")
      frame should endWith regex("\n\nmessage:"+id+"\n")
    }

    // note that the put(1) message gets dropped.
    get(2)
    get(3)
  }

  test("Topic /w Durable sub retains messages.") {
    connect("1.1")

    def put(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/topic/updates\n" +
        "\n" +
        "message:"+id+"\n")
    }

    client.write(
      "SUBSCRIBE\n" +
      "destination:/topic/updates\n" +
      "id:durable:my-sub-name\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0")
    client.close

    // Close him out.. since his id started /w durable: then
    // the topic subscription will be persistent accross client
    // connections.

    connect("1.1")
    put(1)
    put(2)
    put(3)

    client.write(
      "SUBSCRIBE\n" +
      "destination:/topic/updates\n" +
      "id:durable:my-sub-name\n" +
      "\n")

    def get(id:Int) = {
      val frame = client.receive()
      println(frame)
      frame should startWith("MESSAGE\n")
      frame should include ("subscription:durable:my-sub-name\n")
      frame should endWith regex("\n\nmessage:"+id+"\n")
    }

    get(1)
    get(2)
    get(3)
  }

  test("Queue and a selector") {
    connect("1.1")

    def put(id:Int, color:String) = {
      client.write(
        "SEND\n" +
        "destination:/queue/selected\n" +
        "color:"+color+"\n" +
        "\n" +
        "message:"+id+"\n")
    }
    put(1, "red")
    put(2, "blue")
    put(3, "red")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/selected\n" +
      "selector:color='red'\n" +
      "id:0\n" +
      "\n")

    def get(id:Int) = {
      val frame = client.receive()
      println(frame)
      frame should startWith("MESSAGE\n")
      frame should endWith regex("\n\nmessage:"+id+"\n")
    }
    get(1)
    get(3)
  }

  test("Topic and a selector") {
    connect("1.1")

    def put(id:Int, color:String) = {
      client.write(
        "SEND\n" +
        "destination:/topic/selected\n" +
        "color:"+color+"\n" +
        "\n" +
        "message:"+id+"\n")
    }

    client.write(
      "SUBSCRIBE\n" +
      "destination:/topic/selected\n" +
      "selector:color='red'\n" +
      "id:0\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0")

    put(1, "red")
    put(2, "blue")
    put(3, "red")

    def get(id:Int) = {
      val frame = client.receive()
      println(frame)
      frame should startWith("MESSAGE\n")
      frame should endWith regex("\n\nmessage:"+id+"\n")
    }
    get(1)
    get(3)
  }
}

class StompTransactionTest extends StompTestSupport {
  

  test("Queue and a transacted send") {
    connect("1.1")

    def put(id:Int, tx:String=null) = {
      client.write(
        "SEND\n" +
        "destination:/queue/transacted\n" +
        { if(tx!=null) { "transaction:"+tx+"\n" } else { "" } }+
        "\n" +
        "message:"+id+"\n")
    }

    put(1)
    client.write(
      "BEGIN\n" +
      "transaction:x\n" +
      "\n")
    put(2, "x")
    put(3)

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/transacted\n" +
      "id:0\n" +
      "\n")

    def get(id:Int) = {
      val frame = client.receive()
      println(frame)
      frame should startWith("MESSAGE\n")
      frame should endWith regex("\n\nmessage:"+id+"\n")
    }
    get(1)
    get(3)

    client.write(
      "COMMIT\n" +
      "transaction:x\n" +
      "\n")

    get(2)
    
  }

  test("Topic and a transacted send") {
    connect("1.1")

    def put(id:Int, tx:String=null) = {
      client.write(
        "SEND\n" +
        "destination:/topic/transacted\n" +
        { if(tx!=null) { "transaction:"+tx+"\n" } else { "" } }+
        "\n" +
        "message:"+id+"\n")
    }

    client.write(
      "SUBSCRIBE\n" +
      "destination:/topic/transacted\n" +
      "id:0\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0")

    put(1)
    client.write(
      "BEGIN\n" +
      "transaction:x\n" +
      "\n")
    put(2, "x")
    put(3)

    def get(id:Int) = {
      val frame = client.receive()
      println(frame)
      frame should startWith("MESSAGE\n")
      frame should endWith regex("\n\nmessage:"+id+"\n")
    }

    get(1)
    get(3)

    client.write(
      "COMMIT\n" +
      "transaction:x\n" +
      "\n")

    get(2)

  }

}


class StompAckModeTest extends StompTestSupport {

  test("ack:session redelivers on client disconnect") {
    connect("1.1")

    def put(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/queue/ackmode-session\n" +
        "\n" +
        "message:"+id+"\n")
    }
    put(1)
    put(2)
    put(3)

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/ackmode-session\n" +
      "ack:session\n" +
      "id:0\n" +
      "\n")

    def get(id:Int) = {
      val frame = client.receive()
      println(frame)
      frame should startWith("MESSAGE\n")
      frame should include ("subscription:0\n")
      frame should include regex("message-id:.+?\n")
      frame should endWith regex("\n\nmessage:"+id+"\n")

      val p = """(?s).*?\nmessage-id:(.+?)\n.*""".r
      frame match {
        case p(x) => x
        case _=> null
      }
    }

    get(1)
    val mid = get(2)
    get(3)

    // Ack the first 2 messages..
    client.write(
      "ACK\n" +
      "subscription:0\n" +
      "message-id:"+mid+"\n" +
      "receipt:0\n"+
      "\n")

    wait_for_receipt("0")
    client.close

    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/ackmode-session\n" +
      "ack:session\n" +
      "id:0\n" +
      "\n")
    get(3)
    
    
  }


  test("ack:message redelivers on client disconnect") {
    connect("1.1")

    def put(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/queue/ackmode-message\n" +
        "\n" +
        "message:"+id+"\n")
    }
    put(1)
    put(2)
    put(3)

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/ackmode-message\n" +
      "ack:message\n" +
      "id:0\n" +
      "\n")

    def get(id:Int) = {
      val frame = client.receive()
      println(frame)
      frame should startWith("MESSAGE\n")
      frame should include ("subscription:0\n")
      frame should include regex("message-id:.+?\n")
      frame should endWith regex("\n\nmessage:"+id+"\n")

      val p = """(?s).*?\nmessage-id:(.+?)\n.*""".r
      frame match {
        case p(x) => x
        case _=> null
      }
    }

    get(1)
    val mid = get(2)
    get(3)

    // Ack the first 2 messages..
    client.write(
      "ACK\n" +
      "subscription:0\n" +
      "message-id:"+mid+"\n" +
      "receipt:0\n"+
      "\n")

    wait_for_receipt("0")
    client.close

    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/ackmode-message\n" +
      "ack:session\n" +
      "id:0\n" +
      "\n")
    get(1)
    get(3)


  }

}