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
import org.scalatest.BeforeAndAfterEach
import java.lang.String
import org.apache.activemq.apollo.broker.{KeyStorage, Broker, BrokerFactory}
import org.apache.activemq.apollo.util.{FileSupport, Logging, FunSuiteSupport, ServiceControl}
import FileSupport._

class StompTestSupport extends FunSuiteSupport with ShouldMatchers with BeforeAndAfterEach with Logging {
  var broker: Broker = null
  var port = 0

  val broker_config_uri = "xml:classpath:apollo-stomp.xml"

  override protected def beforeAll() = {
    info("Loading broker configuration from the classpath with URI: " + broker_config_uri)
    broker = BrokerFactory.createBroker(broker_config_uri)
    ServiceControl.start(broker, "Starting broker")
    port = broker.connectors.head.transportServer.getSocketAddress.getPort
  }

  var client = new StompClient
  var clients = List[StompClient]()

  override protected def afterAll() = {
    broker.stop
  }

  override protected def afterEach() = {
    super.afterEach
    clients.foreach(_.close)
    clients = Nil
  }

  def connect_request(version:String, c: StompClient, headers:String="") = {
    c.open("localhost", port)
    version match {
      case "1.0"=>
        c.write(
          "CONNECT\n" +
          headers +
          "\n")
      case "1.1"=>
        c.write(
          "CONNECT\n" +
          "accept-version:1.1\n" +
          "host:localhost\n" +
          headers +
          "\n")
      case x=> throw new RuntimeException("invalid version: %f".format(x))
    }
    clients ::= c
    c.receive()
  }

  def connect(version:String, c: StompClient = client, headers:String="") = {
    val frame = connect_request(version, c, headers)
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

    client.open("localhost", port)

    client.write(
      "STOMP\n" +
      "accept-version:1.0,1.1\n" +
      "host:localhost\n" +
      "\n")
    val frame = client.receive()
    frame should startWith("CONNECTED\n")
    frame should include regex("""session:.+?\n""")
    frame should include("version:1.1\n")
  }

  test("Stomp 1.1 CONNECT /w valid version fallback") {

    client.open("localhost", port)

    client.write(
      "CONNECT\n" +
      "accept-version:1.0,10.0\n" +
      "host:localhost\n" +
      "\n")
    val frame = client.receive()
    frame should startWith("CONNECTED\n")
    frame should include regex("""session:.+?\n""")
    frame should include("version:1.0\n")
  }

  test("Stomp 1.1 CONNECT /w invalid version fallback") {

    client.open("localhost", port)

    client.write(
      "CONNECT\n" +
      "accept-version:9.0,10.0\n" +
      "host:localhost\n" +
      "\n")
    val frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include regex("""version:.+?\n""")
    frame should include regex("""message:.+?\n""")
  }

  test("Stomp CONNECT /w invalid virtual host") {

    client.open("localhost", port)

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

    client.open("localhost", port)

    client.write(
      "CONNECT\n" +
      "accept-version:1.1\n" +
      "host:localhost\n" +
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
  
      client.open("localhost", port)

      client.write(
        "CONNECT\n" +
        "accept-version:1.1\n" +
        "host:localhost\n" +
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

  test("Queues load balance across subscribers") {
    connect("1.1")

    // Connect to subscribers
    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/load-balanced\n" +
      "id:1\n" +
      "\n")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/load-balanced\n" +
      "receipt:0\n"+
      "id:2\n" +
      "\n")

    wait_for_receipt("0")

    def put(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/queue/load-balanced\n" +
        "\n" +
        "message:"+id+"\n")
    }

    for( i <- 0 until 4) {
      put(i)
    }

    var sub1_counter=0
    var sub2_counter=0

    def get() = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")

      if( frame.contains("subscription:1\n") ) {
        sub1_counter += 1
      } else if( frame.contains("subscription:2\n") ) {
        sub2_counter += 1
      }
    }

    for( i <- 0 until 4) {
      get()
    }

    sub1_counter should be(2)
    sub2_counter should be(2)

  }

  test("Queues do NOT load balance across exclusive subscribers") {
    connect("1.1")

    // Connect to subscribers
    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/exclusive\n" +
      "id:1\n" +
      "\n")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/exclusive\n" +
      "exclusive:true\n"+
      "receipt:0\n"+
      "ack:client\n"+
      "id:2\n" +
      "\n")

    wait_for_receipt("0")

    def put(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/queue/exclusive\n" +
        "\n" +
        "message:"+id+"\n")
    }

    for( i <- 0 until 4) {
      put(i)
    }

    var sub1_counter=0
    var sub2_counter=0

    def get() = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")

      if( frame.contains("subscription:1\n") ) {
        sub1_counter += 1
      } else if( frame.contains("subscription:2\n") ) {
        sub2_counter += 1
      }
    }

    for( i <- 0 until 4) {
      get()
    }

    sub1_counter should be(0)
    sub2_counter should be(4)

    // disconnect the exclusive subscriber.
    client.write(
      "UNSUBSCRIBE\n" +
      "id:2\n" +
      "\n")

    // sub 1 should now get all the messages.
    for( i <- 0 until 4) {
      get()
    }
    sub1_counter should be(4)

  }

  test("Queue browsers don't consume the messages") {
    connect("1.1")

    def put(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/queue/browsing\n" +
        "receipt:0\n" +
        "\n" +
        "message:"+id+"\n")
      wait_for_receipt("0")
    }

    put(1)
    put(2)
    put(3)

    // create a browser subscription.
    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/browsing\n" +
      "browser:true\n" +
      "id:0\n" +
      "\n")

    def get(sub:Int, id:Int) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should include ("subscription:%d\n".format(sub))
      frame should endWith regex("\n\nmessage:%d\n".format(id))
    }
    get(0,1)
    get(0,2)
    get(0,3)

    // Should get a browse end message
    val frame = client.receive()
    frame should startWith("MESSAGE\n")
    frame should include ("subscription:0\n")
    frame should include ("browser:end\n")

    // create a regular subscription.
    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/browsing\n" +
      "id:1\n" +
      "\n")

    get(1,1)
    get(1,2)
    get(1,3)

  }

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
      "id:my-sub-name\n" +
      "persistent:true\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0")
    client.close

    // Close him out.. since persistent:true then
    // the topic subscription will be persistent accross client
    // connections.

    connect("1.1")
    put(1)
    put(2)
    put(3)

    client.write(
      "SUBSCRIBE\n" +
      "destination:/topic/updates\n" +
      "id:my-sub-name\n" +
      "persistent:true\n" +
      "\n")

    def get(id:Int) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should include ("subscription:my-sub-name\n")
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
      frame should startWith("MESSAGE\n")
      frame should endWith regex("\n\nmessage:"+id+"\n")
    }
    get(1)
    get(3)
  }

}

class StompSslDestinationTest extends StompDestinationTest {
  override val broker_config_uri: String = "xml:classpath:apollo-stomp-ssl.xml"

  client.key_storeage = new KeyStorage
  client.key_storeage.config.file = basedir/"src"/"test"/"resources"/"client.ks"
  client.key_storeage.config.password = "password"
  client.key_storeage.config.key_password = "password"

}

class StompReceiptTest extends StompTestSupport {

  test("Receipts on SEND to unconsummed topic") {
    connect("1.1")

    def put(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/topic/receipt-test\n" +
        "receipt:"+id+"\n" +
        "\n" +
        "message:"+id+"\n")
    }

    put(1)
    put(2)
    wait_for_receipt("1")
    wait_for_receipt("2")


  }

  test("Receipts on SEND to a consumed topic") {
    connect("1.1")

    def put(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/topic/receipt-test\n" +
        "receipt:"+id+"\n" +
        "\n" +
        "message:"+id+"\n")
    }

    // start a consumer on a different connection
    var consumer = new StompClient
    connect("1.1", consumer)
    consumer.write(
      "SUBSCRIBE\n" +
      "destination:/topic/receipt-test\n" +
      "id:0\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0", consumer)

    put(1)
    put(2)
    wait_for_receipt("1")
    wait_for_receipt("2")
    
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

class StompSecurityTest extends StompTestSupport {

  override val broker_config_uri: String = "xml:classpath:apollo-stomp-secure.xml"

  override protected def beforeAll = {
    try {
      val login_file = new java.io.File(getClass.getClassLoader.getResource("login.config").getFile())
      System.setProperty("java.security.auth.login.config", login_file.getCanonicalPath)
    } catch {
      case x:Throwable => x.printStackTrace
    }
    super.beforeAll
  }

  test("Connect with valid id password but can't connect") {

    val frame = connect_request("1.1", client,
      "login:can_not_connect\n" +
      "passcode:can_not_connect\n")
    frame should startWith("ERROR\n")
    frame should include("message:Connect not authorized.\n")

  }

  test("Connect with no id password") {
    val frame = connect_request("1.1", client)
    frame should startWith("ERROR\n")
    frame should include("message:Authentication failed.\n")
  }

  test("Connect with invalid id password") {
    val frame = connect_request("1.1", client,
      "login:foo\n" +
      "passcode:bar\n")
    frame should startWith("ERROR\n")
    frame should include("message:Authentication failed.\n")

  }

  test("Connect with valid id password that can connect") {
    connect("1.1", client,
      "login:can_only_connect\n" +
      "passcode:can_only_connect\n")

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
    frame should include("message:Not authorized to send to the queue\n")
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
    frame should include("message:Not authorized to create the queue\n")

  }

//
//  test("Consume authorized but not create") {
//    connect("1.1", client,
//      "login:can_consume_queue\n" +
//      "passcode:can_consume_queue\n")
//
//    client.write(
//      "SUBSCRIBE\n" +
//      "destination:/queue/secure\n" +
//      "id:0\n" +
//      "receipt:0\n" +
//      "\n")
//    wait_for_receipt("0")
//
//    val frame = client.receive()
//    frame should startWith("ERROR\n")
//    frame should include("message:Not authorized to create the queue\n")
//  }

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
    frame should include("message:Not authorized to consume from the queue\n")
  }

//  test("Consume authorized and JMSXUserID is set on message") {
//    connect("1.1", client,
//      "login:can_consume_queue\n" +
//      "passcode:can_consume_queue\n")
//
//    client.write(
//      "SUBSCRIBE\n" +
//      "destination:/queue/secure\n" +
//      "id:0\n" +
//      "\n")
//
//    val frame = client.receive()
//    frame should startWith("MESSAGE\n")
//    frame should include("JMSXUserID:can_send_create_queue\n")
//  }
}

class StompSslSecurityTest extends StompTestSupport {

  override val broker_config_uri: String = "xml:classpath:apollo-stomp-ssl-secure.xml"

  override protected def beforeAll = {
    // System.setProperty("javax.net.debug", "all")
    try {
      val login_file = new java.io.File(getClass.getClassLoader.getResource("login.config").getFile())
      System.setProperty("java.security.auth.login.config", login_file.getCanonicalPath)
    } catch {
      case x:Throwable => x.printStackTrace
    }
    super.beforeAll
  }

  def use_client_cert = {
    client.key_storeage = new KeyStorage
    client.key_storeage.config.file = basedir/"src"/"test"/"resources"/"client.ks"
    client.key_storeage.config.password = "password"
    client.key_storeage.config.key_password = "password"
  }

  test("Connect with cert and no id password") {
    use_client_cert
    connect("1.1", client)
  }

}
