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

import java.util.concurrent.TimeUnit._
import java.nio.channels.DatagramChannel
import org.fusesource.hawtbuf.AsciiBuffer
import org.apache.activemq.apollo.broker._
import java.net.{SocketTimeoutException, InetSocketAddress}
import org.apache.activemq.apollo.stomp.{Stomp, StompProtocolHandler}
import org.fusesource.hawtdispatch._
import collection.mutable
import java.util.concurrent.atomic.{AtomicLong, AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, ConcurrentHashMap}

/**
 * These tests can be run in parallel against a single Apollo broker.
 */
class StompParallelTest extends StompTestSupport with BrokerParallelTestExecution {

  def skip_if_using_store = skip(broker_config_uri.endsWith("-bdb.xml") || broker_config_uri.endsWith("-leveldb.xml"))
  def skip_if_not_using_store = skip(!(broker_config_uri.endsWith("-bdb.xml") || broker_config_uri.endsWith("-leveldb.xml")))

  test("Stomp 1.0 CONNECT") {
    connect("1.0")
  }

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
    frame should include regex ("""session:.+?\n""")
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
    frame should include regex ("""session:.+?\n""")
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
    frame should include regex ("""version:.+?\n""")
    frame should include regex ("""message:.+?\n""")
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
    frame should include regex ("""message:.+?\n""")
  }

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
    frame should include regex ("""heart-beat:.+?\n""")

    def heart_beat_after(time: Long) {
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
      frame should include regex ("""heart-beat:.+?\n""")

      var start = System.currentTimeMillis

      frame = client.receive()
      frame should startWith("ERROR\n")
      frame should include regex ("""message:.+?\n""")

      var end = System.currentTimeMillis
      (end - start) should be >= 1000L

    } finally {
      StompProtocolHandler.inbound_heartbeat = StompProtocolHandler.DEFAULT_INBOUND_HEARTBEAT
    }
  }

  test("UDP to STOMP interop") {

    connect("1.1")
    subscribe("0", "/topic/udp")

    val udp_port: Int = connector_port("udp").get
    val channel = DatagramChannel.open();

    val target = new InetSocketAddress("127.0.0.1", udp_port)
    channel.send(new AsciiBuffer("Hello").toByteBuffer, target)

    assert_received("Hello")
    channel.send(new AsciiBuffer("World").toByteBuffer, target)
    assert_received("World")
  }

  test("STOMP UDP to STOMP interop") {

    connect("1.1")
    subscribe("0", "/topic/some-other-udp")

    val udp_port: Int = connector_port("stomp-udp").get
    val channel = DatagramChannel.open();
    info("The UDP port is: "+udp_port)

    val target = new InetSocketAddress("127.0.0.1", udp_port)
    channel.send(new AsciiBuffer(
      "SEND\n" +
      "destination:/topic/some-other-udp\n" +
      "login:admin\n" +
      "passcode:password\n" +
      "\n" +
      "Hello\u0000\n").toByteBuffer, target)
    assert_received("Hello")

    channel.send(new AsciiBuffer(
      "SEND\n" +
      "destination:/topic/some-other-udp\n" +
      "login:admin\n" +
      "passcode:password\n" +
      "\n" +
      "World\u0000\n").toByteBuffer, target)
    assert_received("World")
  }

//  /**
//   * These disconnect tests assure that we don't drop message deliviers that are in flight
//   * if a client disconnects before those deliveries are accepted by the target destination.
//   */
//  test("Messages delivery assured to a queued once a disconnect receipt is received") {
//
//    // figure out at what point a quota'ed queue stops accepting more messages.
//    connect("1.1")
//    val dest_base = next_id("/queue/quota.assured")
//    var dest = dest_base+"-1"
//    client.socket.setSoTimeout(1 * 1000)
//    var block_count = 0
//    var start_bytes = client.bytes_written
//    var wrote = 0L
//    try {
//      receipt_counter.set(0L)
//      while (true) {
//        sync_send(dest, "%01024d".format(block_count), "message-id:"+block_count+"\n")
//        wrote = client.bytes_written - start_bytes
//        block_count += 1
//      }
//    } catch {
//      case e: SocketTimeoutException =>
//    }
//    close()
//
//    dest = dest_base+"-2"
//
//    connect("1.1")
//    receipt_counter.set(0L)
//    start_bytes = client.bytes_written
//    for (i <- 0 until block_count-1) {
//      sync_send(dest, "%01024d".format(i), "message-id:"+i+"\n")
//    }
//
//    async_send(dest, "%01024d".format(block_count-1),
//      "message-id:"+(block_count-1)+"\n"+
//      "receipt:" + receipt_counter.incrementAndGet() + "\n")
//
//    // lets make sure the amount of data we sent the first time.. matches the 2nd time.
//    ( client.bytes_written - start_bytes ).should(be(wrote))
//
//    disconnect()
//
//    // Lets make sure non of the messages were dropped.
//    connect("1.1")
//    subscribe("0", dest)
//    for (i <- 0 until block_count) {
//      assert_received("%01024d".format(i))
//    }
//    disconnect()
//  }
//
//  test("Messages delivery assured to a topic once a disconnect receipt is received") {
//
//    //setup a subscription which will block quickly..
//    val consumer = new StompClient
//    val dest_base = next_id("/topic/quota.assured")
//    var dest = dest_base+"-1"
//
//    connect("1.1", consumer)
//    subscribe("0", dest, "client", headers = "credit:1,0\n", c = consumer)
//
//    // figure out at what point a quota'ed consumer stops accepting more messages.
//    connect("1.1")
//    client.socket.setSoTimeout(1 * 1000)
//    var block_count = 0
//    try {
//      receipt_counter.set(0L)
//      while (true) {
//        sync_send(dest, "%01024d".format(block_count), "message-id:"+block_count+"\n")
//        block_count += 1
//      }
//    } catch {
//      case e: SocketTimeoutException =>
//    }
//
//    close()
//    close(consumer)
//
//    dest = dest_base+"-2"
//
//    connect("1.1", consumer)
//    subscribe("1", dest, "client", headers = "credit:1,0\n", c = consumer)
//
//    connect("1.1")
//    receipt_counter.set(0L)
//    for (i <- 0 until block_count-1) {
//      sync_send(dest, "%01024d".format(i), "message-id:"+i+"\n")
//    }
//    async_send(dest, "%01024d".format(block_count-1), "message-id:"+(block_count-1)+"\n")
//    disconnect()
//
//    // Lets make sure non of the messages were dropped.
//    for (i <- 0 until block_count-1) {
//      assert_received("%01024d".format(i), c = consumer)(true)
//    }
//    disconnect(consumer)
//  }

//  test("APLO-206 - Load balance of job queues using small consumer credit windows") {
//
//    connect("1.1")
//
//    for (i <- 1 to 4) {
//      async_send("/queue/load-balanced2", i)
//    }
//
//    subscribe("1", "/queue/load-balanced2", "client", false, "credit:1,0\n")
//    val ack1 = assert_received(1, "1")
//
//    subscribe("2", "/queue/load-balanced2", "client", false, "credit:1,0\n")
//    val ack2 = assert_received(2, "2")
//
//    // Ok lets ack now..
//    ack1(true)
//    val ack3 = assert_received(3, "1")
//
//    ack2(true)
//    val ack4 = assert_received(4, "2")
//  }

  test("Browsing queues does not cause AssertionError.  Reported in APLO-156") {
    skip_if_using_store
    connect("1.1")
    subscribe("0", "/queue/TOOL.DEFAULT")
    async_send("/queue/TOOL.DEFAULT", "1")
    async_send("/queue/TOOL.DEFAULT", "2")
    assert_received("1", "0")
    assert_received("2", "0")
    subscribe("1", "/queue/TOOL.DEFAULT", "auto", false, "browser:true\n")
    val frame = client.receive()
    frame should startWith(
      "MESSAGE\n" +
              "subscription:1\n" +
              "destination:\n" +
              "message-id:\n" +
              "browser:end")
  }

  test("retain:set makes a topic remeber the message") {
    connect("1.1")
    async_send("/topic/retained-example", 1)
    async_send("/topic/retained-example", 2, "retain:set\n")
    sync_send("/topic/retained-example", 3)
    subscribe("0", "/topic/retained-example")
    assert_received(2)
    async_send("/topic/retained-example", 4)
    assert_received(4)
  }

  test("retain:remove makes a topic forget the message") {
    connect("1.1")
    async_send("/topic/retained-example2", 1)
    async_send("/topic/retained-example2", 2, "retain:set\n")
    async_send("/topic/retained-example2", 3, "retain:remove\n")
    subscribe("0", "/topic/retained-example2")
    async_send("/topic/retained-example2", 4)
    assert_received(4)
  }

  test("Setting `from-seq` header to -1 results in subscription starting at end of the queue.") {
    skip_if_using_store
    connect("1.1")
    async_send("/queue/from-seq-end", 1)
    async_send("/queue/from-seq-end", 2)
    sync_send("/queue/from-seq-end", 3)
    subscribe("0", "/queue/from-seq-end", headers=
                  "browser:true\n" +
                  "browser-end:false\n" +
                  "from-seq:-1\n" )
    async_send("/queue/from-seq-end", 4)
    assert_received(4)
  }

  test("The `browser-end:false` can be used to continously browse a queue.") {
    connect("1.1")
    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/browsing-continous\n" +
              "browser:true\n" +
              "browser-end:false\n" +
              "receipt:0\n" +
              "id:0\n" +
              "\n")
    wait_for_receipt("0")

    def send(id: Int) = client.write(
      "SEND\n" +
              "destination:/queue/browsing-continous\n" +
              "\n" +
              "message:" + id + "\n")

    send(1)
    send(2)

    def get(seq: Long) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      expect(true)(frame.contains("message:" + seq + "\n"))
    }
    get(1)
    get(2)
  }

  test("Message sequence headers are added when `include-seq` is used.") {
    connect("1.1")
    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/seq_queue\n" +
              "receipt:0\n" +
              "id:0\n" +
              "include-seq:seq\n" +
              "\n")
    wait_for_receipt("0")

    def send(id: Int) = client.write(
      "SEND\n" +
              "destination:/queue/seq_queue\n" +
              "\n" +
              "message:" + id + "\n")

    send(1)
    send(2)

    def get(seq: Long) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      expect(true)(frame.contains("seq:" + seq + "\n"))
    }
    get(1)
    get(2)
  }

  test("The `from-seq` header can be used to resume delivery from a given point in a queue.") {
    skip_if_using_store
    connect("1.1")

    def send(id: Int) = {
      client.write(
        "SEND\n" +
                "destination:/queue/from_queue\n" +
                "receipt:0\n" +
                "\n" +
                "message:" + id + "\n")
      wait_for_receipt("0")
    }

    send(1)
    send(2)
    send(3)

    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/from_queue\n" +
              "receipt:0\n" +
              "browser:true\n" +
              "id:0\n" +
              "include-seq:seq\n" +
              "from-seq:2\n" +
              "\n")
    wait_for_receipt("0")

    def get(seq: Long) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should include("seq:" + seq + "\n")
    }
    get(2)
    get(3)
  }


  test("The `from-seq` header is not supported with wildcard or composite destinations.") {
    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/some,/queue/other\n" +
              "browser:true\n" +
              "id:0\n" +
              "include-seq:seq\n" +
              "from-seq:2\n" +
              "\n")

    var frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include("message:The from-seq header is only supported when you subscribe to one destination")

    client.close
    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/some.*\n" +
              "browser:true\n" +
              "id:0\n" +
              "include-seq:seq\n" +
              "from-seq:2\n" +
              "\n")

    frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include("message:The from-seq header is only supported when you subscribe to one destination")
  }

  test("Selector Syntax") {
    connect("1.1")

    var sub_id = 0;
    def test_selector(selector: String, headers: List[String], expected_matches: List[Int]) = {
      subscribe(""+sub_id, "/topic/selected-"+sub_id, headers="selector:" + selector + "\n")
      var id = 1;
      for( header <- headers) {
        async_send("/topic/selected-"+sub_id, "message:%d:%d\n".format(sub_id, id), header + "\n")
        id += 1;
      }
      for( id <- expected_matches) {
          val frame = client.receive()
          frame should startWith("MESSAGE\n")
          frame should endWith("\n\nmessage:%d:%d\n".format(sub_id, id))
      }
      unsubscribe(""+sub_id)
      sub_id += 1
    }

    test_selector("color = 'red'", List("color:blue", "not:set", "color:red"), List(3))
    test_selector("age >= 21", List("age:3", "not:set", "age:21", "age:30"), List(3, 4))
    test_selector("hyphen - 5 = 5", List("hyphen:9", "not:set", "hyphen:10"), List(3))
    test_selector("hyphen -5 = 5", List("hyphen:9", "not:set", "hyphen:10"), List(3))
    test_selector("hyphen-5 = 5", List("hyphen:9", "hyphen-5:5", "not:set", "hyphen:10"), List(2))
  }

  test("Queues load balance across subscribers") {
    connect("1.1")
    subscribe("1", "/queue/load-balanced")
    subscribe("2", "/queue/load-balanced")

    for (i <- 0 until 4) {
      async_send("/queue/load-balanced", "message:" + i)
    }

    var sub1_counter = 0
    var sub2_counter = 0

    def get() = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")

      if (frame.contains("subscription:1\n")) {
        sub1_counter += 1
      } else if (frame.contains("subscription:2\n")) {
        sub2_counter += 1
      }
    }

    for (i <- 0 until 4) {
      get()
    }

    sub1_counter should be(2)
    sub2_counter should be(2)

  }

  test("Queues do not load balance on queues with round_robin=false") {
    connect("1.1")
    subscribe("1", "/queue/noroundrobin.test1")
    subscribe("2", "/queue/noroundrobin.test1")

    for (i <- 0 until 4) {
      async_send("/queue/noroundrobin.test1", "message:" + i)
    }

    var sub1_counter = 0
    var sub2_counter = 0

    for (i <- 0 until 4) {
      val (frame, ack) = receive_message()
      if (frame.contains("subscription:1\n")) {
        sub1_counter += 1
      } else if (frame.contains("subscription:2\n")) {
        sub2_counter += 1
      }
    }

    sub2_counter should be(0)
    sub1_counter should be(4)
  }

  test("Message groups are sticky to a consumer") {

    val dest = next_id("/queue/msggroups")
    connect("1.1")
    subscribe("1", dest)
    subscribe("2", dest)

    var actual_mapping = mutable.HashMap[String, mutable.HashSet[Char]]()

    def send_receive = {
      for (i <- 0 until 26 ) { async_send(dest, "data", "message_group:"+('a'+i).toChar+"\n") }
      for (i <- 0 until 26 ) {
        val (frame, ack) = receive_message()
        for( sub <- List("1", "2", "3") if( frame.contains("subscription:"+sub+"\n")) ) {
          val set = actual_mapping.getOrElseUpdate(sub, mutable.HashSet())
          for (i <- 0 until 26 ) {
            var c = ('a' + i).toChar
            if( frame.contains("message_group:"+c+"\n")) {
              set.add(c)
            }
          }
        }
        ack
      }
    }

    send_receive

    var expected_mapping = actual_mapping
    info(expected_mapping.toString())
    expected_mapping.get("1").get.intersect(expected_mapping.get("2").get).isEmpty should be(true)

    actual_mapping = mutable.HashMap[String, mutable.HashSet[Char]]()
    // Send more messages in, make sure they stay mapping to same consumers.
    send_receive; send_receive; send_receive; send_receive

    actual_mapping should be (expected_mapping)

    // Add another subscriber, the groups should re-balance
    subscribe("3", dest)

    actual_mapping = mutable.HashMap[String, mutable.HashSet[Char]]()
    send_receive
    expected_mapping = actual_mapping
    info(expected_mapping.toString())

    expected_mapping.get("1").get.intersect(expected_mapping.get("2").get).isEmpty should be(true)
    expected_mapping.get("2").get.intersect(expected_mapping.get("3").get).isEmpty should be(true)
    expected_mapping.get("1").get.intersect(expected_mapping.get("3").get).isEmpty should be(true)

    actual_mapping = mutable.HashMap[String, mutable.HashSet[Char]]()
    // Send more messages in, make sure they stay mapping to same consumers.
    send_receive; send_receive; send_receive; send_receive
    actual_mapping should be (expected_mapping)

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
              "exclusive:true\n" +
              "receipt:0\n" +
              "ack:client\n" +
              "id:2\n" +
              "\n")

    wait_for_receipt("0")

    def put(id: Int) = {
      client.write(
        "SEND\n" +
                "destination:/queue/exclusive\n" +
                "\n" +
                "message:" + id + "\n")
    }

    for (i <- 0 until 4) {
      put(i)
    }

    var sub1_counter = 0
    var sub2_counter = 0

    def get() = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")

      if (frame.contains("subscription:1\n")) {
        sub1_counter += 1
      } else if (frame.contains("subscription:2\n")) {
        sub2_counter += 1
      }
    }

    for (i <- 0 until 4) {
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
    for (i <- 0 until 4) {
      get()
    }
    sub1_counter should be(4)

  }

  test("Queue browsers don't consume the messages") {
    skip_if_using_store
    connect("1.1")

    def put(id: Int) = {
      client.write(
        "SEND\n" +
                "destination:/queue/browsing\n" +
                "receipt:0\n" +
                "\n" +
                "message:" + id + "\n")
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

    def get(sub: Int, id: Int) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should include("subscription:%d\n".format(sub))
      frame should endWith regex ("\n\nmessage:%d\n".format(id))
    }
    get(0, 1)
    get(0, 2)
    get(0, 3)

    // Should get a browse end message
    val frame = client.receive()
    frame should startWith("MESSAGE\n")
    frame should include("subscription:0\n")
    frame should include("browser:end\n")
    frame should include("\nmessage-id:")
    frame should include("\ndestination:")

    // create a regular subscription.
    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/browsing\n" +
              "id:1\n" +
              "\n")

    get(1, 1)
    get(1, 2)
    get(1, 3)

  }

  test("Queue order preserved") {
    connect("1.1")
    async_send("/queue/example", 1)
    async_send("/queue/example", 2)
    async_send("/queue/example", 3)
    subscribe("0", "/queue/example")
    assert_received(1, "0")
    assert_received(2, "0")
    assert_received(3, "0")
  }

  test("Topic drops messages sent before before subscription is established") {
    connect("1.1")

    def put(id: Int) = {
      client.write(
        "SEND\n" +
                "destination:/topic/updates1\n" +
                "\n" +
                "message:" + id + "\n")
    }
    put(1)

    client.write(
      "SUBSCRIBE\n" +
              "destination:/topic/updates1\n" +
              "id:0\n" +
              "receipt:0\n" +
              "\n")
    wait_for_receipt("0")

    put(2)
    put(3)

    def get(id: Int) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should include("subscription:0\n")
      frame should endWith regex ("\n\nmessage:" + id + "\n")
    }

    // note that the put(1) message gets dropped.
    get(2)
    get(3)
  }

  test("Topic /w Durable sub retains messages.") {
    connect("1.1")
    val dest = next_id("/topic/dsub_test_")
    val subid = next_id("my-sub-name_")
    subscribe(subid, dest, persistent=true)
    client.close

    // Close him out.. since persistent:true then
    // the topic subscription will be persistent accross client
    // connections.
    connect("1.1")
    async_send(dest, 1)
    async_send(dest, 2)
    async_send(dest, 3)

    subscribe(subid, dest, persistent=true)

    assert_received(1, subid)
    assert_received(2, subid)
    assert_received(3, subid)
  }

  test("Topic /w Wildcard durable sub retains messages.") {
    connect("1.1")
    val dest = next_id("/topic/dsub_test_")
    val subid = next_id("my-sub-name_")
    subscribe(subid, dest+".*", persistent=true)
    client.close

    // Close him out.. since persistent:true then
    // the topic subscription will be persistent across client
    // connections.
    connect("1.1")
    async_send(dest+".1", 1)
    async_send(dest+".2", 2)
    async_send(dest+".3", 3)

    subscribe(subid, dest, persistent=true)

    assert_received(1, subid)
    assert_received(2, subid)
    assert_received(3, subid)
  }

  test("Queue and a selector") {
    connect("1.1")

    def put(id: Int, color: String) = {
      client.write(
        "SEND\n" +
                "destination:/queue/selected\n" +
                "color:" + color + "\n" +
                "\n" +
                "message:" + id + "\n")
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

    def get(id: Int) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith regex ("\n\nmessage:" + id + "\n")
    }
    get(1)
    get(3)
  }

  test("Topic and a selector") {
    connect("1.1")

    def put(id: Int, color: String) = {
      client.write(
        "SEND\n" +
                "destination:/topic/selected\n" +
                "color:" + color + "\n" +
                "\n" +
                "message:" + id + "\n")
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

    def get(id: Int) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith regex ("\n\nmessage:" + id + "\n")
    }
    get(1)
    get(3)
  }

  test("Topic gets copy of message sent to queue") {
    connect("1.1")
    subscribe("1", "/topic/mirrored.a")
    async_send("/queue/mirrored.a", "message:1\n")
    assert_received("message:1\n")
  }

  test("Queue gets copy of message sent to topic") {
    connect("1.1")

    // Connect to subscribers
    subscribe("1", "/queue/mirrored.b")
    for( i <- 1 to 10 ) {
      async_send("/topic/mirrored.b", i)
      assert_received(i)
    }
  }

  test("Queue does not get copies from topic until it's first created") {
    connect("1.1")

    def put(id: Int) = {
      client.write(
        "SEND\n" +
                "destination:/topic/mirrored.c\n" +
                "\n" +
                "message:" + id + "\n")
    }

    put(1)

    // Connect to subscribers
    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/mirrored.c\n" +
              "id:1\n" +
              "receipt:0\n" +
              "\n")
    wait_for_receipt("0")

    put(2)

    def get(id: Int) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith regex ("\n\nmessage:" + id + "\n")
    }
    get(2)
  }

  def path_separator = "."

  test("Messages Expire") {
    connect("1.1")

    def put(msg: String, ttl: Option[Long] = None) = {
      val expires_header = ttl.map(t => "expires:" + (System.currentTimeMillis() + t) + "\n").getOrElse("")
      client.write(
        "SEND\n" +
                expires_header +
                "destination:/queue/exp\n" +
                "\n" +
                "message:" + msg + "\n")
    }

    put("1")
    put("2", Some(1000L))
    put("3")

    Thread.sleep(2000)

    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/exp\n" +
              "id:1\n" +
              "receipt:0\n" +
              "\n")
    wait_for_receipt("0")


    def get(dest: String) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith("\n\nmessage:%s\n".format(dest))
    }

    get("1")
    get("3")
  }

  test("Messages Expire Using TTL") {
    connect("1.1")

    def put(msg: String, ttl: Option[Long] = None) = {
      val ttl_header = ttl.map(t => "ttl:" + t + "\n").getOrElse("")
      client.write(
        "SEND\n" +
                ttl_header +
                "destination:/queue/exp2\n" +
                "\n" +
                "message:" + msg + "\n")
    }

    put("1")
    put("2", Some(1000L))
    put("3")

    Thread.sleep(2000)

    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/exp2\n" +
              "id:1\n" +
              "receipt:0\n" +
              "\n")
    wait_for_receipt("0")


    def get(dest: String) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith("\n\nmessage:%s\n".format(dest))
    }

    get("1")
    get("3")
  }

  test("Expired message sent to DLQ") {
    connect("1.1")

    val now = System.currentTimeMillis()
    var dest = "/queue/nacker.expires"
    async_send(dest, "1")
    async_send(dest, "2", "expires:"+(now+500)+"\n")
    sync_send(dest, "3")

    Thread.sleep(1000)
    subscribe("a", dest)
    assert_received("1", "a")
    assert_received("3", "a")

    subscribe("b", "/queue/dlq.nacker.expires")
    assert_received("2", "b")
  }

  test("Receipts on SEND to unconsummed topic") {
    connect("1.1")

    def put(id: Int) = {
      client.write(
        "SEND\n" +
                "destination:/topic/receipt-test\n" +
                "receipt:" + id + "\n" +
                "\n" +
                "message:" + id + "\n")
    }

    put(1)
    put(2)
    wait_for_receipt("1")
    wait_for_receipt("2")


  }

  test("Receipts on SEND to a consumed topic") {
    connect("1.1")

    def put(id: Int) = {
      client.write(
        "SEND\n" +
                "destination:/topic/receipt-test\n" +
                "receipt:" + id + "\n" +
                "\n" +
                "message:" + id + "\n")
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

  test("Transacted commit after unsubscribe") {
    val producer = new StompClient
    val consumer = new StompClient

    connect("1.1", producer)
    connect("1.1", consumer)

    // subscribe the consumer
    subscribe("0", "/queue/test", "client-individual", false, "", true, consumer)

    // begin the transaction on the consumer
    consumer.write(
      "BEGIN\n" +
              "transaction:x\n" +
              "\n")

    sync_send("/queue/test", "Hello world", "", producer)

    val ack = assert_received("Hello world", "0", consumer, "x")
    ack(true)

    unsubscribe("0", "", consumer)

    consumer.write(
      "COMMIT\n" +
              "transaction:x\n" +
              "\n")

    sync_send("/queue/test", "END", "", producer)
    subscribe("1", "/queue/test", c = producer)
    assert_received("END", "1", producer)
    // since we committed the transaction AFTER un-subscribing, there should be nothing in
    // the queue

  }

  test("Queue and a transacted send") {
    connect("1.1")

    def put(id: Int, tx: String = null) = {
      client.write(
        "SEND\n" +
                "destination:/queue/transacted\n" + {
          if (tx != null) {
            "transaction:" + tx + "\n"
          } else {
            ""
          }
        } +
                "\n" +
                "message:" + id + "\n")
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

    def get(id: Int) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith regex ("\n\nmessage:" + id + "\n")
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

    def put(id: Int, tx: String = null) = {
      client.write(
        "SEND\n" +
                "destination:/topic/transacted\n" + {
          if (tx != null) {
            "transaction:" + tx + "\n"
          } else {
            ""
          }
        } +
                "\n" +
                "message:" + id + "\n")
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

    def get(id: Int) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith regex ("\n\nmessage:" + id + "\n")
    }

    get(1)
    get(3)

    client.write(
      "COMMIT\n" +
              "transaction:x\n" +
              "\n")

    get(2)

  }

  test("ack:client redelivers on client disconnect") {
    connect("1.1")

    async_send("/queue/ackmode-client", 1)
    async_send("/queue/ackmode-client", 2)
    async_send("/queue/ackmode-client", 3)

    subscribe("0", "/queue/ackmode-client", "client")

    val ack1 = assert_received(1, "0")
    val ack2 = assert_received(2, "0")
    val ack3 = assert_received(3, "0")

    ack2(true)
    disconnect()

    connect("1.1")
    subscribe("0", "/queue/ackmode-client", "client")
    assert_received(3, "0")
  }


  test("ack:client-individual redelivers on client disconnect") {
    connect("1.1")

    def put(id: Int) = {
      client.write(
        "SEND\n" +
                "destination:/queue/ackmode-message\n" +
                "\n" +
                "message:" + id + "\n")
    }
    put(1)
    put(2)
    put(3)

    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/ackmode-message\n" +
              "ack:client-individual\n" +
              "id:0\n" +
              "\n")

    def get(id: Int) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should include("subscription:0\n")
      frame should include regex ("message-id:.+?\n")
      frame should endWith regex ("\n\nmessage:" + id + "\n")

      val p = """(?s).*?\nmessage-id:(.+?)\n.*""".r
      frame match {
        case p(x) => x
        case _ => null
      }
    }

    get(1)
    val mid = get(2)
    get(3)

    // Ack the first 2 messages..
    client.write(
      "ACK\n" +
              "subscription:0\n" +
              "message-id:" + mid + "\n" +
              "receipt:0\n" +
              "\n")

    wait_for_receipt("0")
    client.close

    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/ackmode-message\n" +
              "ack:client-individual\n" +
              "id:0\n" +
              "\n")
    get(1)
    get(3)

  }

  test("Temp Queue Send Receive") {
    connect("1.1")

    sync_send("/temp-queue/test", "1", "reply-to:/temp-queue/test\n")
    subscribe("my-sub-name", "/temp-queue/test")

    def get(dest: String) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith("\n\n%s".format(dest))

      // extract headers as a map of values.
      Map((frame.split("\n").reverse.flatMap {
        line =>
          if (line.contains(":")) {
            val parts = line.split(":", 2)
            Some((parts(0), parts(1)))
          } else {
            None
          }
      }): _*)
    }

    // The destination and reply-to headers should get updated with actual
    // Queue names
    val message = get("1")
    val actual_temp_dest_name = message.get("destination").get
    actual_temp_dest_name should startWith("/queue/temp.default.")
    message.get("reply-to") should be === (message.get("destination"))

    // Different connection should be able to send a message to the temp destination..
    var other = connect("1.1", new StompClient)
    sync_send(actual_temp_dest_name, "2", c=other)

    // First client should get the message.
    assert_received("2", "my-sub-name")

    // But not consume from it.
    other.write(
      "SUBSCRIBE\n" +
      "destination:" + actual_temp_dest_name + "\n" +
      "id:1\n" +
      "receipt:0\n" +
      "\n")

    val frame = other.receive()
    frame should startWith("ERROR\n")
    frame should include regex ("""message:Not authorized to receive from the temporary destination""")
    other.close()

    // Check that temp queue is deleted once the client disconnects
    async_send("/temp-queue/test", "3", "reply-to:/temp-queue/test\n")
    expect(true)(queue_exists(actual_temp_dest_name.stripPrefix("/queue/")))
    client.close();

    within(10, SECONDS) {
      expect(false)(queue_exists(actual_temp_dest_name.stripPrefix("/queue/")))
    }
  }

  test("Temp Topic Send Receive") {
    connect("1.1")

    subscribe("my-sub-name", "/temp-topic/test")

    def get(dest: String) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith("\n\n%s".format(dest))

      // extract headers as a map of values.
      Map((frame.split("\n").reverse.flatMap {
        line =>
          if (line.contains(":")) {
            val parts = line.split(":", 2)
            Some((parts(0), parts(1)))
          } else {
            None
          }
      }): _*)
    }

    async_send("/temp-topic/test", "1", "reply-to:/temp-topic/test\n")

    // The destination and reply-to headers should get updated with actual
    // Queue names
    val message = get("1")
    val actual_temp_dest_name = message.get("destination").get
    actual_temp_dest_name should startWith("/topic/temp.default.")
    message.get("reply-to") should be === (message.get("destination"))

    // Different connection should be able to send a message to the temp destination..
    var other = connect("1.1", new StompClient)
    sync_send(actual_temp_dest_name, "2", c=other)

    // First client chould get the message.
    assert_received("2")

    // But not consume from it.
    other.write(
      "SUBSCRIBE\n" +
              "destination:" + actual_temp_dest_name + "\n" +
              "id:1\n" +
              "receipt:0\n" +
              "\n")
    val frame = other.receive()
    frame should startWith("ERROR\n")
    frame should include regex ("""message:Not authorized to receive from the temporary destination""")
    other.close()

    // Check that temp queue is deleted once the client disconnects
    async_send("/temp-topic/test", "3", "reply-to:/temp-topic/test\n")
    expect(true)(topic_exists(actual_temp_dest_name.stripPrefix("/topic/")))
    client.close();

    within(10, SECONDS) {
      expect(false)(topic_exists(actual_temp_dest_name.stripPrefix("/topic/")))
    }
  }

  test("Odd reply-to headers do not cause errors") {
    connect("1.1")

    client.write(
      "SEND\n" +
              "destination:/queue/oddrepyto\n" +
              "reply-to:sms:8139993334444\n" +
              "receipt:0\n" +
              "\n")
    wait_for_receipt("0")

    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/oddrepyto\n" +
              "id:1\n" +
              "\n")

    val frame = client.receive()
    frame should startWith("MESSAGE\n")
    frame should include("reply-to:sms:8139993334444\n")
  }

  test("NACKing moves messages to DLQ (non-persistent)") {
    connect("1.1")
    sync_send("/queue/nacker.a", "this msg is not persistent")

    subscribe("0", "/queue/nacker.a", "client", false, "", false)
    subscribe("dlq", "/queue/dlq.nacker.a", "auto", false, "", false)
    var ack = assert_received("this msg is not persistent", "0")
    ack(false)
    ack = assert_received("this msg is not persistent", "0")
    ack(false)

    // It should be sent to the DLQ after the 2nd nak
    assert_received("this msg is not persistent", "dlq")
  }

  test("NACKing moves messages to DLQ (persistent)") {
    connect("1.1")
    sync_send("/queue/nacker.b", "this msg is persistent", "persistent:true\n")

    subscribe("0", "/queue/nacker.b", "client", false, "", false)
    subscribe("dlq", "/queue/dlq.nacker.b", "auto", false, "", false)
    var ack = assert_received("this msg is persistent", "0")
    ack(false)
    ack = assert_received("this msg is persistent", "0")
    ack(false)

    // It should be sent to the DLQ after the 2nd nak
    assert_received("this msg is persistent", "dlq")
  }

  test("NACKing without DLQ consumer (persistent)") {
    connect("1.1")
    sync_send("/queue/nacker.c", "this msg is persistent", "persistent:true\n")

    subscribe("0", "/queue/nacker.c", "client", false, "", false)

    var ack = assert_received("this msg is persistent", "0")
    ack(false)
    ack = assert_received("this msg is persistent", "0")
    ack(false)
    Thread.sleep(1000)
  }

  test("Sending as a Telnet client"){
    client.open("localhost", port)

    client.out.write("CONNECT\r\n".getBytes)
    client.out.write("accept-version:1.2\r\n".getBytes)
    client.out.write("login:admin\r\n".getBytes)
    client.out.write("passcode:password\r\n".getBytes)
    client.out.write("\r\n".getBytes)
    client.out.write("\u0000\r\n".getBytes)
    client.out.flush

    val frame = client.receive()
    frame should startWith("CONNECTED\n")
    frame should include regex ("""session:.+?\n""")

    client.out.write((
      "SUBSCRIBE\r\n" +
      "id:0\r\n" +
      "destination:/queue/somedest\r\n" +
      "receipt:0\r\n" +
      "\r\n"+
      "\u0000\r\n"
    ).getBytes)

    client.out.flush
    wait_for_receipt("0")

  }


  test("STOMP 1.2 client ack.") {
    val dest = next_id("/queue/stomp12clientack_")

    connect("1.2")
    subscribe("my-sub-name", dest, "client")
    async_send(dest, 1)
    async_send(dest, 2)
    assert_received(1, "my-sub-name")
    assert_received(2, "my-sub-name")(true)
    disconnect()

    connect("1.2")
    subscribe("my-sub-name", dest, "client")
    async_send(dest, 3)
    assert_received(3, "my-sub-name")(true)
    disconnect()
  }

  test("STOMP 1.2 client-individual ack.") {
    val dest = next_id("/queue/stomp12clientack_")

    connect("1.2")
    subscribe("my-sub-name", dest, "client-individual")
    async_send(dest, 1)
    async_send(dest, 2)
    assert_received(1, "my-sub-name")
    assert_received(2, "my-sub-name")(true)
    disconnect()

    connect("1.2")
    subscribe("my-sub-name", dest, "client")
    async_send(dest, 3)
    assert_received(1, "my-sub-name")(true)
    assert_received(3, "my-sub-name")(true)
    disconnect()
  }

  for ( prefix<- List("queued", "block")) {
    test("APLO-249: Message expiration does not (always) work on topics: "+prefix) {
      val dest = next_id("/topic/"+prefix+".expiration")
      val msg_count = 1000

      connect("1.1")
      subscribe("0", dest, "client")

      Broker.BLOCKABLE_THREAD_POOL {
        val sender = new StompClient
        connect("1.1", sender)
        val exp = System.currentTimeMillis()+500
        for( i <- 1 to msg_count ) {
          async_send(dest, "%01024d".format(i), "expires:"+exp+"\n", sender)
        }
        sync_send(dest, "DONE", c=sender)
        close(sender)
      }

      var done = false
      var received = 0
      Thread.sleep(1000)
      while( !done ) {
        val (frame, ack) = receive_message()
        val body = frame.substring(frame.indexOf("\n\n")+2)
        if( body == "DONE" ) {
          done = true
        } else {
          received +=1
          ack(true)
        }
      }

      val expired = (msg_count-received)
      info("expired: "+expired)
      expired should not be(0)

    }

  }


  test("STOMP flow control.") {
    val dest = next_id("/queue/quota.flow_control")

    // start a sub client /w a small credit window.
    val sub = connect("1.1")
    subscribe("my-sub-name", dest, "client", headers="credit:1,0\n")

    val sent = new AtomicInteger(0)
    val body = "x"*1024*10
    Broker.BLOCKABLE_THREAD_POOL {
      for( i <- 1 to 10 ) {
        info("sending: "+i)
        val client = connect("1.1", new StompClient)
        async_send(dest, body, c=client)
        disconnect(client)
        sent.incrementAndGet()
      }
    }

    for( i <- 1 to 10 ) {
      within(1, SECONDS) {
        sent.get should be(i)
      }
      assert_received(body)(true)
      Thread.sleep(200)
    }

  }

  for( kind <- Array("/queue/", "/topic/", "/topic/queued.")) {
    test("Transaction commit order on "+kind) {

      val dest = next_id(kind+"send_transaction-")

      val receiver = connect("1.1", new StompClient)
      subscribe("mysub",dest,c=receiver)

      connect("1.1")
      async_send(dest, "m1")

      val tx = begin()
      async_send(dest, "t1", "transaction:"+tx+"\n")
      async_send(dest, "m2")
      async_send(dest, "t2", "transaction:"+tx+"\n")
      commit(tx)

      async_send(dest, "m3")

      assert_received("m1",c=receiver)
      assert_received("m2",c=receiver)
      assert_received("t1",c=receiver)
      assert_received("t2",c=receiver)
      assert_received("m3",c=receiver)
    }
  }

  for( kind <- Array("/queue/", "/topic/", "/topic/queued.")) {
    test("Transaction commit acks on "+kind) {

      val dest = next_id(kind+"tx-commit-acks-")

      val receiver = connect("1.1", new StompClient)
      subscribe("mysub",dest,mode="client",c=receiver)

      connect("1.1")

      async_send(dest, "m1")
      async_send(dest, "m2")
      async_send(dest, "m3")
      async_send(dest, "m4")

      val tx = begin(c=receiver)
      assert_received("m1",c=receiver)(true)
      assert_received("m2",c=receiver, txid=tx)(true)
      assert_received("m3",c=receiver)(true)
      assert_received("m4",c=receiver, txid=tx)(true)
      commit(tx, c=receiver)

      async_send(dest, "m5")
      assert_received("m5",c=receiver)
    }
  }

  for( kind <- Array("/queue/", "/topic/", "/topic/queued.")) {
    test("Transaction abort acks on "+kind) {

      val dest = next_id(kind+"tx-abort-acks-")

      val receiver = connect("1.1", new StompClient)
      subscribe("mysub",dest,mode="client",c=receiver)

      connect("1.1")

      async_send(dest, "m1")
      async_send(dest, "m2")
      async_send(dest, "m3")
      async_send(dest, "m4")

      val tx = begin(c=receiver)
      assert_received("m1",c=receiver)(true)
      assert_received("m2",c=receiver, txid=tx)(true)
      assert_received("m3",c=receiver)(true)
      assert_received("m4",c=receiver, txid=tx)(true)
      abort(tx, c=receiver)

      // aborting a tx does not cause a redelivery to occur.
      async_send(dest, "m5")
      assert_received("m5",c=receiver)
    }
  }


  for( kind <- Array("/queue/", "/topic/", "/topic/queued.")) {
    test("Sending already expired message to "+kind) {

      val dest = next_id(kind+"expired-")

      val receiver = connect("1.1", new StompClient)
      subscribe("mysub",dest,c=receiver)

      connect("1.1")

      async_send(dest, "m1", "persistent:true\n")
      val exp = System.currentTimeMillis()-1000
      async_send(dest, "e1", "persistent:true\nexpires:"+exp+"\n")
      async_send(dest, "m2", "persistent:true\n")

      assert_received("m1",c=receiver)
      assert_received("m2",c=receiver)
    }
  }

  test("APLO-315: Evil producers that don't read thier sockets for receipts") {
    val pending = new ConcurrentHashMap[String, java.lang.Long]()
    val start = System.currentTimeMillis();
    val producer_counter = new AtomicLong(0);

    var producer_done = new AtomicBoolean(false)
    var producer_shutdown = new CountDownLatch(15*2);

    val client = connect("1.1", new StompClient());

    // use one thread to send..
    val producer = new BlockingTask() {
      def run() {
        var i = 0;
        while(!done.get()) {
          val receipt_id = "-"+i;
          i += 1
          pending.put(receipt_id, new java.lang.Long(System.nanoTime()));
          async_send("/topic/APLO-315", "This is message "+i, headers="receipt:"+receipt_id+"\npersistent:true\n", c=client);
          producer_counter.incrementAndGet();
        }
      }
    }

    // The producer should block since he's not draining his receipts..
    within(10, SECONDS) {
      val start = producer_counter.get()
      Thread.sleep(1000)
      producer_counter.get() should be (start)
    }

    producer.stop

    // Start draining those receipts..
    val drainer = new BlockingTask() {
      def run() {
        var i = 0;
        while(!pending.isEmpty) {
          if( done.get() ) {
            return;
          }
          try {
            var r = wait_for_receipt(c = client, timeout = 500);
            if (r != null) {
              val start = pending.remove(r);
              if (start == null) {
                fail("Got unexpected receipt: " + r)
              }
              val latency = System.nanoTime() - start.longValue();
            }
          } catch {
            case e:SocketTimeoutException =>
          }
        }
      }
    }

    producer.await
    drainer.await
  }

  test("APLO-349: Empty STOMP Header Name") {
    connect("1.1")
    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/APLO-349\n" +
              "id:0\n" +
              ":invalid header\n" +
              "\n")

    var frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include("message:Unable to parser header line")
  }
}
