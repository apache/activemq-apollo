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
import java.util.concurrent.TimeUnit._
import org.apache.activemq.apollo.util._
import java.util.concurrent.atomic.AtomicLong
import FileSupport._
import java.nio.channels.DatagramChannel
import org.fusesource.hawtbuf.AsciiBuffer
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.dto.{TopicStatusDTO, KeyStorageDTO}
import java.net.{SocketTimeoutException, InetSocketAddress}

class StompTestSupport extends BrokerFunSuiteSupport with ShouldMatchers with BeforeAndAfterEach {

  override def broker_config_uri = "xml:classpath:apollo-stomp.xml"

  var client = new StompClient
  var clients = List[StompClient]()

  override protected def afterEach() = {
    super.afterEach
    clients.foreach(_.close)
    clients = Nil
  }

  def connect_request(version:String, c: StompClient, headers:String="", connector:String=null) = {
    val p = connector_port(connector).getOrElse(port)
    c.open("localhost", p)
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

  def connect(version:String, c: StompClient = client, headers:String="", connector:String=null) = {
    val frame = connect_request(version, c, headers, connector)
    frame should startWith("CONNECTED\n")
    frame should include regex("""session:.+?\n""")
    frame should include("version:"+version+"\n")
    c
  }

  def disconnect(c: StompClient = client) = {
    val rid = receipt_counter.incrementAndGet()
    client.write(
      "DISCONNECT\n" +
      "receipt:"+rid+"\n" +
      "\n")
    wait_for_receipt(""+rid, c)
    close(c)
  }

  def close(c: StompClient = client) = c.close()

  val receipt_counter = new AtomicLong()

  def sync_send(dest:String, body:Any, headers:String="", c:StompClient = client) = {
    val rid = receipt_counter.incrementAndGet()
    c.write(
      "SEND\n" +
      "destination:"+dest+"\n" +
      "receipt:"+rid+"\n" +
      headers+
      "\n" +
      body)
    wait_for_receipt(""+rid, c)
  }

  def async_send(dest:String, body:Any, headers:String="", c: StompClient = client) = {
    c.write(
      "SEND\n" +
      "destination:"+dest+"\n" +
      headers+
      "\n" +
      body)
  }

  def subscribe(id:String, dest:String, mode:String="auto", persistent:Boolean=false, headers:String="", sync:Boolean=true, c: StompClient = client) = {
    val rid = receipt_counter.incrementAndGet()
    c.write(
      "SUBSCRIBE\n" +
      "destination:"+dest+"\n" +
      "id:"+id+"\n" +
      (if(persistent) "persistent:true\n" else "")+
      "ack:"+mode+"\n"+
      (if(sync) "receipt:"+rid+"\n" else "") +
      headers+
      "\n")
    if(sync) {
      wait_for_receipt(""+rid, c)
    }
  }

  def unsubscribe(id:String, headers:String="", c: StompClient=client) = {
    val rid = receipt_counter.incrementAndGet()
    c.write(
      "UNSUBSCRIBE\n" +
      "id:"+id+"\n" +
      "receipt:"+rid+"\n" +
      headers+
      "\n")
    wait_for_receipt(""+rid, c)
  }

  def assert_received(body:Any, sub:String=null, c: StompClient=client, txid:String = null):(Boolean)=>Unit = {
    val frame = c.receive()
    frame should startWith("MESSAGE\n")
    if(sub!=null) {
      frame should include ("subscription:"+sub+"\n")
    }
    body match {
      case null =>
      case body:scala.util.matching.Regex => frame should endWith regex(body)
      case body => frame should endWith("\n\n"+body)
    }
    // return a func that can ack the message.
    (ack:Boolean)=> {
      val sub_regex = """(?s).*\nsubscription:([^\n]+)\n.*""".r
      val msgid_regex = """(?s).*\nmessage-id:([^\n]+)\n.*""".r
      val sub_regex(sub) = frame
      val msgid_regex(msgid) = frame
      c.write(
        (if(ack) "ACK\n" else "NACK\n") +
        "subscription:"+sub+"\n" +
        "message-id:"+msgid+"\n" +
        (if(txid != null) "transaction:"+txid+"\n" else "") +

          "\n")
    }
  }

  def wait_for_receipt(id:String, c: StompClient = client, discard_others:Boolean=false): Unit = {
    if( !discard_others ) {
      val frame = c.receive()
      frame should startWith("RECEIPT\n")
      frame should include("receipt-id:"+id+"\n")
    } else {
      while(true) {
        val frame = c.receive()
        if( frame.startsWith("RECEIPT\n") && frame.indexOf("receipt-id:"+id+"\n")>=0 ) {
          return
        }
      }
    }
  }
}

/**
 * These test cases check to make sure the broker stats are consistent with what
 * would be expected.
 */
class StompMetricsTest extends StompTestSupport {

  test("slow_consumer_policy='queue' metrics stay consistent on consumer close (APLO-211)") {
    connect("1.1")

    subscribe("0", "/topic/queued.APLO-211", "client");
    async_send("/topic/queued.APLO-211", 1)
    assert_received(1)(true)

    val stat1 = topic_status("queued.APLO-211").metrics
    disconnect()

    within(3, SECONDS) {
      val stat2 = topic_status("queued.APLO-211").metrics
      stat2.producer_count should be(stat1.producer_count-1)
      stat2.consumer_count should be(stat1.consumer_count-1)
      stat2.enqueue_item_counter should be(stat1.enqueue_item_counter)
      stat2.dequeue_item_counter should be(stat1.dequeue_item_counter)
    }
  }


  test("Deleted qeueus are removed to aggregate queue-stats") {
    connect("1.1")

    val stat1 = get_queue_metrics

    async_send("/queue/willdelete", 1)
    async_send("/queue/willdelete", 2)
    sync_send("/queue/willdelete", 3)

    // not acked yet.
    val stat2 = get_queue_metrics
    stat2.producer_count should be(stat1.producer_count+1)
    stat2.consumer_count should be(stat1.consumer_count)
    stat2.enqueue_item_counter should be(stat1.enqueue_item_counter+3)
    stat2.dequeue_item_counter should be(stat1.dequeue_item_counter+0)
    stat2.queue_items should be(stat1.queue_items+3)

    // Delete the queue
    delete_queue("willdelete")

    within(1, SECONDS) {
      val stat3 = get_queue_metrics
      stat3.producer_count should be(stat1.producer_count)
      stat3.consumer_count should be(stat1.consumer_count)
      stat3.enqueue_item_counter should be(stat1.enqueue_item_counter+3)
      stat3.dequeue_item_counter should be(stat1.dequeue_item_counter)
      stat3.queue_items should be(stat1.queue_items)
    }
  }

  test("Old consumers on topic slow_consumer_policy='queue' does not affect the agregate queue-metrics") {
    connect("1.1")

    subscribe("0", "/topic/queued.test1", "client");
    sync_send("/topic/queued.test1", 1)

    val stat1 = get_topic_metrics

    async_send("/topic/queued.test1", 2)
    async_send("/topic/queued.test1", 3)
    val ack1 = assert_received(1)
    val ack2 = assert_received(2)
    val ack3 = assert_received(3)

    // not acked yet.
    val stat2 = get_topic_metrics
    stat2.producer_count should be(stat1.producer_count)
    stat2.consumer_count should be(stat1.consumer_count)
    stat2.enqueue_item_counter should be(stat1.enqueue_item_counter+2)
    stat2.dequeue_item_counter should be(stat1.dequeue_item_counter+0)
    stat2.queue_items should be(stat1.queue_items+2)

    // Close the subscription.
    unsubscribe("0")

    within(1, SECONDS) {
      val stat3 = get_topic_metrics
      stat3.producer_count should be(stat1.producer_count)
      stat3.consumer_count should be(stat1.consumer_count-1)
      stat3.enqueue_item_counter should be(stat1.enqueue_item_counter+2)
      stat3.dequeue_item_counter should be(stat1.dequeue_item_counter+0)
      stat3.queue_items should be(stat1.queue_items-1)
    }
  }

  test("New Topic Stats") {
    connect("1.1")
    subscribe("0", "/topic/newstats");
    val stats = topic_status("newstats")
    var now = System.currentTimeMillis()
    (now-stats.metrics.enqueue_ts) should ( be < 10*1000L)
    (now-stats.metrics.dequeue_ts) should ( be < 10*1000L)
  }

  test("Topic Stats") {
    connect("1.1")

    sync_send("/topic/stats", 1)
    val stat1 = topic_status("stats")
    stat1.producers.size() should be(1)
    stat1.consumers.size() should be(0)
    stat1.dsubs.size() should be(0)
    stat1.metrics.enqueue_item_counter should be(1)
    stat1.metrics.dequeue_item_counter should be(0)
    stat1.metrics.queue_items should be(0)

    subscribe("0", "/topic/stats");
    async_send("/topic/stats", 2)
    async_send("/topic/stats", 3)
    assert_received(2)
    assert_received(3)

    val stat2 = topic_status("stats")
    stat2.producers.size() should be(1)
    stat2.consumers.size() should be(1)
    stat2.dsubs.size() should be(0)
    stat2.metrics.enqueue_item_counter should be(3)
    stat2.metrics.dequeue_item_counter should be(2)
    stat2.metrics.queue_items should be(0)
    client.close()

    within(1, SECONDS) {
      val stat3 = topic_status("stats")
      stat3.producers.size() should be(0)
      stat3.consumers.size() should be(0)
      stat3.dsubs.size() should be(0)
      stat3.metrics.enqueue_item_counter should be(3)
      stat3.metrics.dequeue_item_counter should be(2)
      stat3.metrics.queue_items should be(0)
    }
  }

  test("Topic slow_consumer_policy='queue' Stats") {
    connect("1.1")

    sync_send("/topic/queued.stats", 1)
    val stat1 = topic_status("queued.stats")
    stat1.producers.size() should be(1)
    stat1.consumers.size() should be(0)
    stat1.dsubs.size() should be(0)
    stat1.metrics.enqueue_item_counter should be(1)
    stat1.metrics.dequeue_item_counter should be(0)
    stat1.metrics.queue_items should be(0)

    subscribe("0", "/topic/queued.stats", "client");
    async_send("/topic/queued.stats", 2)
    async_send("/topic/queued.stats", 3)
    val ack2 = assert_received(2)
    val ack3 = assert_received(3)

    // not acked yet.
    val stat2 = topic_status("queued.stats")
    stat2.producers.size() should be(1)
    stat2.consumers.size() should be(1)
    stat2.dsubs.size() should be(0)
    stat2.metrics.enqueue_item_counter should be(3)
    stat2.metrics.dequeue_item_counter should be(0)
    stat2.metrics.queue_items should be(2)

    // Ack now..
    ack2(true) ; ack3(true)

    within(1, SECONDS) {
      val stat3 = topic_status("queued.stats")
      stat3.producers.size() should be(1)
      stat3.consumers.size() should be(1)
      stat3.dsubs.size() should be(0)
      stat3.metrics.enqueue_item_counter should be(3)
      stat3.metrics.dequeue_item_counter should be(2)
      stat3.metrics.queue_items should be(0)
    }

    unsubscribe("0")
    client.close()
    within(1, SECONDS) {
      val stat4 = topic_status("queued.stats")
      stat4.producers.size() should be(0)
      stat4.consumers.size() should be(0)
      stat4.dsubs.size() should be(0)
      stat4.metrics.enqueue_item_counter should be(3)
      stat4.metrics.dequeue_item_counter should be(2)
      stat4.metrics.queue_items should be(0)
    }
  }

  test("Topic Durable Sub Stats.") {
    connect("1.1")

    sync_send("/topic/dsubed.stats", 1)
    val stat1 = topic_status("dsubed.stats")
    stat1.producers.size() should be(1)
    stat1.consumers.size() should be(0)
    stat1.dsubs.size() should be(0)
    stat1.metrics.enqueue_item_counter should be(1)
    stat1.metrics.dequeue_item_counter should be(0)
    stat1.metrics.queue_items should be(0)
    
    subscribe("dsub1", "/topic/dsubed.stats", "client", true);
    async_send("/topic/dsubed.stats", 2)
    async_send("/topic/dsubed.stats", 3)
    val ack2 = assert_received(2)
    val ack3 = assert_received(3)

    // not acked yet.
    val stat2 = topic_status("dsubed.stats")
    stat2.producers.size() should be(1)
    stat2.consumers.size() should be(1)
    stat2.dsubs.size() should be(1)
    stat2.metrics.enqueue_item_counter should be(3)
    stat2.metrics.dequeue_item_counter should be(0)
    stat2.metrics.queue_items should be(2)

    // Ack SOME now..
    ack2(true);

    within(1, SECONDS) {
      val stat3 = topic_status("dsubed.stats")
      stat3.producers.size() should be(1)
      stat3.consumers.size() should be(1)
      stat3.dsubs.size() should be(1)
      stat3.metrics.enqueue_item_counter should be(3)
      stat3.metrics.dequeue_item_counter should be(1)
      stat3.metrics.queue_items should be(1)
    }

    unsubscribe("dsub1")
    client.close()
    within(1, SECONDS) {
      val stat4 = topic_status("dsubed.stats")
      stat4.producers.size() should be(0)
      stat4.consumers.size() should be(1)
      stat4.dsubs.size() should be(1)
      stat4.metrics.enqueue_item_counter should be(3)
      stat4.metrics.dequeue_item_counter should be(1)
      stat4.metrics.queue_items should be(1)
    }
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

class StompPersistentQueueTest extends StompTestSupport {

  override def broker_config_uri: String = "xml:classpath:apollo-stomp-leveldb.xml"

  test("(APLO-198) Apollo sometimes does not send all the messages in a queue") {
    connect("1.1")
    for( i <- 0 until 10000 ) {
      async_send("/queue/BIGQUEUE", "message #"+i)
    }
    sync_send("/queue/BIGQUEUE", "END")
    client.close
    
    var counter = 0
    for( i <- 0 until 100 ) {
      connect("1.1")
      subscribe("1", "/queue/BIGQUEUE", "client", false, "", false)
      for( j <- 0 until 100 ) {
        assert_received("message #"+counter)(true)
        counter+=1
      }
      client.write(
        "DISCONNECT\n" +
        "receipt:disco\n" +
        "\n")
      wait_for_receipt("disco", client, true)
      client.close
      within(2, SECONDS) {
        val status = queue_status("BIGQUEUE")
        status.consumers.size() should be(0)
      }
    }

    connect("1.1")
    subscribe("1", "/queue/BIGQUEUE", "client")
    assert_received("END")(true)

  }

}

/**
 * These disconnect tests assure that we don't drop message deliviers that are in flight
 * if a client disconnects before those deliveries are accepted by the target destination.
 */
class StompDisconnectTest extends StompTestSupport {

  test("Messages delivery assured to a queued once a disconnect receipt is received") {

    // figure out at what point a quota'ed queue stops accepting more messages.
    connect("1.1")
    client.socket.setSoTimeout(1*1000)
    var block_count = 0
    try {
      while( true ) {
        sync_send("/queue/quota.assured1", "%01024d".format(block_count))
        block_count += 1
      }
    } catch{
      case e:SocketTimeoutException =>
    }
    close()

    // Send 5 more messages which do not fit in the queue, they will be
    // held in the producer connection's delivery session buffer..
    connect("1.1")
    for(i <- 0 until (block_count+5)) {
      async_send("/queue/quota.assured2", "%01024d".format(i))
    }

    // Even though we disconnect, those 5 that did not fit should still
    // get delivered once the queue unblocks..
    disconnect()

    // Lets make sure non of the messages were dropped.
    connect("1.1")
    subscribe("0", "/queue/quota.assured2")
    for(i <- 0 until (block_count+5)) {
      assert_received("%01024d".format(i))
    }

  }

  test("Messages delivery assured to a topic once a disconnect receipt is received") {

    //setup a subscription which will block quickly..
    var consumer = new StompClient
    connect("1.1", consumer)
    subscribe("0", "/topic/quota.assured1", "client", headers="credit:1,0\n", c=consumer)

    // figure out at what point a quota'ed consumer stops accepting more messages.
    connect("1.1")
    client.socket.setSoTimeout(1*1000)
    var block_count = 0
    try {
      while( true ) {
        sync_send("/topic/quota.assured1", "%01024d".format(block_count))
        block_count += 1
      }
    } catch{
      case e:SocketTimeoutException =>
    }
    close()
    close(consumer)

    connect("1.1", consumer)
    subscribe("0", "/topic/quota.assured2", "client", headers="credit:1,0\n", c=consumer)

    // Send 5 more messages which do not fit in the consumer buffer, they will be
    // held in the producer connection's delivery session buffer..
    connect("1.1")
    for(i <- 0 until (block_count+5)) {
      async_send("/topic/quota.assured2", "%01024d".format(i))
    }

    // Even though we disconnect, those 5 that did not fit should still
    // get delivered once the queue unblocks..
    disconnect()

    // Lets make sure non of the messages were dropped.
    for(i <- 0 until (block_count+5)) {
      assert_received("%01024d".format(i), c=consumer)(true)
    }

  }
}

class StompDestinationTest extends StompTestSupport {

  test("APLO-206 - Load balance of job queues using small consumer credit windows") {
    connect("1.1")

    for( i <- 1 to 4) {
      async_send("/queue/load-balanced2", i)
    }

    subscribe("1", "/queue/load-balanced2", "client", false, "credit:1,0\n")
    val ack1 = assert_received(1, "1")

    subscribe("2", "/queue/load-balanced2", "client", false, "credit:1,0\n")
    val ack2 = assert_received(2, "2")

    // Ok lets ack now..
    ack1(true)
    val ack3 = assert_received(3, "1")

    ack2(true)
    val ack4 = assert_received(4, "2")
  }

  test("Browsing queues does not cause AssertionError.  Reported in APLO-156") {
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

  // This is the test case for https://issues.apache.org/jira/browse/APLO-88
  test("ACK then socket close with/without DISCONNECT, should still ACK") {
    for(i <- 1 until 3) {
      connect("1.1")

      def send(id:Int) = {
        client.write(
          "SEND\n" +
          "destination:/queue/from-seq-end\n" +
          "message-id:id-"+i+"-"+id+"\n"+
          "receipt:0\n"+
          "\n")
        wait_for_receipt("0")
      }

      def get(seq:Long) = {
        val frame = client.receive()
        frame should startWith("MESSAGE\n")
        frame should include("message-id:id-"+i+"-"+seq+"\n")
        client.write(
          "ACK\n" +
          "subscription:0\n" +
          "message-id:id-"+i+"-"+seq+"\n" +
          "\n")
      }

      send(1)
      send(2)

      client.write(
        "SUBSCRIBE\n" +
        "destination:/queue/from-seq-end\n" +
        "id:0\n" +
        "ack:client\n"+
        "\n")
      get(1)
      client.write(
        "DISCONNECT\n" +
        "\n")
      client.close

      connect("1.1")
      client.write(
        "SUBSCRIBE\n" +
        "destination:/queue/from-seq-end\n" +
        "id:0\n" +
        "ack:client\n"+
        "\n")
      get(2)
      client.close
    }
  }

  test("Setting `from-seq` header to -1 results in subscription starting at end of the queue.") {
    connect("1.1")

    def send(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/queue/from-seq-end\n" +
        "receipt:0\n"+
        "\n" +
        "message:"+id+"\n")
      wait_for_receipt("0")
    }

    send(1)
    send(2)
    send(3)

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/from-seq-end\n" +
      "receipt:0\n"+
      "browser:true\n"+
      "browser-end:false\n"+
      "id:0\n" +
      "from-seq:-1\n"+
      "\n")
    wait_for_receipt("0")

    send(4)

    def get(seq:Long) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should include("message:"+seq+"\n")
    }
    get(4)
  }

  test("The `browser-end:false` can be used to continously browse a queue.") {
    connect("1.1")
    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/browsing-continous\n" +
      "browser:true\n"+
      "browser-end:false\n"+
      "receipt:0\n"+
      "id:0\n" +
      "\n")
    wait_for_receipt("0")

    def send(id:Int) = client.write(
      "SEND\n" +
      "destination:/queue/browsing-continous\n" +
      "\n" +
      "message:"+id+"\n")

    send(1)
    send(2)

    def get(seq:Long) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      expect(true)(frame.contains("message:"+seq+"\n"))
    }
    get(1)
    get(2)
  }

  test("Message sequence headers are added when `include-seq` is used.") {
    connect("1.1")
    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/seq_queue\n" +
      "receipt:0\n"+
      "id:0\n" +
      "include-seq:seq\n"+
      "\n")
    wait_for_receipt("0")

    def send(id:Int) = client.write(
      "SEND\n" +
      "destination:/queue/seq_queue\n" +
      "\n" +
      "message:"+id+"\n")

    send(1)
    send(2)

    def get(seq:Long) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      expect(true)(frame.contains("seq:"+seq+"\n"))
    }
    get(1)
    get(2)
  }

  test("The `from-seq` header can be used to resume delivery from a given point in a queue.") {
    connect("1.1")

    def send(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/queue/from_queue\n" +
        "receipt:0\n"+
        "\n" +
        "message:"+id+"\n")
      wait_for_receipt("0")
    }

    send(1)
    send(2)
    send(3)

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/from_queue\n" +
      "receipt:0\n"+
      "browser:true\n"+
      "id:0\n" +
      "include-seq:seq\n"+
      "from-seq:2\n"+
      "\n")
    wait_for_receipt("0")

    def get(seq:Long) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should include("seq:"+seq+"\n")
    }
    get(2)
    get(3)
  }


  test("The `from-seq` header is not supported with wildcard or composite destinations.") {
    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/some,/queue/other\n" +
      "browser:true\n"+
      "id:0\n" +
      "include-seq:seq\n"+
      "from-seq:2\n"+
      "\n")

    var frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include("message:The from-seq header is only supported when you subscribe to one destination")

    client.close
    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/some.*\n" +
      "browser:true\n"+
      "id:0\n" +
      "include-seq:seq\n"+
      "from-seq:2\n"+
      "\n")

    frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include("message:The from-seq header is only supported when you subscribe to one destination")
  }

  test("Selector Syntax") {
    connect("1.1")

    var sub_id=0;
    def test_selector(selector:String, headers: List[String], expected_matches: List[Int]) = {

      client.write(
        "SUBSCRIBE\n" +
        "destination:/topic/selected\n" +
        "selector:"+selector+"\n" +
        "receipt:0\n"+
        "id:"+sub_id+"\n" +
        "\n")
      wait_for_receipt("0")

      var id=1;

      headers.foreach { header=>
        client.write(
          "SEND\n" +
          "destination:/topic/selected\n" +
          header+"\n" +
          "\n" +
          "message:"+id+"\n")
        id += 1;
      }

      expected_matches.foreach{id=>
        val frame = client.receive()
        frame should startWith("MESSAGE\n")
        frame should endWith regex("\n\nmessage:"+id+"\n")
      }

      client.write(
        "UNSUBSCRIBE\n" +
        "id:"+sub_id+"\n" +
        "receipt:0\n"+
        "\n")

      wait_for_receipt("0")

      sub_id+=1
    }

    test_selector("color = 'red'", List("color:blue", "not:set", "color:red"), List(3))
    test_selector("hyphen-field = 'red'", List("hyphen-field:blue", "not:set", "hyphen-field:red"), List(3))
    test_selector("age >= 21", List("age:3", "not:set", "age:21", "age:30"), List(3,4))

  }

  test("Queues load balance across subscribers") {
    connect("1.1")
    subscribe("1", "/queue/load-balanced")
    subscribe("2", "/queue/load-balanced")

    for( i <- 0 until 4) {
      async_send("/queue/load-balanced", "message:"+i)
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
    frame should include ("\nmessage-id:")
    frame should include ("\ndestination:")

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

class DurableSubscriptionOnLevelDBTest extends StompTestSupport {

  override def broker_config_uri: String = "xml:classpath:apollo-stomp-leveldb.xml"

  test("Multiple dsubs contain the same messages (Test case for APLO-210)") {

    val sub_count = 3
    val message_count = 1000

    // establish 3 durable subs..
    connect("1.1")
    for( sub <- 1 to sub_count ) {
      subscribe(id="sub"+sub, dest="/topic/sometopic", persistent=true)
    }
    close()

    connect("1.1")

    val filler = ":"+("x"*(1024*10))

    // Now send a bunch of messages....
    for( i <- 1 to message_count ) {
      async_send(dest="/topic/sometopic", headers="persistent:true\n", body=i+filler)
    }

    // Empty out the durable durable sub
    for( sub <- 1 to sub_count ) {
      subscribe(id="sub"+sub, dest="/topic/sometopic", persistent=true, sync=false)
      for( i <- 1 to message_count ) {
        assert_received(body=i+filler, sub="sub"+sub)
      }
    }

  }

  test("Can directly send an recieve from a durable sub") {
    connect("1.1")

    // establish 2 durable subs..
    client.write(
      "SUBSCRIBE\n" +
      "destination:/topic/sometopic\n" +
      "id:sub1\n" +
      "persistent:true\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/topic/sometopic\n" +
      "id:sub2\n" +
      "persistent:true\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0")

    client.close
    connect("1.1")

    // Now send a bunch of messages....
    // Send only to sub 1
    client.write(
      "SEND\n" +
      "destination:/dsub/sub1\n" +
      "\n" +
      "sub1 msg\n")

    // Send to all subs
    client.write(
      "SEND\n" +
      "destination:/topic/sometopic\n" +
      "\n" +
      "LAST\n")


    // Now try to get all the previously sent messages.
    def get(expected:String) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith("\n\n"+expected)
    }

    // Empty out the first durable sub
    client.write(
      "SUBSCRIBE\n" +
      "destination:/dsub/sub1\n" +
      "id:1\n" +
      "\n")

    get("sub1 msg\n")
    get("LAST\n")

    // Empty out the 2nd durable sub
    client.write(
      "SUBSCRIBE\n" +
      "destination:/dsub/sub2\n" +
      "id:2\n" +
      "\n")

    get("LAST\n")
  }
  test("You can connect and then unsubscribe from existing durable sub (APLO-157)") {
    connect("1.1")
    subscribe("APLO-157", "/topic/APLO-157", "auto", true)
    client.close()

    // Make sure the durable sub exists.
    connect("1.1")
    sync_send("/topic/APLO-157", "1")
    subscribe("APLO-157", "/topic/APLO-157", "client", true)
    assert_received("1")
    client.close()

    // Delete the durable sub..
    connect("1.1")
    unsubscribe("APLO-157", "persistent:true\n")
    client.close()

    // Make sure the durable sub does not exists.
    connect("1.1")
    subscribe("APLO-157", "/topic/APLO-157", "client", true)
    async_send("/topic/APLO-157", "2")
    assert_received("2")
    unsubscribe("APLO-157", "persistent:true\n")

  }

  test("Can create dsubs with dots in them") {
    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/topic/sometopic\n" +
      "id:sub.1\n" +
      "persistent:true\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0")

    client.write(
      "SEND\n" +
      "destination:/dsub/sub.1\n" +
      "receipt:0\n" +
      "\n" +
      "content\n")
    wait_for_receipt("0")

  }

  test("Duplicate SUBSCRIBE updates durable subscription bindings") {
    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/topic/a\n" +
      "id:sub1\n" +
      "persistent:true\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0")

    def get(expected:String) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith("\n\n"+expected)
    }

    // Validate that the durable sub is bound to /topic/a
    client.write(
      "SEND\n" +
      "destination:/topic/a\n" +
      "\n" +
      "1\n")
    get("1\n")

    client.write(
      "UNSUBSCRIBE\n" +
      "id:sub1\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0")

    // Switch the durable sub to /topic/b
    client.write(
      "SUBSCRIBE\n" +
      "destination:/topic/b\n" +
      "id:sub1\n" +
      "persistent:true\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0")

    // all these should get dropped
    for ( i <- 1 to 500 ) {
      client.write(
        "SEND\n" +
        "destination:/topic/a\n" +
        "\n" +
        "DROPPED\n")
    }

    // Not this one.. it's on the updated topic
    client.write(
      "SEND\n" +
      "destination:/topic/b\n" +
      "\n" +
      "2\n")
    get("2\n")

  }

  test("Direct send to a non-existant a durable sub fails") {
    connect("1.1")

    client.write(
      "SEND\n" +
      "destination:/dsub/doesnotexist\n" +
      "receipt:0\n" +
      "\n" +
      "content\n")

    val frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include("message:The destination does not exist")
  }

  test("Direct subscribe to a non-existant a durable sub fails") {
    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/dsub/doesnotexist\n" +
      "id:1\n" +
      "receipt:0\n" +
      "\n")

    val frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include("message:Durable subscription does not exist")

  }
}

class DurableSubscriptionOnBDBTest extends DurableSubscriptionOnLevelDBTest {
  override def broker_config_uri: String = "xml:classpath:apollo-stomp-bdb.xml"
}

class StompMirroredQueueTest extends StompTestSupport {

  test("Topic gets copy of message sent to queue") {
    connect("1.1")
    subscribe("1", "/topic/mirrored.a")
    async_send("/queue/mirrored.a", "message:1\n")
    assert_received("message:1\n")
  }

  test("Queue gets copy of message sent to topic") {
    connect("1.1")

    // Connect to subscribers
    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/mirrored.b\n" +
      "id:1\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0")

    def put(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/topic/mirrored.b\n" +
        "\n" +
        "message:"+id+"\n")
    }

    put(1)

    def get(id:Int) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith regex("\n\nmessage:"+id+"\n")
    }
    get(1)

  }

  test("Queue does not get copies from topic until it's first created") {
    connect("1.1")

    def put(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/topic/mirrored.c\n" +
        "\n" +
        "message:"+id+"\n")
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

    def get(id:Int) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith regex("\n\nmessage:"+id+"\n")
    }
    get(2)
  }


}

class StompSslDestinationTest extends StompDestinationTest {
  override def broker_config_uri: String = "xml:classpath:apollo-stomp-ssl.xml"

  val config = new KeyStorageDTO
  config.file = basedir/"src"/"test"/"resources"/"client.ks"
  config.password = "password"
  config.key_password = "password"

  client.key_storeage = new KeyStorage(config)

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
  
  test("Transacted commit after unsubscribe"){
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
    subscribe("1", "/queue/test", c=producer)
    assert_received("END", "1", producer)
    // since we committed the transaction AFTER un-subscribing, there should be nothing in
    // the queue

  }

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

  test("ack:client redelivers on client disconnect") {
    connect("1.1")

    def put(id:Int) = {
      client.write(
        "SEND\n" +
        "destination:/queue/ackmode-client\n" +
        "\n" +
        "message:"+id+"\n")
    }
    put(1)
    put(2)
    put(3)

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/ackmode-client\n" +
      "ack:client\n" +
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
      "destination:/queue/ackmode-client\n" +
      "ack:client\n" +
      "id:0\n" +
      "\n")
    get(3)
    
    
  }


  test("ack:client-individual redelivers on client disconnect") {
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
      "ack:client-individual\n" +
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
      "ack:client-individual\n" +
      "id:0\n" +
      "\n")
    get(1)
    get(3)


  }

}

class StompSecurityTest extends StompTestSupport {

  override def broker_config_uri: String = "xml:classpath:apollo-stomp-secure.xml"

  override def beforeAll = {
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

    subscribe("0","/queue/sendsid")
    async_send("/queue/sendsid", "hello")

    val frame = client.receive()
    frame should startWith("MESSAGE\n")
    frame should include("JMSXUserID:can_send_create_consume_queue\n")
    frame should include("sender-ip:127.0.0.1\n")
  }
}

class StompSslSecurityTest extends StompTestSupport {

  override def broker_config_uri: String = "xml:classpath:apollo-stomp-ssl-secure.xml"

  override def beforeAll = {
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
    val config = new KeyStorageDTO
    config.file = basedir/"src"/"test"/"resources"/"client.ks"
    config.password = "password"
    config.key_password = "password"
    client.key_storeage = new KeyStorage(config)
  }

  test("Connect with cert and no id password") {
    use_client_cert
    connect("1.1", client)
  }

}

class StompWildcardTest extends StompTestSupport {

  def path_separator = "."

  test("Wildcard subscription") {
    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/foo"+path_separator+"*\n" +
      "id:1\n" +
      "receipt:0\n"+
      "\n")

    wait_for_receipt("0")

    def put(dest:String) = {
      client.write(
        "SEND\n" +
        "destination:/queue/"+dest+"\n" +
        "\n" +
        "message:"+dest+"\n")
    }

    def get(dest:String) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith("\n\nmessage:%s\n".format(dest))
    }

    // We should not get this one..
    put("bar"+path_separator+"a")

    put("foo"+path_separator+"a")
    get("foo"+path_separator+"a")

    put("foo"+path_separator+"b")
    get("foo"+path_separator+"b")
  }
}

class CustomStompWildcardTest extends StompWildcardTest {
  override def broker_config_uri: String = "xml:classpath:apollo-stomp-custom-dest-delimiters.xml"
  override def path_separator = "/"
}

class StompExpirationTest extends StompTestSupport {

  def path_separator = "."

  test("Messages Expire") {
    connect("1.1")

    def put(msg:String, ttl:Option[Long]=None) = {
      val expires_header = ttl.map(t=> "expires:"+(System.currentTimeMillis()+t)+"\n").getOrElse("")
      client.write(
        "SEND\n" +
        expires_header +
        "destination:/queue/exp\n" +
        "\n" +
        "message:"+msg+"\n")
    }

    put("1")
    put("2", Some(1000L))
    put("3")

    Thread.sleep(2000)

    client.write(
      "SUBSCRIBE\n" +
      "destination:/queue/exp\n" +
      "id:1\n" +
      "receipt:0\n"+
      "\n")
    wait_for_receipt("0")


    def get(dest:String) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith("\n\nmessage:%s\n".format(dest))
    }

    get("1")
    get("3")
  }
}

class StompTempDestinationTest extends StompTestSupport {

  def path_separator = "."

  test("Temp Queue Send Receive") {
    connect("1.1")

    def put(msg:String) = {
      client.write(
        "SEND\n" +
        "destination:/temp-queue/test\n" +
        "reply-to:/temp-queue/test\n" +
        "receipt:0\n" +
        "\n" +
        "message:"+msg+"\n")
      wait_for_receipt("0")
    }

    put("1")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/temp-queue/test\n" +
      "id:1\n" +
      "\n")

    def get(dest:String) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith("\n\nmessage:%s\n".format(dest))

      // extract headers as a map of values.
      Map((frame.split("\n").reverse.flatMap { line =>
        if( line.contains(":") ) {
          val parts = line.split(":", 2)
          Some((parts(0), parts(1)))
        } else {
          None
        }
      }):_*)
    }

    // The destination and reply-to headers should get updated with actual
    // Queue names
    val message = get("1")
    val actual_temp_dest_name = message.get("destination").get
    actual_temp_dest_name should startWith("/queue/temp.default.")
    message.get("reply-to") should be === ( message.get("destination") )

    // Different connection should be able to send a message to the temp destination..
    var other = new StompClient
    connect("1.1", other)
    other.write(
      "SEND\n" +
      "destination:"+actual_temp_dest_name+"\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0", other)

    // First client chould get the message.
    var frame = client.receive()
    frame should startWith("MESSAGE\n")

    // But not consume from it.
    other.write(
      "SUBSCRIBE\n" +
      "destination:"+actual_temp_dest_name+"\n" +
      "id:1\n" +
      "receipt:0\n" +
      "\n")
    frame = other.receive()
    frame should startWith("ERROR\n")
    frame should include regex("""message:Not authorized to receive from the temporary destination""")
    other.close()

    // Check that temp queue is deleted once the client disconnects
    put("2")
    expect(true)(queue_exists(actual_temp_dest_name.stripPrefix("/queue/")))
    client.close();

    within(10, SECONDS) {
      expect(false)(queue_exists(actual_temp_dest_name.stripPrefix("/queue/")))
    }
  }

  test("Temp Topic Send Receive") {
    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
      "destination:/temp-topic/test\n" +
      "id:1\n" +
      "\n")

    def get(dest:String) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith("\n\nmessage:%s\n".format(dest))

      // extract headers as a map of values.
      Map((frame.split("\n").reverse.flatMap { line =>
        if( line.contains(":") ) {
          val parts = line.split(":", 2)
          Some((parts(0), parts(1)))
        } else {
          None
        }
      }):_*)
    }

    def put(msg:String) = {
      client.write(
        "SEND\n" +
        "destination:/temp-topic/test\n" +
        "reply-to:/temp-topic/test\n" +
        "receipt:0\n" +
        "\n" +
        "message:"+msg+"\n")
      wait_for_receipt("0", client)
    }
    put("1")

    // The destination and reply-to headers should get updated with actual
    // Queue names
    val message = get("1")
    val actual_temp_dest_name = message.get("destination").get
    actual_temp_dest_name should startWith("/topic/temp.default.")
    message.get("reply-to") should be === ( message.get("destination") )

    // Different connection should be able to send a message to the temp destination..
    var other = new StompClient
    connect("1.1", other)
    other.write(
      "SEND\n" +
      "destination:"+actual_temp_dest_name+"\n" +
      "receipt:0\n" +
      "\n")
    wait_for_receipt("0", other)

    // First client chould get the message.
    var frame = client.receive()
    frame should startWith("MESSAGE\n")

    // But not consume from it.
    other.write(
      "SUBSCRIBE\n" +
      "destination:"+actual_temp_dest_name+"\n" +
      "id:1\n" +
      "receipt:0\n" +
      "\n")
    frame = other.receive()
    frame should startWith("ERROR\n")
    frame should include regex("""message:Not authorized to receive from the temporary destination""")
    other.close()

    // Check that temp queue is deleted once the client disconnects
    put("2")
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
}

class StompUdpInteropTest extends StompTestSupport {

  test("UDP to STOMP interop") {
    
    connect("1.1")
    subscribe("0", "/topic/udp")

    val udp_port:Int = connector_port("udp").get
    val channel = DatagramChannel.open();

    val target = new InetSocketAddress("127.0.0.1", udp_port)
    channel.send(new AsciiBuffer("Hello").toByteBuffer, target)

    assert_received("Hello")
  }
}

class StompNackTest extends StompTestSupport {

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
}

class StompNackTestOnLevelDBTest extends StompNackTest {
  override def broker_config_uri: String = "xml:classpath:apollo-stomp-leveldb.xml"

  test("NACKing without DLQ consumer (persistent)"){
    connect("1.1")
    sync_send("/queue/nacker.b", "this msg is persistent", "persistent:true\n")

    subscribe("0", "/queue/nacker.b", "client", false, "", false)

    var ack = assert_received("this msg is persistent", "0")
    ack(false)
    ack = assert_received("this msg is persistent", "0")
    ack(false)
    Thread.sleep(1000)
  }
}

class StompDropPolicyTest extends StompTestSupport {

  override def broker_config_uri: String = "xml:classpath:apollo-stomp-leveldb.xml"

  test("Head Drop Policy: Persistent") {
    connect("1.1")
    // Some of these messages should get dropped.
    for(i <- 0 until 1000) {
      sync_send("/queue/drop.head.persistent", "%0100d".format(i))
    }
    subscribe("0", "/queue/drop.head.persistent")
    for(i <- 446 until 1000) {
      assert_received("%0100d".format(i))
    }
  }

  test("Head Drop Policy: Non Persistent") {
    connect("1.1")
    // Some of these messages should get dropped.
    for(i <- 0 until 1000) {
      sync_send("/queue/drop.head.non", "%0100d".format(i))
    }
    subscribe("0", "/queue/drop.head.non")
    for(i <- 427 until 1000) {
      assert_received("%0100d".format(i))
    }
  }

  test("Tail Drop Policy: Persistent") {
    connect("1.1")
    // Some of these messages should get dropped.
    for(i <- 0 until 1000) {
      sync_send("/queue/drop.tail.persistent", "%0100d".format(i))
    }

    val metrics = queue_status("drop.tail.persistent").metrics
    metrics.queue_items should be < ( 1000L )

    subscribe("0", "/queue/drop.tail.persistent")
    for(i <- 0L until metrics.queue_items) {
      assert_received("%0100d".format(i))
    }

    async_send("/queue/drop.tail.persistent", "end")
    assert_received("end")

  }

  test("Tail Drop Policy: Non Persistent") {
    connect("1.1")
    // Some of these messages should get dropped.
    for(i <- 0 until 1000) {
      sync_send("/queue/drop.tail.non", "%0100d".format(i))
    }

    val metrics = queue_status("drop.tail.non").metrics
    metrics.queue_items should be < ( 1000L )

    subscribe("0", "/queue/drop.tail.non")
    for(i <- 0L until metrics.queue_items) {
      assert_received("%0100d".format(i))
    }

    async_send("/queue/drop.tail.non", "end")
    assert_received("end")
  }
}