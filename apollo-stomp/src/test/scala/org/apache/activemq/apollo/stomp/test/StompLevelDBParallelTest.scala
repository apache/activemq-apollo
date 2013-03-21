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
import java.util.concurrent.TimeUnit._
import org.apache.activemq.apollo.broker._
import java.net.SocketTimeoutException
import java.util.concurrent.CountDownLatch

class StompLevelDBParallelTest extends StompParallelTest {

  override def broker_config_uri: String = "xml:classpath:apollo-stomp-leveldb.xml"

  def skip_if_not_leveldb = skip(!broker_config_uri.endsWith("-leveldb.xml"))

  test("pending_stores stuck") {
    skip_if_not_leveldb

    def pending_stores = {
      var rc = 0
      val done =new CountDownLatch(1);
      broker.default_virtual_host.store.get_store_status{ status =>
        rc = status.pending_stores
        done.countDown()
      }
      done.await()
      rc
    }

    run_exclusive {
      connect("1.1")
      val dest = next_id("pending_stores.n")
      var data: String = "x" * 1024
      var done = false

      within(10, SECONDS) {
        pending_stores should be (0)
      }

      var sent = 0
      while(!done) {
        async_send("/queue/"+dest, data, "persistent:true\nreceipt:x\n")
        try {
          wait_for_receipt("x", timeout = 2000)
          sent += 1
        } catch {
          case e:SocketTimeoutException =>
            done = true
        }
      }

      pending_stores should be (1)
      close()
      connect("1.1")
      subscribe("mysub", "/queue/"+dest)
      for( i <- 0 until sent) {
        assert_received(data)
      }

      within(10, SECONDS) {
        pending_stores should be (0)
      }
    }
  }


  test("(APLO-198) Apollo sometimes does not send all the messages in a queue") {
    skip_if_using_store
    connect("1.1")
    for (i <- 0 until 10000) {
      async_send("/queue/BIGQUEUE", "message #" + i)
    }
    sync_send("/queue/BIGQUEUE", "END")
    client.close

    var counter = 0
    for (i <- 0 until 100) {
      connect("1.1")
      subscribe("1", "/queue/BIGQUEUE", "client", false, "", false)
      for (j <- 0 until 100) {
        assert_received("message #" + counter)(true)
        counter += 1
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

  test("Multiple dsubs contain the same messages (Test case for APLO-210)") {
    skip_if_using_store

    val sub_count = 3
    val message_count = 1000

    // establish 3 durable subs..
    connect("1.1")
    for (sub <- 1 to sub_count) {
      subscribe(id = "sub" + sub, dest = "/topic/sometopic", persistent = true)
    }
    close()

    connect("1.1")

    val filler = ":" + ("x" * (1024 * 10))

    // Now send a bunch of messages....
    for (i <- 1 to message_count) {
      async_send(dest = "/topic/sometopic", headers = "persistent:true\n", body = i + filler)
    }

    // Empty out the durable durable sub
    for (sub <- 1 to sub_count) {
      subscribe(id = "sub" + sub, dest = "/topic/sometopic", persistent = true, sync = false)
      for (i <- 1 to message_count) {
        assert_received(body = i + filler, sub = "sub" + sub)
      }
    }

  }

  test("Can directly send an recieve from a durable sub") {
    skip_if_using_store
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
    def get(expected: String) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith("\n\n" + expected)
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
    skip_if_using_store
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
    subscribe("sub.1", "/topic/sometopic", headers="persistent:true\n")
    unsubscribe("sub.1")
    sync_send("/dsub/sub.1", 1)
  }

  test("Duplicate SUBSCRIBE updates durable subscription bindings") {
    skip_if_using_store
    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
              "destination:/topic/a\n" +
              "id:sub1\n" +
              "persistent:true\n" +
              "receipt:0\n" +
              "\n")
    wait_for_receipt("0")

    def get(expected: String) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith("\n\n" + expected)
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
    for (i <- 1 to 500) {
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
