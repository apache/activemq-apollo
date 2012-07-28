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
import org.apache.activemq.apollo.broker._

class StompDropPolicyTest extends StompTestSupport with BrokerParallelTestExecution {

  override def broker_config_uri: String = "xml:classpath:apollo-stomp-leveldb.xml"

  test("Head Drop Policy: Persistent") {
    connect("1.1")
    // Some of these messages should get dropped.
    for (i <- 0 until 1000) {
      sync_send("/queue/drop.head.persistent", "%0100d".format(i))
    }
    subscribe("0", "/queue/drop.head.persistent")

    val initial = client.receive().split("\n").last.toInt
    initial should be > (100)
    for (i <- (initial + 1) until 1000) {
      assert_received("%0100d".format(i))
    }
  }

  test("Head Drop Policy: Non Persistent") {
    connect("1.1")
    // Some of these messages should get dropped.
    for (i <- 0 until 1000) {
      sync_send("/queue/drop.head.non", "%0100d".format(i))
    }
    subscribe("0", "/queue/drop.head.non")
    val initial = client.receive().split("\n").last.toInt
    initial should be > (100)
    for (i <- (initial + 1) until 1000) {
      assert_received("%0100d".format(i))
    }
  }

  test("Tail Drop Policy: Persistent") {
    connect("1.1")
    // Some of these messages should get dropped.
    for (i <- 0 until 1000) {
      sync_send("/queue/drop.tail.persistent", "%0100d".format(i))
    }

    val metrics = queue_status("drop.tail.persistent").metrics
    metrics.queue_items should be < (1000L)

    subscribe("0", "/queue/drop.tail.persistent")
    for (i <- 0L until metrics.queue_items) {
      assert_received("%0100d".format(i))
    }

    async_send("/queue/drop.tail.persistent", "end")
    assert_received("end")

  }

  test("Tail Drop Policy: Non Persistent") {
    connect("1.1")
    // Some of these messages should get dropped.
    for (i <- 0 until 1000) {
      sync_send("/queue/drop.tail.non", "%0100d".format(i))
    }

    val metrics = queue_status("drop.tail.non").metrics
    metrics.queue_items should be < (1000L)

    subscribe("0", "/queue/drop.tail.non")
    for (i <- 0L until metrics.queue_items) {
      assert_received("%0100d".format(i))
    }

    async_send("/queue/drop.tail.non", "end")
    assert_received("end")
  }
}
