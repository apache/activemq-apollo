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

/**
 * These test cases check to make sure the broker stats are consistent with what
 * would be expected.  These tests can't be run in parallel since they look at
 * aggregate destination metrics.
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
      stat2.producer_count should be(stat1.producer_count - 1)
      stat2.consumer_count should be(stat1.consumer_count - 1)
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
    within(1, SECONDS) {
      val stat2 = get_queue_metrics
      stat2.producer_count should be(stat1.producer_count + 1)
      stat2.consumer_count should be(stat1.consumer_count)
      stat2.enqueue_item_counter should be(stat1.enqueue_item_counter + 3)
      stat2.dequeue_item_counter should be(stat1.dequeue_item_counter + 0)
      stat2.queue_items should be(stat1.queue_items + 3)
    }

    // Delete the queue
    delete_queue("willdelete")

    within(1, SECONDS) {
      val stat3 = get_queue_metrics
      stat3.producer_count should be(stat1.producer_count)
      stat3.consumer_count should be(stat1.consumer_count)
      stat3.enqueue_item_counter should be(stat1.enqueue_item_counter + 3)
      stat3.dequeue_item_counter should be(stat1.dequeue_item_counter)
      stat3.queue_items should be(stat1.queue_items)
    }
  }

  test("Old consumers on topic slow_consumer_policy='queue' does not affect the agregate queue-metrics") {
    connect("1.1")

    subscribe("0", "/topic/queued.test1", "client");
    val stat1 = get_topic_metrics

    async_send("/topic/queued.test1", 1)
    async_send("/topic/queued.test1", 2)
    async_send("/topic/queued.test1", 3)

    val ack1 = assert_received(1)
    val ack2 = assert_received(2)
    val ack3 = assert_received(3)

    // not acked yet.
    within(1, SECONDS) {
      val stat2 = get_topic_metrics
      stat2.producer_count should be(stat1.producer_count+1)
      stat2.consumer_count should be(stat1.consumer_count)
      stat2.enqueue_item_counter should be(stat1.enqueue_item_counter + 3)
      stat2.dequeue_item_counter should be(stat1.dequeue_item_counter + 3)
      stat2.queue_items should be(stat1.queue_items)
    }

    // Close the subscription.
    unsubscribe("0")

    within(1, SECONDS) {
      val stat3 = get_topic_metrics
      stat3.producer_count should be(stat1.producer_count+1)
      stat3.consumer_count should be(stat1.consumer_count-1)
      stat3.enqueue_item_counter should be(stat1.enqueue_item_counter + 3)
      stat3.dequeue_item_counter should be(stat1.dequeue_item_counter + 3)
      stat3.queue_items should be(stat1.queue_items)
    }
  }

  test("New Topic Stats") {
    connect("1.1")
    subscribe("0", "/topic/newstats");
    val stats = topic_status("newstats")
    var now = System.currentTimeMillis()
    (now - stats.metrics.enqueue_ts) should (be < 10 * 1000L)
    (now - stats.metrics.dequeue_ts) should (be < 10 * 1000L)
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
    // Also look at the aggregatee metrics..
    val get_dest_metrics1 = get_dest_metrics

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
    stat2.metrics.dequeue_item_counter should be(2)
    stat2.metrics.queue_items should be(0)

    // Ack now..
    ack2(true);
    ack3(true)

    within(1, SECONDS) {
      val stat3 = topic_status("queued.stats")
      stat3.producers.size() should be(1)
      stat3.consumers.size() should be(1)
      stat3.dsubs.size() should be(0)
      stat3.metrics.enqueue_item_counter should be(3)
      stat3.metrics.dequeue_item_counter should be(2)
      stat3.metrics.queue_items should be(0)

      val get_dest_metrics2 = get_dest_metrics

      get_dest_metrics2.enqueue_item_counter should be( get_dest_metrics1.enqueue_item_counter+3 )
      get_dest_metrics2.dequeue_item_counter should be( get_dest_metrics1.dequeue_item_counter+2 )
      get_dest_metrics2.queue_items should be( get_dest_metrics1.queue_items )
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

      val get_dest_metrics2 = get_dest_metrics

      get_dest_metrics2.enqueue_item_counter should be( get_dest_metrics1.enqueue_item_counter+3 )
      get_dest_metrics2.dequeue_item_counter should be( get_dest_metrics1.dequeue_item_counter+2 )
      get_dest_metrics2.queue_items should be( get_dest_metrics1.queue_items )
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

class StompLevelDBMetricsTest extends StompMetricsTest {

  override def broker_config_uri: String = "xml:classpath:apollo-stomp-leveldb.xml"

  test("slow_consumer_policy='queue' /w 1 slow and 1 fast consumer.") {
    var dest_name = next_id("queued.metrics")
    val dest = "/topic/"+dest_name

    val fast = new StompClient
    connect("1.1", fast)
    subscribe("fast", dest, "auto", c=fast);

    val slow = new StompClient
    connect("1.1", slow)
    subscribe("fast", dest, "client", c=slow);

    connect("1.1")
    for( i <- 1 to 1000 ) {
      async_send(dest, "%01204d".format(i))
    }

    for( i <- 1 to 1000 ) {
      assert_received("%01204d".format(i),c=fast)
    }

    within(3, SECONDS) {
      val stat = topic_status(dest_name).metrics
      stat.queue_items should be >= (0L)
      stat.swapped_in_items should be <= ( stat.queue_items ) // some of it swapped.
      stat.enqueue_item_counter should be(1000L)
    }

    slow.close()

    within(3, SECONDS) {
      val stat = topic_status(dest_name).metrics
      stat.queue_items should be (0L)
      stat.swapped_in_items should be(0L)
      stat.enqueue_item_counter should be(1000L)
    }
  }

}
