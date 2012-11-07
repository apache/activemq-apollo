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

package org.apache.activemq.apollo.mqtt.test

import org.fusesource.hawtdispatch._
import org.fusesource.mqtt.client._
import QoS._
import java.util.concurrent.TimeUnit._

class MqttCleanSessionTest extends MqttTestSupport {

  test("Subscribing to overlapping topics") {
    connect()
    subscribe("overlap/#")
    subscribe("overlap/a/b")
    subscribe("overlap/a/+")

    // This is checking that we don't get duplicate messages
    // due to the overlapping nature of the subscriptions.
    publish("overlap/a/b", "1", EXACTLY_ONCE)
    should_receive("1", "overlap/a/b")
    publish("overlap/a", "2", EXACTLY_ONCE)
    should_receive("2", "overlap/a")
    publish("overlap/a/b", "3", EXACTLY_ONCE)
    should_receive("3", "overlap/a/b")

    // Dropping subscriptions should not affect us while there
    // is still a matching sub left.
    unsubscribe("overlap/#")
    publish("overlap/a/b", "4", EXACTLY_ONCE)
    should_receive("4", "overlap/a/b")

    unsubscribe("overlap/a/b")
    publish("overlap/a/b", "5", EXACTLY_ONCE)
    should_receive("5", "overlap/a/b")

    // Drop the last subscription.. but setup root sub we can test
    // without using timeouts.
    publish("foo", "6", EXACTLY_ONCE) // never did match
    unsubscribe("overlap/a/+")
    publish("overlap/a/b", "7", EXACTLY_ONCE) // should not match anymore.

    // Send a message through to flush everything out and verify none of the other
    // are getting routed to us.
    println("subscribing...")
    subscribe("#")
    println("publishinng...")
    publish("foo", "8", EXACTLY_ONCE)
    println("receiving...")
    should_receive("8", "foo")

  }

  def will_test(kill_action: (MqttClient) => Unit) = {
    connect()
    subscribe("will/foo")

    val will_client = new MqttClient
    will_client.setWillTopic("will/foo")
    will_client.setWillQos(AT_LEAST_ONCE)
    will_client.setWillRetain(false)
    will_client.setWillMessage("1");
    kill_action(will_client)
    should_receive("1", "will/foo")
  }

  test("Will sent on socket failure") {
    will_test {
      client =>
        connect(client)
        kill(client)
    }
  }

  test("Will sent on keepalive failure") {
    will_test {
      client =>
        val queue = createQueue("")
        client.setKeepAlive(1)
        client.setDispatchQueue(queue)
        client.setReconnectAttemptsMax(0)
        client.setDispatchQueue(queue);
        connect(client)

        // Client should time out once we suspend the queue.
        queue.suspend()
        Thread.sleep(1000 * 2);
        queue.resume()
    }
  }

  test("Will NOT sent on clean disconnect") {
    expect(true) {
      try {
        will_test {
          client =>
            connect(client)
            disconnect(client)
        }
        false
      } catch {
        case e: Throwable =>
          true
      }
    }
  }

  test("Publish") {
    connect()
    publish("test", "message", EXACTLY_ONCE)
    topic_status("test").metrics.enqueue_item_counter should be(1)

    publish("test", "message", AT_LEAST_ONCE)
    topic_status("test").metrics.enqueue_item_counter should be(2)

    publish("test", "message", AT_MOST_ONCE)

    within(1, SECONDS) {
      // since AT_MOST_ONCE use non-blocking sends.
      topic_status("test").metrics.enqueue_item_counter should be(3)
    }
  }

  test("Subscribe") {
    connect()
    subscribe("foo")
    publish("foo", "1", EXACTLY_ONCE)
    should_receive("1", "foo")
  }

  test("Subscribing wiht multi-level wildcard") {
    connect()
    subscribe("mwild/#")
    publish("mwild", "1", EXACTLY_ONCE)
    // Should not match
    publish("mwild.", "2", EXACTLY_ONCE)
    publish("mwild/hello", "3", EXACTLY_ONCE)
    publish("mwild/hello/world", "4", EXACTLY_ONCE)

    for (i <- List(("mwild", "1"), ("mwild/hello", "3"), ("mwild/hello/world", "4"))) {
      should_receive(i._2, i._1)
    }
  }

  test("Subscribing with single-level wildcard") {
    connect()
    subscribe("swild/+")
    // Should not a match
    publish("swild", "1", EXACTLY_ONCE)
    publish("swild/hello", "2", EXACTLY_ONCE)
    // Should not match..
    publish("swild/hello/world", "3", EXACTLY_ONCE)
    // Should match. so.cool is only one level, but STOMP would treat it like 2,
    // Lets make sure Apollo's STOMP support does not mess with us.
    publish("swild/so.cool", "4", EXACTLY_ONCE)

    for (i <- List(("swild/hello", "2"), ("swild/so.cool", "4"))) {
      should_receive(i._2, i._1)
    }
  }

  test("Retained Messages are retained") {
    connect()
    publish("retained", "1", AT_LEAST_ONCE, false)
    publish("retained", "2", AT_LEAST_ONCE, true)
    publish("retained", "3", AT_LEAST_ONCE, false)
    subscribe("retained")
    should_receive("2", "retained")
  }

  test("Non-retained Messages are not retained") {
    connect()
    publish("notretained", "1", AT_LEAST_ONCE, false)
    subscribe("notretained")
    publish("notretained", "2", AT_LEAST_ONCE, false)
    should_receive("2", "notretained")
  }

  test("You can clear out topic's retained message, by sending a retained empty message.") {
    connect()
    publish("clearretained", "1", AT_LEAST_ONCE, true)
    publish("clearretained", "", AT_LEAST_ONCE, true)
    subscribe("clearretained")
    publish("clearretained", "2", AT_LEAST_ONCE, false)
    should_receive("2", "clearretained")
  }

}
