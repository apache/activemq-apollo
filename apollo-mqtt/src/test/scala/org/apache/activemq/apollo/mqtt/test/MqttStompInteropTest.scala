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

import org.fusesource.mqtt.client._
import QoS._
import java.util.concurrent.TimeUnit._
import org.fusesource.stomp.codec.StompFrame
import org.fusesource.stomp.client.{Constants, Stomp}
import org.fusesource.hawtbuf.Buffer

class MqttStompInteropTest extends MqttTestSupport {

  import Constants._
  import Buffer._

  test("MQTT to STOMP via topic") {
    val stomp = new Stomp("localhost", port).connectFuture().await()

    // Setup the STOMP subscription.
    val subscribe = new StompFrame(SUBSCRIBE)
    subscribe.addHeader(ID, ascii("0"))
    subscribe.addHeader(DESTINATION, ascii("/topic/mqtt.to.stomp"))
    stomp.request(subscribe).await()

    // Send from MQTT.
    connect()
    publish("mqtt/to/stomp", "Hello World", AT_LEAST_ONCE)

    val frame = stomp.receive().await(5, SECONDS)
    expect(true, "receive timeout")(frame != null)
    frame.action().toString should be(MESSAGE.toString)
    frame.contentAsString() should be("Hello World")

  }

  test("STOMP to MQTT via topic") {
    connect()
    subscribe("stomp/to/mqtt")

    val stomp = new Stomp("localhost", port).connectFuture().await()
    val send = new StompFrame(SEND)
    send.addHeader(DESTINATION, ascii("/topic/stomp.to.mqtt"))
    send.addHeader(MESSAGE_ID, ascii("test"))
    send.content(ascii("Hello World"))
    stomp.send(send)

    should_receive(
      "MESSAGE\n" +
              "content-length:11\n" +
              "message-id:test\n" +
              "destination:/topic/stomp.to.mqtt\n" +
              "\n" +
              "Hello World",
      "stomp/to/mqtt")
  }
}
