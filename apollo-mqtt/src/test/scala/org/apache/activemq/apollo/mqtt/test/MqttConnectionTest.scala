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
import java.util.concurrent.TimeUnit._

class MqttConnectionTest extends MqttTestSupport {

  test("MQTT CONNECT") {
    client.open("localhost", port)
  }

  test("MQTT Broker times out idle connection") {

    val queue = createQueue("test")
    client.setKeepAlive(1)
    client.setDispatchQueue(queue)
    client.setReconnectAttemptsMax(0)
    client.open("localhost", port)

    client.connection.isConnected should be(true)
    queue.suspend() // this will cause the client to hang
    Thread.sleep(1000 * 2);
    queue.resume()
    within(1, SECONDS) {
      client.connection.isConnected should be(false)
    }
  }

}

