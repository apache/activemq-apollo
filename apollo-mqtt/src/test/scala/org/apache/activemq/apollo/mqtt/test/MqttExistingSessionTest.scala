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

class MqttExistingSessionTest extends MqttTestSupport {
  client.setCleanSession(false);
  client.setClientId("default")

  def restart = {}

  test("Subscribe is remembered on existing sessions.") {
    connect()
    subscribe("existing/sub")

    // reconnect...
    disconnect()
    restart
    connect()

    // The subscribe should still be remembered.
    publish("existing/sub", "1", EXACTLY_ONCE)
    should_receive("1", "existing/sub")
  }

  test("Subscribe") {

    connect()

    var client2 = create_client
    client2.setCleanSession(false);
    client2.setClientId("another")
    connect(c = client2)

    subscribe(topic = "1/#", c = client2)
    publish("1/data/apps/crm/Opportunity/60", "2", EXACTLY_ONCE)
    should_receive(body = "2", topic = "1/data/apps/crm/Opportunity/60", c = client2)
  }
}
