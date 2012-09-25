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

class MqttQosTest extends MqttTestSupport {

  //
  // Lets make sure we can publish and subscribe with all the QoS combinations.
  //
  for (clean <- List(true, false)) {
    for (send_qos <- List(AT_MOST_ONCE, AT_LEAST_ONCE, EXACTLY_ONCE)) {
      for (receive_qos <- List(AT_MOST_ONCE, AT_LEAST_ONCE, EXACTLY_ONCE)) {
        test("Publish " + send_qos + " and subscribe " + receive_qos + " on clean session: " + clean) {
          val topic = "qos/" + send_qos + "/" + receive_qos + "/" + clean
          client.setClientId(topic)
          client.setCleanSession(clean)
          connect()
          subscribe(topic, receive_qos)
          publish(topic, "1", send_qos)
          should_receive("1", topic)
        }
      }
    }
  }
}
