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

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach
import java.lang.String
import org.fusesource.hawtdispatch._
import org.fusesource.hawtbuf.Buffer._
import java.net.InetSocketAddress
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.util._
import org.fusesource.mqtt.client._
import QoS._
import org.apache.activemq.apollo.dto.TopicStatusDTO
import java.util.concurrent.TimeUnit._
import FileSupport._
import FutureResult._

class MqttTestSupport extends BrokerFunSuiteSupport with ShouldMatchers with BeforeAndAfterEach with Logging {


  override def broker_config_uri = "xml:classpath:apollo-mqtt.xml"

  var clients = List[MqttClient]()
  var client = create_client

  def create_client = {
    val client = new MqttClient
    clients ::= client
    client
  }

  override protected def afterEach() = {
    super.afterEach
    clients.foreach(_.disconnect)
    clients = Nil
    client = create_client
  }

  class MqttClient extends MQTT {

    var connection: BlockingConnection = _

    def open(host: String, port: Int) = {
      setHost(host, port)
      connection = blockingConnection();
      connection.connect();
    }

    def disconnect() = {
      if( connection!=null ) {
        connection.disconnect()
      }
    }
  }

  def connect(c: MqttClient = client) = {
    c.open("localhost", port)
  }

  def disconnect(c: MqttClient = client) = {
    c.disconnect()
  }

  def kill(c: MqttClient = client) = {
    c.connection.kill()
  }

  def publish(topic: String, message: String, qos: QoS = AT_MOST_ONCE, retain: Boolean = false, c: MqttClient = client) = {
    c.connection.publish(topic, message.getBytes("UTF-8"), qos, retain)
  }

  def subscribe(topic: String, qos: QoS = AT_MOST_ONCE, c: MqttClient = client) = {
    c.connection.subscribe(Array(new org.fusesource.mqtt.client.Topic(topic, qos)))
  }

  def unsubscribe(topic: String, c: MqttClient = client) = {
    c.connection.unsubscribe(Array(topic))
  }

  def should_receive(body: String, topic: String = null, c: MqttClient = client) = {
    val msg = c.connection.receive(5, SECONDS);
    expect(true)(msg != null)
    if (topic != null) {
      msg.getTopic should equal(topic)
    }
    new String(msg.getPayload, "UTF-8") should equal(body)
    msg.ack()
  }

}
