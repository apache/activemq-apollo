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

package org.apache.activemq.apollo.openwire

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach
import java.lang.String
import org.apache.activemq.apollo.broker.{KeyStorage, Broker, BrokerFactory}
import org.apache.activemq.apollo.util.{FileSupport, Logging, FunSuiteSupport, ServiceControl}
import FileSupport._
import javax.jms.Connection
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.command.{ActiveMQTopic, ActiveMQQueue}
import java.net.InetSocketAddress

class OpenwireTestSupport extends FunSuiteSupport with ShouldMatchers with BeforeAndAfterEach with Logging {
  var broker: Broker = null
  var port = 0

  val broker_config_uri = "xml:classpath:apollo-openwire.xml"
  val transport_scheme = "tcp"
  val transport_host = "localhost"
  val uri_options = ""//"?wireFormat.maxInactivityDuration=1000000&wireFormat.maxInactivityDurationInitalDelay=1000000"

  override protected def beforeAll() {
    info("Loading broker configuration from the classpath with URI: " + broker_config_uri)
    broker = BrokerFactory.createBroker(broker_config_uri)
    ServiceControl.start(broker, "Starting broker")
    port = broker.get_socket_address.asInstanceOf[InetSocketAddress].getPort
  }

  var default_connection:Connection = _
  var connections = List[Connection]()

  override protected def afterAll() {
    broker.stop()
  }

  override protected def afterEach() {
    super.afterEach()
    connections.foreach(_.close())
    connections = Nil
    default_connection = null
  }

//  def connection_uri = transportScheme + "://localhost:%d?wireFormat.maxInactivityDuration=1000000&wireFormat.maxInactivityDurationInitalDelay=1000000".format(port)
  def connection_uri = (transport_scheme + "://" + transport_host + ":%d" + uri_options).format(port)
  def create_connection_factory = new ActiveMQConnectionFactory(connection_uri)
  def create_connection: Connection = create_connection_factory.createConnection
  def queue(value:String) = new ActiveMQQueue(value);
  def topic(value:String) = new ActiveMQTopic(value);

  def connect(start:Boolean=true) = {
    val connection = create_connection
    connections ::= connection
    if(default_connection==null) {
      default_connection = connection
    }
    if( start ) {
      connection.start()
    }
    connection
  }

}