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

package org.apache.activemq.apollo.openwire.test

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach
import java.lang.String
import javax.jms.{MessageConsumer, TextMessage, Connection}
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.command.{ActiveMQTopic, ActiveMQQueue}
import org.apache.activemq.apollo.broker.BrokerFunSuiteSupport

class OpenwireTestSupport extends BrokerFunSuiteSupport with ShouldMatchers with BeforeAndAfterEach {

  override def broker_config_uri = "xml:classpath:apollo-openwire.xml"

  val transport_scheme = "tcp"
  val transport_host = "localhost"

  var default_connection: Connection = _
  var connections = List[Connection]()

  val receive_timeout = 10*1000

  override protected def afterEach() {
    super.afterEach()
    for ( connection <- connections ) {
      try {
        connection.close()
      } catch {
        case e:Throwable =>
      }
    }
    connections = Nil
    default_connection = null
  }

  //  def connection_uri = transportScheme + "://localhost:%d?wireFormat.maxInactivityDuration=1000000&wireFormat.maxInactivityDurationInitalDelay=1000000".format(port)
  def connection_uri(uri_options: String = "") = (transport_scheme + "://" + transport_host + ":%d" + uri_options).format(port)

  def create_connection_factory(uri_options: String = "") = new ActiveMQConnectionFactory(connection_uri(uri_options))

  def create_connection(uri_options: String = "", user:String=null, password:String=null): Connection = {
    create_connection_factory(uri_options).createConnection(user, password)
  }

  def queue(value: String) = new ActiveMQQueue(value);

  def topic(value: String) = new ActiveMQTopic(value);

  def connect(uri_options: String = "", start: Boolean = true, user:String=null, password:String=null) = {
    val connection = create_connection(uri_options, user, password)
    connections ::= connection
    if (default_connection == null) {
      default_connection = connection
    }
    if (start) {
      connection.start()
    }
    connection
  }
  def disconnect(connection:Connection=default_connection) = {
    connection.close()
    if (connection == default_connection) {
      default_connection = null
    }
    connections = connections.filterNot(_ == connection)
  }

  def receive_text(consumer:MessageConsumer) = consumer.receive().asInstanceOf[TextMessage].getText


}