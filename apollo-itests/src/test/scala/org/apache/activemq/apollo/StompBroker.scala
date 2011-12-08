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
package org.apache.activemq.apollo

import broker.{BrokerFactory, Broker}
import java.net.InetSocketAddress
import util.{Logging, ServiceControl}
import java.util.Hashtable
import javax.naming.InitialContext
import javax.jms.ConnectionFactory


/**
 *
 */
class StompBroker extends BrokerService with Logging {

  var broker: Broker = null
  var port = 0
  var started = false
  val broker_config_uri = "xml:classpath:apollo-stomp.xml"

  override def start = {
    try {
      info("Loading broker configuration from the classpath with URI: " + broker_config_uri)
      broker = BrokerFactory.createBroker(broker_config_uri)
      ServiceControl.start(broker, "Starting broker")
      port = broker.get_socket_address.asInstanceOf[InetSocketAddress].getPort
    }
    catch {
      case e:Throwable => e.printStackTrace
      throw e
    }
  }

  override def get_connection_factory = {
    if (!started) {
      start
    }
    val jndiConfig = new Hashtable[String, String]
    jndiConfig.put("java.naming.factory.initial", "org.fusesource.stompjms.jndi.StompJmsInitialContextFactory")
    jndiConfig.put("java.naming.provider.url", get_connection_uri)
    jndiConfig.put("java.naming.security.principal", "admin")
    jndiConfig.put("java.naming.security.credentials", "password")
    val ctx = new InitialContext(jndiConfig)
    ctx.lookup("ConnectionFactory").asInstanceOf[ConnectionFactory]
  }
  

  override def get_connection_uri = "tcp://localhost:%s".format(port);

  override def stop = ServiceControl.stop(broker, "Stopping broker")

}