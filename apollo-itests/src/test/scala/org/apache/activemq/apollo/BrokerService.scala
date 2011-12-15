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
import javax.jms.ConnectionFactory
import java.net.InetSocketAddress
import util.{Logging, ServiceControl}
import java.util.Hashtable
import javax.naming.InitialContext


/**
 *
 */
trait BrokerService extends Logging {

  var broker: Broker = null
  var port = 0
  var started = false

  def start = {
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


  def stop = ServiceControl.stop(broker, "Stopping broker")

  def broker_config_uri:String

  def getConnectionFactory = {
    if (!started) {
      start
    }
    val jndiConfig = new Hashtable[String, String]
    jndiConfig.put("java.naming.factory.initial", getInitialContextFactoryClass)
    jndiConfig.put("java.naming.provider.url", getConnectionUri)
    jndiConfig.put("java.naming.security.principal", "admin")
    jndiConfig.put("java.naming.security.credentials", "password")
    val ctx = new InitialContext(jndiConfig)
    ctx.lookup("ConnectionFactory").asInstanceOf[ConnectionFactory]
  }

  protected def getInitialContextFactoryClass:String

  def getConnectionUri:String
}

/**
 *
 */
class StompBroker extends BrokerService {

  def broker_config_uri = "xml:classpath:apollo-stomp.xml"

  protected def getInitialContextFactoryClass = "org.fusesource.stompjms.jndi.StompJmsInitialContextFactory"

  def getConnectionUri = "tcp://localhost:%s".format(port);

}

/**
 *
 */
class OpenwireBroker extends BrokerService {

  def broker_config_uri = "xml:classpath:apollo-openwire.xml"

  protected def getInitialContextFactoryClass = "org.apache.activemq.jndi.ActiveMQInitialContextFactory"

  def getConnectionUri = "tcp://localhost:%s".format(port)

}
