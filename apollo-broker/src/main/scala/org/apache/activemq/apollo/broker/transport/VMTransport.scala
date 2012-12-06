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
package org.apache.activemq.apollo.broker.transport

import java.io.IOException
import java.net.URI
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import org.apache.activemq.apollo.broker._

import scala.collection.JavaConversions._
import org.fusesource.hawtdispatch.transport._
import org.apache.activemq.apollo.util._
import java.lang.String
import org.apache.activemq.apollo.dto.AcceptingConnectorDTO
import org.fusesource.hawtdispatch._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object VMTransportFactory extends Log {
  val DEFAULT_PIPE_NAME = "default"
}

/**
 * Implements the vm transport which behaves like the pipe transport except that
 * it can start embedded brokers up on demand.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class VMTransportFactory extends Logging with TransportFactory.Provider {
  import PipeTransportRegistry._
  import VMTransportFactory._
  override protected def log = VMTransportFactory

  /**
   * This extension of the PipeTransportServer shuts down the broker
   * when all the connections are disconnected.
   *
   * @author chirino
   */
  class VmTransportServer extends PipeTransportServer {
    val refs = new AtomicInteger()
    var broker: Broker = null

    override def createClientTransport(): PipeTransport = {
      refs.incrementAndGet();
      new PipeTransport(this) {
        val stopped = new AtomicBoolean()

        override def stop(onComplete:Task) = {
          if (stopped.compareAndSet(false, true)) {
            super.stop(onComplete);
            if (refs.decrementAndGet() == 0) {
              stopBroker();
            }
          }
        }
      };
    }

    def setBroker(broker: Broker) = {
      this.broker = broker;
    }

    def stopBroker() = {
      try {
        this.broker.stop(NOOP);
        unbind(this);
      } catch {
        case e: Exception =>
          error("Failed to stop the broker gracefully: " + e);
          debug("Failed to stop the broker gracefully: ", e);
      }
    }
  }

  override def bind(location: String):TransportServer = {
    if( !location.startsWith("vm:") ) {
        return null;
    }
    PipeTransportRegistry.bind(location)
  }

  override def connect(location: String): Transport = {
    if( !location.startsWith("vm:") ) {
        return null;
    }

    try {
      var uri = new URI(location)
      var brokerURI: String = null;
      var create = true;
      var name = uri.getHost();
      if (name == null) {
        name = DEFAULT_PIPE_NAME;
      }

      var options = URISupport.parseParamters(uri);
      var config = options.remove("broker").asInstanceOf[String]
      if (config != null) {
        brokerURI = config;
      }
      if ("false".equals(options.remove("create"))) {
        create = false;
      }


      var server = servers.get(name);
      if (server == null && create) {

        // This is the connector that the broker needs.
        val connector = new AcceptingConnectorDTO
        connector.id = "vm"
        connector.bind = "vm://" + name

        // Create the broker on demand.
        var broker: Broker = null
        if (brokerURI == null) {
          // Lets create and configure it...
          broker = new Broker()
          broker.config.connectors.clear
          broker.config.connectors.add(connector)
        } else {
          // Use the user specified config
          broker = BrokerFactory.createBroker(brokerURI);
          // we need to add in the connector if it was not in the config...
          val found = broker.config.connectors.toList.find { _ match {
            case dto:AcceptingConnectorDTO=> dto.bind == connector.bind
            case _ => false
          }}
          if (found.isEmpty) {
            broker.config.connectors.add(connector)
          }
        }

        // TODO: get rid of this blocking wait.
        val tracker = new LoggingTracker("vm broker startup")
        tracker.start(broker)
        tracker.await

        server = servers.get(name)
      }

      if (server == null) {
        throw new IOException("Server is not bound: " + name)
      }

      var transport = server.connect()
      import TransportFactorySupport._
      verify(configure(transport, options), options)

    } catch {
      case e:IllegalArgumentException=>
        throw e
      case e: Exception =>
        throw IOExceptionSupport.create(e)
    }
  }

}
