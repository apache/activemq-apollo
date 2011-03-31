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
package org.apache.activemq.apollo.broker

import org.fusesource.hawtdispatch._
import org.fusesource.hawtdispatch.{Dispatch}
import org.apache.activemq.apollo.dto.{ConnectorDTO}
import protocol.{ProtocolFactory, Protocol}
import collection.mutable.HashMap
import org.apache.activemq.apollo.transport._
import org.apache.activemq.apollo.util._
import ReporterLevel._
import org.apache.activemq.apollo.util.OptionSupport._


/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Connector extends Log {

  /**
   * Creates a default a configuration object.
   */
  def defaultConfig() = {
    val rc = new ConnectorDTO
    rc.id = "default"
    rc.bind = "tcp://0.0.0.0:61613"
    rc.protocol = "multi"
    rc.connection_limit = 1000
    rc
  }

  /**
   * Validates a configuration object.
   */
  def validate(config: ConnectorDTO, reporter:Reporter):ReporterLevel = {
    new Reporting(reporter) {
      if( empty(config.id) ) {
        error("Connector id must be specified")
      }
    }.result
  }


}

trait Connector extends BaseService {

  def broker:Broker
  def id:String
  def stopped(connection:BrokerConnection):Unit
  def config:ConnectorDTO
  def connections:HashMap[Long, BrokerConnection]
  def connection_counter:LongCounter

}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class AcceptingConnector(val broker:Broker, val id:String) extends Connector {

  import Connector._

  override val dispatch_queue = broker.dispatch_queue

  var config:ConnectorDTO = defaultConfig
  var transport_server:TransportServer = _
  var protocol:Protocol = _

  val connections = HashMap[Long, BrokerConnection]()
  val connection_counter = new LongCounter()

  override def toString = "connector: "+config.id

  object BrokerAcceptListener extends TransportAcceptListener {
    def onAcceptError(e: Exception): Unit = {
      warn(e, "Error occured while accepting client connection.")
    }

    def onAccept(transport: Transport): Unit = {
      if( protocol!=null ) {
        transport.setProtocolCodec(protocol.createProtocolCodec)
      }

      connection_counter.incrementAndGet
      var connection = new BrokerConnection(AcceptingConnector.this, broker.connection_id_counter.incrementAndGet)
      connection.dispatch_queue.setLabel("connection %d to %s".format(connection.id, transport.getRemoteAddress))
      connection.protocol_handler = protocol.createProtocolHandler
      connection.transport = transport

      broker.init_dispatch_queue(connection.dispatch_queue)

      connections.put(connection.id, connection)
      try {
        connection.start()
      } catch {
        case e1: Exception => {
          onAcceptError(e1)
        }
      }

      if(at_connection_limit) {
        // We stop accepting connections at this point.
        info("Connection limit reached. Clients connected: %d", connections.size)
        transport_server.suspend
      }
    }
  }

  def at_connection_limit = {
    connections.size >= config.connection_limit.getOrElse(Integer.MAX_VALUE)
  }

  /**
   * Validates and then applies the configuration.
   */
  def configure(config: ConnectorDTO, reporter:Reporter) = ^{
    if ( validate(config, reporter) < ERROR ) {
      this.config = config

      if( service_state.is_started ) {
        // TODO: apply changes while running
        reporter.report(WARN, "Updating connector configuration at runtime is not yet supported.  You must restart the broker for the change to take effect.")

      }
    }
  } |>>: dispatch_queue


  override def _start(on_completed:Runnable) = {
    assert(config!=null, "Connector must be configured before it is started.")
    protocol = ProtocolFactory.get(config.protocol.getOrElse("multi")).get
    transport_server = TransportFactory.bind( config.bind )
    transport_server.setDispatchQueue(dispatch_queue)
    transport_server.setAcceptListener(BrokerAcceptListener)

    if( transport_server.isInstanceOf[KeyAndTrustAware] ) {
      if( broker.key_storage!=null ) {
        transport_server.asInstanceOf[KeyAndTrustAware].setTrustManagers(broker.key_storage.create_trust_managers)
        transport_server.asInstanceOf[KeyAndTrustAware].setKeyManagers(broker.key_storage.create_key_managers)
      } else {
        warn("You are using a transport the expects the broker's key storage to be configured.")
      }
    }
    transport_server.start(^{
      broker.console_log.info("Accepting connections at: "+transport_server.getBoundAddress)
      on_completed.run
    })
  }


  override def _stop(on_completed:Runnable): Unit = {
    transport_server.stop(^{
      broker.console_log.info("Stopped connector at: "+config.bind)
      val tracker = new LoggingTracker(toString, broker.console_log, dispatch_queue)
      connections.valuesIterator.foreach { connection=>
        tracker.stop(connection)
      }
      tracker.callback(on_completed)
    })
  }

  /**
   * Connections callback into the connector when they are stopped so that we can
   * stop tracking them.
   */
  def stopped(connection:BrokerConnection) = dispatch_queue {
    val at_limit = at_connection_limit
    if( connections.remove(connection.id).isDefined ) {
      if( at_limit ) {
        transport_server.resume
      }
    }
  }

}
