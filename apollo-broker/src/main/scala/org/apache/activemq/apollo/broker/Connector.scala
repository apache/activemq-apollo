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

import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import org.fusesource.hawtdispatch.{Dispatch}
import org.apache.activemq.apollo.dto.{ConnectorDTO}
import protocol.{ProtocolFactory, Protocol}
import collection.mutable.HashMap
import org.apache.activemq.apollo.transport._
import org.apache.activemq.apollo.util._
import ReporterLevel._


/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Connector extends Log {

  val STICK_ON_THREAD_QUEUES = Broker.STICK_ON_THREAD_QUEUES

  /**
   * Creates a default a configuration object.
   */
  def defaultConfig() = {
    val rc = new ConnectorDTO
    rc.id = "default"
    rc.enabled = true
    rc.advertise = "tcp://localhost:61613"
    rc.bind = "tcp://0.0.0.0:61613"
    rc.protocol = "multi"
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

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Connector(val broker:Broker, val id:Long) extends BaseService with DispatchLogging {
  import Connector._

  override protected def log = Connector
  override val dispatchQueue = broker.dispatchQueue

  var config:ConnectorDTO = defaultConfig
  var transportServer:TransportServer = _
  var protocol:Protocol = _

  val connections = HashMap[Long, BrokerConnection]()
  override def toString = "connector: "+config.id
  val accept_counter = new LongCounter

  object BrokerAcceptListener extends TransportAcceptListener {
    def onAcceptError(error: Exception): Unit = {
      error.printStackTrace
      warn("Accept error: " + error)
      debug("Accept error details: ", error)
    }

    def onAccept(transport: Transport): Unit = {
      debug("Accepted connection from: %s", transport.getRemoteAddress)

      if( protocol!=null ) {
        transport.setProtocolCodec(protocol.createProtocolCodec)
      }

      accept_counter.incrementAndGet
      var connection = new BrokerConnection(Connector.this, broker.connection_id_counter.incrementAndGet)
      connection.protocolHandler = protocol.createProtocolHandler
      connection.transport = transport

      if( STICK_ON_THREAD_QUEUES ) {
        connection.dispatchQueue.setTargetQueue(Dispatch.getRandomThreadQueue)
      }

      // We release when it gets removed form the connections list.
      connection.dispatchQueue.retain
      connections.put(connection.id, connection)

      try {
        connection.start()
      } catch {
        case e1: Exception => {
          onAcceptError(e1)
        }
      }
    }
  }


  /**
   * Validates and then applies the configuration.
   */
  def configure(config: ConnectorDTO, reporter:Reporter) = ^{
    if ( validate(config, reporter) < ERROR ) {
      this.config = config

      if( serviceState.isStarted ) {
        // TODO: apply changes while running
        reporter.report(WARN, "Updating connector configuration at runtime is not yet supported.  You must restart the broker for the change to take effect.")

      }
    }
  } |>>: dispatchQueue


  override def _start(onCompleted:Runnable) = {
    assert(config!=null, "Connector must be configured before it is started.")
    protocol = ProtocolFactory.get(config.protocol).get
    transportServer = TransportFactory.bind( config.bind )
    transportServer.setDispatchQueue(dispatchQueue)
    transportServer.setAcceptListener(BrokerAcceptListener)
    transportServer.start(onCompleted)
  }


  override def _stop(onCompleted:Runnable): Unit = {
    transportServer.stop(^{
      val tracker = new LoggingTracker(toString, dispatchQueue)
      connections.valuesIterator.foreach { connection=>
        tracker.stop(connection)
      }
      tracker.callback(onCompleted)
    })
  }

  /**
   * Connections callback into the connector when they are stopped so that we can
   * stop tracking them.
   */
  def stopped(connection:BrokerConnection) = ^{
    if( connections.remove(connection.id).isDefined ) {
      connection.dispatchQueue.release
    }
  } |>>: dispatchQueue

}