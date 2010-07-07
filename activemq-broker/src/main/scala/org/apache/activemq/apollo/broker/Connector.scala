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

import _root_.java.io.{File}
import _root_.org.apache.activemq.transport._
import _root_.org.apache.activemq.Service
import _root_.java.lang.{String}
import _root_.org.apache.activemq.util.{FactoryFinder, IOHelper}
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import _root_.scala.reflect.BeanProperty
import org.fusesource.hawtdispatch.{Dispatch, DispatchQueue, BaseRetained}
import java.util.{HashSet, LinkedList, LinkedHashMap, ArrayList}
import org.fusesource.hawtbuf._
import collection.JavaConversions
import org.apache.activemq.apollo.dto.{ConnectorDTO, BrokerDTO}
import JavaConversions._
import org.apache.activemq.wireformat.WireFormatFactory
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
  def default() = {
    val rc = new ConnectorDTO
    rc.id = "default"
    rc.enabled = true
    rc.advertise = "tcp://localhost:61616"
    rc.bind = "tcp://0.0.0.0:61616"
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
class Connector(val broker:Broker) extends BaseService with DispatchLogging {
  import Connector._

  override protected def log = Connector
  override val dispatchQueue = broker.dispatchQueue

  var config:ConnectorDTO = default
  var transportServer:TransportServer = _
  var wireFormatFactory:WireFormatFactory = _

  val connections: HashSet[Connection] = new HashSet[Connection]

  override def toString = "connector: "+config.id

  object BrokerAcceptListener extends TransportAcceptListener {
    def onAcceptError(error: Exception): Unit = {
      error.printStackTrace
      warn("Accept error: " + error)
      debug("Accept error details: ", error)
    }

    def onAccept(transport: Transport): Unit = {
      debug("Accepted connection from: %s", transport.getRemoteAddress)

      if( wireFormatFactory!=null ) {
        transport.setWireformat(wireFormatFactory.createWireFormat)
      }

      var connection = new BrokerConnection(Connector.this)
      connection.transport = transport

      if( STICK_ON_THREAD_QUEUES ) {
        connection.dispatchQueue.setTargetQueue(Dispatch.getRandomThreadQueue)
      }

      // We release when it gets removed form the connections list.
      connection.dispatchQueue.retain
      connections.add(connection)

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
    wireFormatFactory = TransportFactorySupport.createWireFormatFactory(config.protocol)

    transportServer = TransportFactory.bind( config.bind )
    transportServer.setDispatchQueue(dispatchQueue)
    transportServer.setAcceptListener(BrokerAcceptListener)
    transportServer.start(onCompleted)
  }


  override def _stop(onCompleted:Runnable): Unit = {
    transportServer.stop(^{
      val tracker = new LoggingTracker(toString, dispatchQueue)
      for (connection <- connections) {
        tracker.stop(connection)
      }
      tracker.callback(onCompleted)
    })
  }

  /**
   * Connections callback into the connector when they are stopped so that we can
   * stop tracking them.
   */
  def stopped(connection:Connection) = ^{
    if( connections.remove(connection) ) {
      connection.dispatchQueue.release
    }
  } |>>: dispatchQueue

}