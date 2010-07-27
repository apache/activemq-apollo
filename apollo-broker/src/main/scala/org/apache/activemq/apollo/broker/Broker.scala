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
import _root_.java.lang.{String}
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import _root_.org.fusesource.hawtdispatch.ScalaDispatchHelpers._
import org.fusesource.hawtdispatch.{Dispatch}
import org.fusesource.hawtbuf._
import AsciiBuffer._
import collection.{JavaConversions, SortedMap}
import JavaConversions._
import org.apache.activemq.apollo.dto.{VirtualHostStatusDTO, ConnectorStatusDTO, BrokerStatusDTO, BrokerDTO}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentHashMap
import org.apache.activemq.apollo.util._
import ReporterLevel._

/**
 * <p>
 * The BrokerFactory creates Broker objects from a URI.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object BrokerFactory {

  trait Provider {
    def createBroker(brokerURI:String):Broker
  }

  def discover = {
    val finder = new ClassFinder[Provider]("META-INF/services/org.apache.activemq.apollo/broker-factory.index")
    finder.new_instances
  }

  var providers = discover


  def createBroker(uri:String, start:Boolean=false):Broker = {
    if( uri == null ) {
      return null
    }
    providers.foreach { provider=>
      val broker = provider.createBroker(uri)
      if( broker!=null ) {
        if (start) {
          broker.start();
        }
        return broker
      }
    }
    throw new IllegalArgumentException("Uknonwn broker uri: "+uri)
  }
}


object BufferConversions {

  implicit def toAsciiBuffer(value:String) = new AsciiBuffer(value)
  implicit def toUTF8Buffer(value:String) = new UTF8Buffer(value)
  implicit def fromAsciiBuffer(value:AsciiBuffer) = value.toString
  implicit def fromUTF8Buffer(value:UTF8Buffer) = value.toString

  implicit def toAsciiBuffer(value:Buffer) = value.ascii
  implicit def toUTF8Buffer(value:Buffer) = value.utf8
}


/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object BrokerRegistry {

  val brokers = new ConcurrentHashMap[String, Broker]()

  def list():Seq[String] = {
    import JavaConversions._
    brokers.keys.toSeq
  }

  def get(id:String) = brokers.get(id)

  def add(id:String, broker:Broker) = brokers.put(id, broker)

  def remove(id:String) = brokers.remove(id)

}

object Broker extends Log {

  val broker_id_counter = new AtomicLong()

  val STICK_ON_THREAD_QUEUES = true


  /**
   * Creates a default a configuration object.
   */
  def defaultConfig() = {
    val rc = new BrokerDTO
    rc.id = "default"
    rc.enabled = true
    rc.notes = "A default configuration"
    rc.virtual_hosts.add(VirtualHost.defaultConfig)
    rc.connectors.add(Connector.defaultConfig)
    rc.basedir = "./activemq-data/default"
    rc
  }

  /**
   * Validates a configuration object.
   */
  def validate(config: BrokerDTO, reporter:Reporter):ReporterLevel = {
    new Reporting(reporter) {
      if( empty(config.id) ) {
        error("Broker id must be specified.")
      }
      if( config.virtual_hosts.isEmpty ) {
        error("Broker must define at least one virtual host.")
      }
      if( empty(config.basedir) ) {
        error("Broker basedir must be defined.")
      }

      for (host <- config.virtual_hosts ) {
        result |= VirtualHost.validate(host, reporter)
      }
      for (connector <- config.connectors ) {
        result |= Connector.validate(connector, reporter)
      }
    }.result
  }
}

/**
 * <p>
 * A Broker is parent object of all services assoicated with the serverside of
 * a message passing system.  It keeps track of all running connections,
 * virtual hosts and assoicated messaging destintations.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Broker() extends BaseService with DispatchLogging with LoggingReporter {
  
  import Broker._
  override protected def log = Broker

  var config: BrokerDTO = defaultConfig

  var dataDirectory: File = null
  var defaultVirtualHost: VirtualHost = null
  var virtualHosts: Map[AsciiBuffer, VirtualHost] = Map()
  var connectors: List[Connector] = Nil

  val dispatchQueue = createQueue("broker");
  if( STICK_ON_THREAD_QUEUES ) {
    dispatchQueue.setTargetQueue(Dispatch.getRandomThreadQueue)
  }

  val id = broker_id_counter.incrementAndGet
  
  val virtual_host_id_counter = new LongCounter
  val connector_id_counter = new LongCounter
  val connection_id_counter = new LongCounter

  override def toString() = "broker: "+id


  /**
   * Validates and then applies the configuration.
   */
  def configure(config: BrokerDTO, reporter:Reporter) = ^{
    if ( validate(config, reporter) < ERROR ) {
      this.config = config

      if( serviceState.isStarted ) {
        // TODO: apply changes while he broker is running.
        reporter.report(WARN, "Updating broker configuration at runtime is not yet supported.  You must restart the broker for the change to take effect.")

      }
    }
  } >>: dispatchQueue


  override def _start(onCompleted:Runnable) = {

    // create the runtime objects from the config
    {
      dataDirectory = new File(config.basedir)
      defaultVirtualHost = null
      for (c <- config.virtual_hosts) {
        val host = new VirtualHost(this, virtual_host_id_counter.incrementAndGet)
        host.configure(c, this)
        virtualHosts += ascii(c.id)-> host
        // first defined host is the default virtual host
        if( defaultVirtualHost == null ) {
          defaultVirtualHost = host
        }
      }
      for (c <- config.connectors) {
        val connector = new Connector(this, connector_id_counter.incrementAndGet)
        connector.configure(c, this)
        connectors ::= connector
      }
    }

    // Start up the virtual hosts
    val tracker = new LoggingTracker("broker startup", dispatchQueue)
    virtualHosts.valuesIterator.foreach( x=>
      tracker.start(x)
    )

    // Once virtual hosts are up.. start up the connectors.
    tracker.callback(^{
      val tracker = new LoggingTracker("broker startup", dispatchQueue)
      connectors.foreach( x=>
        tracker.start(x)
      )
      tracker.callback(onCompleted)
    })

  }


  def _stop(onCompleted:Runnable): Unit = {
    val tracker = new LoggingTracker("broker shutdown", dispatchQueue)
    // Stop accepting connections..
    connectors.foreach( x=>
      tracker.stop(x)
    )
    // Shutdown the virtual host services
    virtualHosts.valuesIterator.foreach( x=>
      tracker.stop(x)
    )
    tracker.callback(onCompleted)
  }

  def getVirtualHost(name: AsciiBuffer, cb: (VirtualHost) => Unit) = reply(cb) {
    virtualHosts.getOrElse(name, null)
  } >>: dispatchQueue

  def getDefaultVirtualHost(cb: (VirtualHost) => Unit) = reply(cb) {
    defaultVirtualHost
  } >>: dispatchQueue

}
