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

import _root_.java.io.File
import _root_.java.lang.String
import org.fusesource.hawtdispatch._
import org.fusesource.hawtbuf._
import AsciiBuffer._
import collection.JavaConversions
import JavaConversions._
import java.util.concurrent.atomic.AtomicLong
import org.apache.activemq.apollo.util._
import ReporterLevel._
import security.{AclAuthorizer, Authorizer, JaasAuthenticator, Authenticator}
import java.net.InetSocketAddress
import org.apache.activemq.apollo.broker.web._
import collection.mutable.{HashSet, LinkedHashMap, HashMap}
import scala.util.Random
import FileSupport._
import org.apache.activemq.apollo.dto.{LogCategoryDTO, BrokerDTO}

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

  val providers = new ClassFinder[Provider]("META-INF/services/org.apache.activemq.apollo/broker-factory.index",classOf[Provider])

  def createBroker(uri:String):Broker = {
    if( uri == null ) {
      return null
    }
    providers.singletons.foreach { provider=>
      val broker = provider.createBroker(uri)
      if( broker!=null ) {
        return broker;
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
object BrokerRegistry extends Log {

  private val brokers = HashSet[Broker]()

  @volatile
  private var monitor_session = 0

  def list():Array[Broker] = this.synchronized {
    brokers.toArray
  }

  def add(broker:Broker) = this.synchronized {
    val rc = brokers.add(broker)
    if(rc && brokers.size==1 && java.lang.Boolean.getBoolean("hawtdispatch.profile")) {
      // start monitoring when the first broker starts..
      monitor_session += 1
      monitor_hawtdispatch(monitor_session)
    }
    rc
  }

  def remove(broker:Broker) = this.synchronized {
    val rc = brokers.remove(broker)
    if(rc && brokers.size==0 && java.lang.Boolean.getBoolean("hawtdispatch.profile")) {
      // stomp monitoring when the last broker stops..
      monitor_session += 1
    }
    rc
  }


  def monitor_hawtdispatch(session_id:Int):Unit = {

    import collection.JavaConversions._
    import java.util.concurrent.TimeUnit._
    getGlobalQueue().after(1, SECONDS) {
      if( session_id == monitor_session ) {
        val m = Dispatch.metrics.toList.flatMap{x=>
          if( x.totalWaitTimeNS > MILLISECONDS.toNanos(10) ||  x.totalRunTimeNS > MILLISECONDS.toNanos(10) ) {
            Some(x)
          } else {
            None
          }
        }

        if( !m.isEmpty ) {
          info("-- hawtdispatch metrics -----------------------\n"+m.mkString("\n"))
        }

        monitor_hawtdispatch(session_id)
      }
    }
  }

}

object Broker extends Log {

  val BLOCKABLE_THREAD_POOL = ApolloThreadPool.INSTANCE

  def class_loader:ClassLoader = ClassFinder.class_loader

  /**
   * Creates a default a configuration object.
   */
  def defaultConfig() = {
    val rc = new BrokerDTO
    rc.notes = "A default configuration"
    rc.virtual_hosts.add(VirtualHost.default_config)
    rc.connectors.add(Connector.defaultConfig)
    rc
  }

  /**
   * Validates a configuration object.
   */
  def validate(config: BrokerDTO, reporter:Reporter):ReporterLevel = {
    new Reporting(reporter) {
      if( config.virtual_hosts.isEmpty ) {
        error("Broker must define at least one virtual host.")
      }

      for (host <- config.virtual_hosts ) {
        result |= VirtualHost.validate(host, reporter)
      }
      for (connector <- config.connectors ) {
        result |= Connector.validate(connector, reporter)
      }
      if( !config.web_admins.isEmpty ) {
        WebServerFactory.validate(config.web_admins.toList, reporter)
      }

    }.result
  }

  val version = using(getClass().getResourceAsStream("version.txt")) { source=>
    read_text(source).trim
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
class Broker() extends BaseService {

  import Broker._

  var tmp: File = _
  var config: BrokerDTO = defaultConfig

  var default_virtual_host: VirtualHost = null
  val virtual_hosts = LinkedHashMap[AsciiBuffer, VirtualHost]()
  val virtual_hosts_by_hostname = new LinkedHashMap[AsciiBuffer, VirtualHost]()

  var connectors: List[Connector] = Nil
  val connections = HashMap[Long, BrokerConnection]()

  val dispatch_queue = createQueue("broker")

  def id = "default"

  val connection_id_counter = new LongCounter

  var key_storage:KeyStorage = _

  var web_server:WebServer = _

  var audit_log:Log = _
  var security_log:Log  = _
  var connection_log:Log = _
  var console_log:Log = _
  var services = List[Service]()

  override def toString() = "broker: "+id


  /**
   * Validates and then applies the configuration.
   */
  def configure(config: BrokerDTO, reporter:Reporter) = {
    if ( validate(config, reporter) < ERROR ) {
      dispatch_queue {
        this.config = config
      }
      if( service_state.is_started ) {
        // TODO: apply changes while he broker is running.
        reporter.report(WARN, "Updating broker configuration at runtime is not yet supported.  You must restart the broker for the change to take effect.")

      }
    }
  }

  var authenticator:Authenticator = _
  var authorizer:Authorizer = _

  def init_dispatch_queue(dispatch_queue:DispatchQueue) = {
    import OptionSupport._
    if( config.sticky_dispatching.getOrElse(true) ) {
      val queues = getThreadQueues()
      val queue = queues(Random.nextInt(queues.length));
      dispatch_queue.setTargetQueue(queue)
    }
  }

  override def _start(on_completed:Runnable) = {

    // create the runtime objects from the config
    {
      import OptionSupport._
      init_dispatch_queue(dispatch_queue)

      // Configure the logging categories...
      val log_category = config.log_category.getOrElse(new LogCategoryDTO)
      val base_category = "org.apache.activemq.apollo.log."
      security_log = Log(log_category.security.getOrElse(base_category+"security"))
      audit_log = Log(log_category.audit.getOrElse(base_category+"audit"))
      connection_log = Log(log_category.connection.getOrElse(base_category+"connection"))
      console_log = Log(log_category.console.getOrElse(base_category+"console"))

      log_versions
      check_file_limit

      if( config.key_storage!=null ) {
        key_storage = new KeyStorage
        key_storage.config = config.key_storage
      }


      if( config.authentication != null && config.authentication.enabled.getOrElse(true) ) {
        authenticator = new JaasAuthenticator(config.authentication, security_log)
        authorizer = new AclAuthorizer(config.authentication.acl_principal_kinds().toList, security_log)
      }

      default_virtual_host = null
      for (c <- config.virtual_hosts) {
        val host = new VirtualHost(this, c.host_names.head)
        host.configure(c, LoggingReporter(VirtualHost))
        virtual_hosts += ascii(c.id)-> host
        // first defined host is the default virtual host
        if( default_virtual_host == null ) {
          default_virtual_host = host
        }

        // add all the host names of the virtual host to the virtual_hosts_by_hostname map..
        c.host_names.foreach { name=>
          virtual_hosts_by_hostname += ascii(name)->host
        }
      }

      for (c <- config.connectors) {
        val connector = new AcceptingConnector(this, c.id)
        connector.configure(c, LoggingReporter(VirtualHost))
        connectors ::= connector
      }


      services = (config.services.map { clazz =>
        val service = Broker.class_loader.loadClass(clazz).newInstance().asInstanceOf[Service]

        // Try to inject the broker via reflection..
        type BrokerAware = { var broker:Broker }
        try {
          service.asInstanceOf[BrokerAware].broker = this
        } catch { case _ => }

        service

      }).toList
    }

    BrokerRegistry.add(this)

    // Start up the virtual hosts
    val first_tracker = new LoggingTracker("broker startup", console_log, dispatch_queue)
    val second_tracker = new LoggingTracker("broker startup", console_log, dispatch_queue)

    if( !config.web_admins.isEmpty ) {
      WebServerFactory.create(this) match {
        case null =>
          warn("Could not start admistration interface.")
        case x =>
          web_server = x
          second_tracker.start(web_server)
      }
    }

    virtual_hosts.valuesIterator.foreach( x=>
      first_tracker.start(x)
    )

    // Once virtual hosts are up.. start up the connectors.
    first_tracker.callback{
      connectors.foreach(second_tracker.start(_))
      second_tracker.callback {
        // Once the connectors are up, start the services.
        val services_tracker = new LoggingTracker("broker startup", console_log, dispatch_queue)
        services.foreach( x=>
          first_tracker.start(x)
        )
        services_tracker.callback(on_completed)
      }
    }

  }


  def _stop(on_completed:Runnable): Unit = {
    val tracker = new LoggingTracker("broker shutdown", console_log, dispatch_queue)

    // Stop the services...
    services.foreach( x=>
      tracker.stop(x)
    )

    // Stop accepting connections..
    connectors.foreach( x=>
      tracker.stop(x)
    )

    // stop the connections..
    connections.valuesIterator.foreach { connection=>
      tracker.stop(connection)
    }

    // Shutdown the virtual host services
    virtual_hosts.valuesIterator.foreach( x=>
      tracker.stop(x)
    )

    Option(web_server).foreach(tracker.stop(_))

    BrokerRegistry.remove(this)
    tracker.callback(on_completed)
  }

  private def log_versions = {

    def capture(command:String*) = {
      import ProcessSupport._
      try {
        system(command:_*) match {
          case(0, out, _) => Some(new String(out).trim)
          case _ => None
        }
      } catch {
        case _ => None
      }
    }

    val os = {
      val os = System.getProperty("os.name")
      val rc = os +" "+System.getProperty("os.version")

      // Try to get a better version from the OS itself..
      val los = os.toLowerCase()
      if( los.startsWith("linux") ) {
        capture("lsb_release", "-sd").map("%s (%s)".format(rc, _)).getOrElse(rc)
      } else {
        rc
      }

    }

    val jvm = {
      val vendor = System.getProperty("java.vendor")
      val version =System.getProperty("java.version")
      val vm =System.getProperty("java.vm.name")
      "%s %s (%s)".format(vm, version, vendor)
    }

    val location_info = Option(System.getProperty("apollo.home")).map { home=>
      " (at: "+new File(home).getCanonicalPath+")"
    }.getOrElse("")

    console_log.info("OS     : %s", os)
    console_log.info("JVM    : %s", jvm)
    console_log.info("Apollo : %s%s", Broker.version, location_info)

  }
  private def check_file_limit:Unit = {
    if( System.getProperty("os.name").toLowerCase().startsWith("windows") ) {
      return
    }

    import ProcessSupport._
    def process(out:Array[Byte]) = try {
      val limit = new String(out).trim
      console_log.info("OS is restricting the open file limit to: %s", limit)
      if( limit!="unlimited" ) {
        val l = limit.toInt

        var min_limit = 500 // estimate.. perhaps could we do better?
        config.connectors.foreach { connector=>
          import OptionSupport._
          min_limit += connector.connection_limit.getOrElse(10000)
        }

        if( l < min_limit ) {
          console_log.warn("Please increase the process file limit using 'ulimit -n %d' or configure lower connection limits on the broker connectors.", min_limit)
        }
      }
    } catch {
      case _ =>
    }

    try {
      launch("ulimit","-n") { case (rc, out, err) =>
        if( rc==0 ) {
          process(out)
        }
      }
    } catch {
      case _ =>
        try {
          launch("sh", "-c", "ulimit -n") { case (rc, out, err) =>
            if( rc==0 ) {
              process(out)
            }
          }
        } catch {
          case _ =>
        }
    }

  }

  def get_virtual_host(name: AsciiBuffer) = dispatch_queue ! {
    virtual_hosts_by_hostname.getOrElse(name, null)
  }

  def get_default_virtual_host = dispatch_queue ! {
    default_virtual_host
  }

  //useful for testing
  def get_connect_address = {
    Option(config.client_address).getOrElse(first_accepting_connector.get.transport_server.getConnectAddress)
  }

  def get_socket_address = {
    first_accepting_connector.get.transport_server.getSocketAddress
  }

  def first_accepting_connector = connectors.find(_.isInstanceOf[AcceptingConnector]).map(_.asInstanceOf[AcceptingConnector])

}
