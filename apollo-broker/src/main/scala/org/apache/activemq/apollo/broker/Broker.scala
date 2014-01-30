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
import collection.JavaConversions
import JavaConversions._
import security._
import org.apache.activemq.apollo.broker.web._
import collection.mutable.{HashSet, LinkedHashMap, HashMap}
import org.apache.activemq.apollo.util._
import org.fusesource.hawtbuf.AsciiBuffer._
import CollectionsSupport._
import FileSupport._
import management.ManagementFactory
import org.apache.activemq.apollo.dto._
import javax.management.ObjectName
import org.fusesource.hawtdispatch.TaskTracker._
import java.util.concurrent.TimeUnit._
import reflect.BeanProperty
import java.net.InetSocketAddress
import org.fusesource.hawtdispatch.util.BufferPools
import org.apache.activemq.apollo.filter.{Filterable, XPathExpression, XalanXPathEvaluator}
import org.xml.sax.InputSource
import java.util
import javax.management.openmbean.CompositeData
import javax.net.ssl.SSLContext
import util.Properties
import language.implicitConversions

/**
 * <p>
 * The BrokerFactory creates Broker objects from a URI.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait BrokerFactoryTrait {
  def createBroker(brokerURI:String, props:util.Properties): Broker
}

/**
 * <p>
 * The BrokerFactory creates Broker objects from a URI.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object BrokerFactory {

  val finder = new ClassFinder[BrokerFactoryTrait]("META-INF/services/org.apache.activemq.apollo/broker-factory.index",classOf[BrokerFactoryTrait])

  def createBroker(brokerURI:String):Broker = {
    val props = new Properties
    for( entry <- System.getenv().entrySet() ) {
      props.put("env."+entry.getKey, entry.getValue)
    }
    props.putAll(System.getProperties)
    createBroker(brokerURI, props)
  }

  def createBroker(uri:String, props:util.Properties):Broker = {
    if( uri == null ) {
      return null
    }
    finder.singletons.foreach { provider=>
      val broker = provider.createBroker(uri, props)
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

  val mbean_server = ManagementFactory.getPlatformMBeanServer()

  val MAX_JVM_HEAP_SIZE = try {
    val data = mbean_server.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage").asInstanceOf[CompositeData]
    data.get("max").asInstanceOf[java.lang.Long].longValue()
  } catch {
    case _:Throwable => 1024L * 1024 * 1024 // assume it's 1 GIG (that's the default apollo ships with)
  }

  val BLOCKABLE_THREAD_POOL = ApolloThreadPool.INSTANCE
  private val SERVICE_TIMEOUT = 1000*5;
  val buffer_pools = new BufferPools

  // Make sure XPATH selector support is enabled and optimize a little.
  XPathExpression.XPATH_EVALUATOR_FACTORY = new XPathExpression.XPathEvaluatorFactory {
    def create(xpath: String): XPathExpression.XPathEvaluator = {
      new XalanXPathEvaluator(xpath) {
        override def evaluate(m: Filterable): Boolean = {
          val body: Buffer = m.getBodyAs(classOf[Buffer])
          if (body != null) {
            evaluate(new InputSource(new BufferInputStream(body)))
          } else {
            super.evaluate(m)
          }
        }
      }
    }
  }

  def class_loader:ClassLoader = ClassFinder.class_loader
  
  @volatile
  var now = System.currentTimeMillis()

  val version = using(getClass().getResourceAsStream("version.txt")) { source=>
    read_text(source).trim
  }

  def capture(command:String*) = {
    import ProcessSupport._
    try {
      system(command:_*) match {
        case(0, out, _) => Some(new String(out).trim)
        case _ => None
      }
    } catch {
      case _:Throwable => None
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

  val max_fd_limit = {
    if( System.getProperty("os.name").toLowerCase().startsWith("windows") ) {
      None
    } else {
      mbean_server.getAttribute(new ObjectName("java.lang:type=OperatingSystem"), "MaxFileDescriptorCount") match {
        case x:java.lang.Long=> Some(x.longValue)
        case _ => None
      }
    }
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
class Broker() extends BaseService with SecuredResource with PluginStateSupport {

  import Broker._

  @BeanProperty
  var tmp: File = _

  @BeanProperty
  var config: BrokerDTO = new BrokerDTO

  /**
   * A reference to a container that created the broker.
   * This might something like an ServletContext or an OSGi service factory.
   */
  var container:AnyRef = _

  config.virtual_hosts.add({
    val rc = new VirtualHostDTO
    rc.id = "default"
    rc.host_names.add("localhost")
    rc
  })
  config.connectors.add({
    val rc = new AcceptingConnectorDTO()
    rc.id = "default"
    rc.bind = "tcp://0.0.0.0:0"
    rc
  })

  @volatile
  var default_virtual_host: VirtualHost = null
  val virtual_hosts = LinkedHashMap[AsciiBuffer, VirtualHost]()
  val virtual_hosts_by_hostname = new LinkedHashMap[AsciiBuffer, VirtualHost]()

  /**
   * This is a copy of the virtual_hosts_by_hostname variable which
   * can be accessed by any thread.
   */
  @volatile
  var cow_virtual_hosts_by_hostname = Map[AsciiBuffer, VirtualHost]()

  val connectors = LinkedHashMap[String, Connector]()
  val connections = LinkedHashMap[Long, BrokerConnection]()

  // Each period is 1 second long..
  object PeriodStat {
    def apply(values:Seq[PeriodStat]) = {
      val rc = new PeriodStat
      for( s <- values ) {
        rc.max_connections = rc.max_connections.max(s.max_connections)
      }
      rc
    }
  }

  class PeriodStat {
    // yeah just tracking max connections for now.. but we might add more later.
    var max_connections = 0
  }

  var current_period:PeriodStat = new PeriodStat
  val stats_of_5min = new CircularBuffer[PeriodStat](5*60)  // collects 5 min stats.
  var max_connections_in_5min = 0
  var auto_tuned_send_receiver_buffer_size = 64*1024

  val dispatch_queue = createQueue("broker")

  var id = "default"

  val connection_id_counter = new LongCounter

  var key_storage:KeyStorage = _

  var web_server:WebServer = _

  @volatile
  def now = Broker.now

  var config_log:Log = Log(new MemoryLogger(Broker.log))
  var audit_log:Log = Broker
  var security_log:Log  = Broker
  var connection_log:Log = Broker
  var console_log:Log = Broker
  var services = Map[CustomServiceDTO, Service]()

  override def toString() = "broker: "+id

  var authenticator:Authenticator = _
  var authorizer = Authorizer()

  def resource_kind = SecuredResource.BrokerKind

  // Also provide Runnable based interfaces so that it's easier to use from Java.
  def update(config: BrokerDTO, on_completed:Runnable):Unit = update(config, new TaskWrapper(on_completed))
  def start(on_completed:Runnable):Unit = super.start(new TaskWrapper(on_completed))
  def stop(on_completed:Runnable):Unit = super.stop(new TaskWrapper(on_completed))
  override def start(on_completed:Task):Unit = super.start(on_completed)
  override def stop(on_completed:Task):Unit = super.stop(on_completed)

  /**
   * Validates and then applies the configuration.
   */
  def update(config: BrokerDTO, on_completed:Task):Unit = dispatch_queue {
    dispatch_queue.assertExecuting()
    this.config = config

    val tracker = new LoggingTracker("broker reconfiguration", console_log, SERVICE_TIMEOUT)
    if( service_state.is_started ) {
      apply_update(tracker)
    }
    tracker.callback(on_completed)
  }

  override def _start(on_completed:Task) = {

    // create the runtime objects from the config
    this.id = Option(config.id).getOrElse("default")
    init_logs
    log_versions
    check_file_limit

    BrokerRegistry.add(this)
    schedule_reoccurring(100, MILLISECONDS) {
      Broker.now = System.currentTimeMillis
    }
    schedule_reoccurring(1, SECONDS) {
      virtualhost_maintenance
      roll_current_period
      tune_send_receive_buffers
    }

    val tracker = new LoggingTracker("broker startup", console_log, SERVICE_TIMEOUT)
    apply_update(tracker)
    tracker.callback(on_completed)

  }

  def _stop(on_completed:Task): Unit = {
    val tracker = new LoggingTracker("broker shutdown", console_log, SERVICE_TIMEOUT)

    // Stop the services...
    services.values.foreach(tracker.stop(_))
    services = Map()

    // Stop accepting connections..
    connectors.values.foreach( x=>
      tracker.stop(x)
    )
    connectors.clear()

    // stop the connections..
    connections.valuesIterator.foreach { connection=>
      tracker.stop(connection)
    }
    connections.clear()

    // Shutdown the virtual host services
    virtual_hosts.valuesIterator.foreach( x=>
      tracker.stop(x)
    )
    virtual_hosts.clear()
    virtual_hosts_by_hostname.clear()
    cow_virtual_hosts_by_hostname = Map()

    Option(web_server).foreach(tracker.stop(_))
    web_server = null

    BrokerRegistry.remove(this)
    tracker.callback(on_completed)

  }

  def roll_current_period = {
    stats_of_5min += current_period
    current_period = new PeriodStat
    current_period.max_connections = connections.size
    max_connections_in_5min = PeriodStat(stats_of_5min).max_connections
  }

  def tune_send_receive_buffers = {

    max_connections_in_5min = max_connections_in_5min.max(current_period.max_connections)
    if ( max_connections_in_5min == 0 ) {
      auto_tuned_send_receiver_buffer_size = 64*1024
    } else {
      // We start with the JVM heap.
      var x = MAX_JVM_HEAP_SIZE
      // Lets only use 1/8th of the heap for connection buffers.
      x = x / 8
      // 1/2 for send buffers, the other 1/2 for receive buffers.
      x = x / 2
      // Ok, how much space can we use per connection?
      x = x / max_connections_in_5min
      // Drop the bottom bits so that we are working /w 1k increments.
      x = x & 0xFFFFFF00

      // Constrain the result to be between a 2k and a 64k buffer.
      auto_tuned_send_receiver_buffer_size = x.toInt.max(1024*2).min(1024*64)
    }

    // Basically this means that we will use a 64k send/receive buffer
    // for the first 1024 connections established and then the buffer
    // size will start getting reduced down until it gets to 2k buffers.
    // Which will occur when you get to about 32,000 connections.

    for( connector <- connectors.values ) {
      connector.update_buffer_settings
    }
  }

  def virtualhost_maintenance = {
    val active_sessions = connections.values.flatMap(_.session_id).toSet
    virtual_hosts.values.foreach { host=>
      host.dispatch_queue {
        if(host.service_state.is_started) {
          host.router.remove_temp_destinations(active_sessions)
        }
      }
    }
  }

  protected def init_logs = {
    import OptionSupport._
    // Configure the logging categories...
    val log_category = config.log_category.getOrElse(new LogCategoryDTO)
    val base_category = "org.apache.activemq.apollo.log."
    security_log = Log(log_category.security.getOrElse(base_category + "security"))
    audit_log = Log(log_category.audit.getOrElse(base_category + "audit"))
    connection_log = Log(log_category.connection.getOrElse(base_category + "connection"))
    console_log = Log(log_category.console.getOrElse(base_category + "console"))
  }

  protected def apply_update(tracker:LoggingTracker) {

    import OptionSupport._
    init_logs

    key_storage = if (config.key_storage != null) {
      new KeyStorage(config.key_storage)
    } else {
      null
    }

    SecurityFactory.install(this)

    val host_config_by_id = HashMap[AsciiBuffer, VirtualHostDTO]()
    config.virtual_hosts.foreach{ value =>
      host_config_by_id += ascii(value.id) -> value
    }

    diff(virtual_hosts.keySet.toSet, host_config_by_id.keySet.toSet) match { case (added, updated, removed) =>
      removed.foreach { id =>
        for( host <- virtual_hosts.remove(id) ) {
          host.config.host_names.foreach { name =>
            virtual_hosts_by_hostname.remove(ascii(name))
          }
          tracker.stop(host)
        }
      }

      updated.foreach { id=>
        for( host <- virtual_hosts.get(id); config <- host_config_by_id.get(id) ) {

          host.config.host_names.foreach { name =>
            virtual_hosts_by_hostname.remove(ascii(name))
          }

          if( host.config.getClass == config.getClass ) {
            host.update(config, tracker.task("update: "+host))
            config.host_names.foreach { name =>
              virtual_hosts_by_hostname += ascii(name) -> host
            }
          } else {
            // The dto type changed.. so we have to re-create
            val on_completed = tracker.task("recreate virtual host: "+id)
            host.stop(^{
              val host = VirtualHostFactory.create(this, config)
              if( host == null ) {
                console_log.warn("Could not create virtual host: "+config.id);
                on_completed.run()
              } else {
                config.host_names.foreach { name =>
                  virtual_hosts_by_hostname += ascii(name) -> host
                }
                host.start(on_completed)
              }
            })

          }
        }
      }

      added.foreach { id=>
        for( config <- host_config_by_id.get(id) ) {
          val host = VirtualHostFactory.create(this, config)
          if( host == null ) {
            console_log.warn("Could not create virtual host: "+config.id);
          } else {
            virtual_hosts += ascii(config.id) -> host
            // add all the host names of the virtual host to the virtual_hosts_by_hostname map..
            config.host_names.foreach { name =>
              virtual_hosts_by_hostname += ascii(name) -> host
            }
            tracker.start(host)
          }
        }
      }
    }

    cow_virtual_hosts_by_hostname = virtual_hosts_by_hostname.toMap

    // first defined host is the default virtual host
    config.virtual_hosts.headOption.map(x=>ascii(x.id)).foreach { id =>
      default_virtual_host = virtual_hosts.get(id).getOrElse(null)
    }


    val connector_config_by_id = LinkedHashMap[String, ConnectorTypeDTO]()
    config.connectors.foreach{ value =>
      connector_config_by_id += value.id -> value
    }

    diff(connectors.keySet.toSet, connector_config_by_id.keySet.toSet) match { case (added, updated, removed) =>

      removed.foreach { id =>
        for( connector <- connectors.remove(id) ) {
          tracker.stop(connector)
        }
      }

      updated.foreach { id=>
        for( connector <- connectors.get(id); config <- connector_config_by_id.get(id) ) {
          if( connector.config.getClass == config.getClass ) {
            connector.update(config,  tracker.task("update: "+connector))
          } else {
            // The dto type changed.. so we have to re-create the connector.
            val on_completed = tracker.task("recreate connector: "+id)
            connector.stop(^{
              val connector = ConnectorFactory.create(this, config)
              if( connector == null ) {
                console_log.warn("Could not create connector: "+config.id);
                on_completed.run()
              } else {
                connectors += config.id -> connector
                connector.start(on_completed)
              }
            })
          }
        }
      }

      added.foreach { id=>
        for( config <- connector_config_by_id.get(id) ) {
          val connector = ConnectorFactory.create(this, config)
          if( connector == null ) {
            console_log.warn("Could not create connector: "+config.id);
          } else {
            connectors += config.id -> connector
            tracker.start(connector)
          }
        }
      }
    }

    val services_config = asScalaBuffer(config.services).toSet
    diff(services.keySet, services_config) match { case (added, updated, removed) =>

      removed.foreach { service_config =>
        for( service <- services.get(service_config) ) {
          services -= service_config
          tracker.stop(service)
        }
      }

      // Create the new services..
      added.foreach { service_config =>
        val service = CustomServiceFactory.create(this, service_config)
        if( service == null ) {
          console_log.warn("Could not create service: "+service_config);
        } else {
          services += service_config -> service
          tracker.start(service)
        }
      }
    }

    if( !config.web_admins.isEmpty ) {
      if ( web_server!=null ) {
        web_server.update(tracker.task("update: "+web_server))
      } else {
        web_server = WebServerFactory.create(this)
        if (web_server==null) {
          warn("Could not start admistration interface.")
        } else {
          tracker.start(web_server)
        }
      }
    } else {
      if( web_server!=null ) {
        tracker.stop(web_server)
        web_server = null
      }
    }


  }

  def web_admin_url:String = {
    if( web_server == null ) {
      null
    } else {
      web_server.uris().headOption.map(_.toString.stripSuffix("/")).getOrElse(null)
    }
  }

  private def log_versions = {
    val location_info = Option(System.getProperty("apollo.home")).map { home=>
      " (at: "+new File(home).getCanonicalPath+")"
    }.getOrElse("")

    console_log.info("OS     : %s", os)
    console_log.info("JVM    : %s", jvm)
    console_log.info("Apollo : %s%s", Broker.version, location_info)
  }

  private def check_file_limit:Unit = {
    max_fd_limit match {
      case Some(limit) =>
        console_log.info("OS is restricting the open file limit to: %s", limit)
        var min_limit = 500 // estimate.. perhaps could we do better?
        config.connectors.foreach { connector=>
          import OptionSupport._
          min_limit += connector.connection_limit.getOrElse(10000)
        }
        if( limit < min_limit ) {
          console_log.warn("Please increase the process file limit using 'ulimit -n %d' or configure lower connection limits on the broker connectors.", min_limit)
        }
      case None =>
    }
  }

  def get_virtual_host(name: AsciiBuffer) = {
    dispatch_queue.assertExecuting()
    virtual_hosts_by_hostname.getOrElse(name, null)
  }

  def get_default_virtual_host = {
    dispatch_queue.assertExecuting()
    default_virtual_host
  }

  //useful for testing
  def get_connect_address = {
    Option(config.client_address).getOrElse {
      val address= get_socket_address.asInstanceOf[InetSocketAddress]
      "%s:%d".format(address.getHostName, address.getPort)
    }
  }

  def get_socket_address = first_accepting_connector.get.socket_address
  def first_accepting_connector = connectors.values.find(_.isInstanceOf[AcceptingConnector]).map(_.asInstanceOf[AcceptingConnector])

  def get_socket_address(id:String) = accepting_connector(id).get.socket_address
  def accepting_connector(id:String) = {
    connectors.values.find( _ match {
      case connector:AcceptingConnector => connector.id == id
      case _ => false
    }).map(_.asInstanceOf[AcceptingConnector])
  }

  def ssl_context(protocol:String) = {
    val rc = SSLContext.getInstance(protocol);
    if( key_storage!=null ) {
      rc.init(key_storage.create_key_managers, key_storage.create_trust_managers, null);
    } else {
      rc.init(null, null, null);
    }
    rc
  }

}
