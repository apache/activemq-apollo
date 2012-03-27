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
import java.util.concurrent.TimeUnit
import security.SecuredResource.BrokerKind
import reflect.BeanProperty
import java.net.InetSocketAddress

/**
 * <p>
 * The BrokerFactory creates Broker objects from a URI.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait BrokerFactoryTrait {
  def createBroker(brokerURI:String):Broker
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

  def createBroker(uri:String):Broker = {
    if( uri == null ) {
      return null
    }
    finder.singletons.foreach { provider=>
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
  private val SERVICE_TIMEOUT = 1000*5;

  def class_loader:ClassLoader = ClassFinder.class_loader

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

  val max_fd_limit = {
    if( System.getProperty("os.name").toLowerCase().startsWith("windows") ) {
      None
    } else {
      val mbean_server = ManagementFactory.getPlatformMBeanServer()
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
class Broker() extends BaseService with SecuredResource {

  import Broker._

  @BeanProperty
  var tmp: File = _

  @BeanProperty
  var config: BrokerDTO = new BrokerDTO

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

  var default_virtual_host: VirtualHost = null
  val virtual_hosts = LinkedHashMap[AsciiBuffer, VirtualHost]()
  val virtual_hosts_by_hostname = new LinkedHashMap[AsciiBuffer, VirtualHost]()

  val connectors = LinkedHashMap[String, Connector]()
  val connections = LinkedHashMap[Long, BrokerConnection]()

  val dispatch_queue = createQueue("broker")

  def id = "default"

  val connection_id_counter = new LongCounter

  var key_storage:KeyStorage = _

  var web_server:WebServer = _

  @volatile
  var now = System.currentTimeMillis()

  var config_log:Log = Log(new MemoryLogger(Broker.log))
  var audit_log:Log = Broker
  var security_log:Log  = Broker
  var connection_log:Log = Broker
  var console_log:Log = Broker
  var services = Map[String, (CustomServiceDTO, Service)]()

  override def toString() = "broker: "+id

  var authenticator:Authenticator = _
  var authorizer = Authorizer()

  def resource_kind = SecuredResource.BrokerKind

  /**
   * Validates and then applies the configuration.
   */
  def update(config: BrokerDTO, on_completed:Task) = dispatch_queue {
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
    init_logs
    log_versions
    check_file_limit

    BrokerRegistry.add(this)
    schedule_now_update
    schedule_virtualhost_maintenance

    val tracker = new LoggingTracker("broker startup", console_log, SERVICE_TIMEOUT)
    apply_update(tracker)
    tracker.callback(on_completed)

  }

  def _stop(on_completed:Task): Unit = {
    val tracker = new LoggingTracker("broker shutdown", console_log, SERVICE_TIMEOUT)

    // Stop the services...
    services.values.foreach( x=>
      tracker.stop(x._2)
    )
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

    Option(web_server).foreach(tracker.stop(_))
    web_server = null

    BrokerRegistry.remove(this)
    tracker.callback(on_completed)

  }

  def schedule_now_update:Unit = dispatch_queue.after(100, TimeUnit.MILLISECONDS) {
    if( service_state.is_starting_or_started ) {
      now = System.currentTimeMillis
      schedule_now_update
    }
  }

  def schedule_virtualhost_maintenance:Unit = dispatch_queue.after(1, TimeUnit.SECONDS) {
    if( service_state.is_started ) {
      val active_sessions = connections.values.flatMap(_.session_id).toSet

      virtual_hosts.values.foreach { host=>
        host.dispatch_queue {
          if(host.service_state.is_started) {
            host.router.remove_temp_destinations(active_sessions)
          }
        }
      }

      schedule_virtualhost_maintenance
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

    if (config.authentication != null && config.authentication.enabled.getOrElse(true)) {
      authenticator = new JaasAuthenticator(config.authentication, security_log)
      authorizer=Authorizer(this)
    } else {
      authenticator = null
      authorizer=Authorizer()
    }

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

    val t:Seq[(String, CustomServiceDTO)] = asScalaBuffer(config.services).map(x => (x.id ->x) )
    val services_config = Map(t: _*)
    diff(services.keySet, services_config.keySet) match { case (added, updated, removed) =>
      removed.foreach { id =>
        for( service <- services.get(id) ) {
          services -= id
          tracker.stop(service._2)
        }
      }

      // Handle the updates.
      added.foreach { id=>
        for( new_dto <- services_config.get(id); (old_dto, service) <- services.get(id) ) {
          if( new_dto != old_dto ) {

            // restart.. needed.
            val task = tracker.task("restart "+service)
            service.stop(dispatch_queue.runnable {

              // create with the new config..
              val service = CustomServiceFactory.create(this, new_dto)
              if( service == null ) {
                console_log.warn("Could not create service: "+new_dto.id);
                task.run()
              } else {
                // start it again..
                services += new_dto.id -> (new_dto, service)
                service.start(task)
              }
            })
          }
        }
      }

      // Create the new services..
      added.foreach { id=>
        for( dto <- services_config.get(id) ) {
          val service = CustomServiceFactory.create(this, dto)
          if( service == null ) {
            console_log.warn("Could not create service: "+dto.id);
          } else {
            services += dto.id -> (dto, service)
            tracker.start(service)
          }
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

  def get_socket_address = {
    first_accepting_connector.get.socket_address
  }

  def first_accepting_connector = connectors.values.find(_.isInstanceOf[AcceptingConnector]).map(_.asInstanceOf[AcceptingConnector])

}
