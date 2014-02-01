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

package org.apache.activemq.apollo.cli.osgi

import org.apache.activemq.apollo.broker.{BrokerCreate, Broker}
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.dto.{XmlCodec, BrokerDTO}
import org.osgi.framework._
import collection.JavaConversions._
import org.apache.activemq.apollo.util._
import FileSupport._
import java.util.Properties
import org.osgi.service.cm.{Configuration, ConfigurationAdmin}
import java.io._
import java.lang.String

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object BrokerService extends Log {

  var context: BundleContext = _
  var config:BrokerDTO = _
  var configAdmin:ConfigurationAdmin = _
  var broker:Broker = _

  def start(): Unit = this.synchronized {
    try {
      if(broker!=null) {
        error("Apollo has allready been started.")
        return
      }
      info("Starting Apollo.")

      val configuration: Configuration = configAdmin.getConfiguration("org.apache.activemq.apollo")
      var cmProps = configuration.getProperties

      val karaf_data: File = new File(System.getProperty("karaf.data", "."))
      var basedir: File = new File(karaf_data, "apollo")

      if( cmProps!=null ) {
        basedir = Option(cmProps.get("apollo.base").asInstanceOf[String])
                  .map(new File(_)).getOrElse(basedir)
      }

      if( !basedir.exists() ) {
        info("Initializing apollo instance directory: "+basedir)

        // Lets create a broker instance since it does not exist.
        val create = new BrokerCreate()
        create.directory = basedir
        create.home = null

        // Lets just reuse Karaf's Logging and Authentication configurations
        create.create_log_config = false
        create.create_login_config = false
        create.broker_security_config =
  """<!-- used to secure the web admin interface -->
  <authentication domain="karaf">
    <user_principal_kind>org.apache.karaf.jaas.modules.UserPrincipal</user_principal_kind>
    <acl_principal_kind>org.apache.karaf.jaas.modules.RolePrincipal</acl_principal_kind>
  </authentication>

  <acl>
    <admin allow="admin"/>
    <config allow="admin"/>
  </acl>
  """
        create.host_security_config =
    """<!-- Uncomment to disable security for the virtual host -->
    <!-- <authentication enabled="false"/> -->
    <acl>
      <admin allow="admin"/>
      <config allow="admin"/>
    </acl>
    """
        create.run(System.out, System.err)
      }

      // in case the config gets injected.
      val dto = if( config != null ) {
        config
      } else {

        // val base = system_dir("apollo.base")
        val apollo_xml = basedir / "etc" / "apollo.xml"

        if (!apollo_xml.exists) {
          error("Apollo configuration file'%s' does not exist.".format(apollo_xml))
          return
        }

        // Load the configs and start the brokers up.
        info("Loading configuration file '%s'.", apollo_xml)

        val props = new Properties()
        for( entry <- System.getenv().entrySet() ) {
          props.put("env."+entry.getKey, entry.getValue)
        }
        props.putAll(System.getProperties)
        if( cmProps!=null ) {
          cmProps.keySet.foreach { key =>
            props.put(key.asInstanceOf[String], cmProps.get(key).asInstanceOf[String])
          }
        }
        props.put("apollo.base", basedir.getCanonicalPath)
        XmlCodec.decode(classOf[BrokerDTO], new FileInputStream(apollo_xml), props)
      }

      debug("Starting broker")
      broker = new Broker()
      broker.update(dto, NOOP)
      broker.tmp = basedir / "tmp"
      broker.tmp.mkdirs
      broker.start(^{
        info("Apollo started")
      })

    } catch {
      case e: Throwable =>
        println()
        e.printStackTrace
        stop
        error(e)
    }
  }

  def stop(): Unit = this.synchronized {
    info("Stopping Apollo")
    if( broker!=null ) {
      ServiceControl.stop(broker, "Apollo shutdown")
      info("Apollo stopped")
      broker = null
    }
  }

}

import BrokerService._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class BrokerService {

  //
  // Setters to allow blueprint injection.
  //
  def setContext(value:BundleContext):Unit = context = value
  def setConfig(value:BrokerDTO):Unit = config = value
  def setConfigAdmin(value:ConfigurationAdmin):Unit = configAdmin = value

  def start() = BrokerService.start
  def stop() = BrokerService.stop
}

