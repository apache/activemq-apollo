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
package org.apache.activemq.apollo.web

import javax.servlet._
import java.io.File
import org.apache.activemq.apollo.util.FileSupport._
import java.security.{Security, Provider}
import org.apache.activemq.apollo.broker.{BrokerCreate, ConfigStore, Broker}
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.util.{Service, ServiceControl, FileMonitor}
import scala.collection.mutable.ListBuffer
import org.apache.log4j.PropertyConfigurator

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ApolloApplication extends Filter {

  var broker_services = List[Service]()
  var broker:Broker = _

  def init(filterConfig: FilterConfig):Unit = {
    val sc = filterConfig.getServletContext
    val webapp_basedir = new File(sc.getRealPath("/"))

    val base = webapp_basedir.getParentFile / (webapp_basedir.getName + "-broker")

    val etc = base / "etc"
    val conf = etc / "apollo.xml"

    // If the instance data has not yet be initialized.. lets initialize it
    if( !conf.exists() ) {
      val create = new BrokerCreate
      create.directory = base
      create.base = base
      create.broker_security_config = "<jmx admin_url='"+sc.getContextPath+"'/>"
      create.host_security_config = ""
      create.home = null
      println("Generating broker instance directory at: "+create.base)
      create.run(System.out, System.err)
    }

    if( !conf.exists ) {
      sys.error("Could not create the broker instance.");
    } else {
      val log4j_config = etc / "log4j.properties"
      PropertyConfigurator.configure(log4j_config.getCanonicalPath)

  //    if( System.getProperty("java.security.auth.login.config")==null ) {
  //      val login_config = etc / "login.config"
  //      if( login_config.exists ) {
  //        System.setProperty("java.security.auth.login.config", login_config.getCanonicalPath)
  //      }
  //    }

      val tmp = base / "tmp"
      tmp.mkdirs

      // Load the configs and start the brokers up.
      println("Loading configuration file '%s'.".format(conf))

      // Use bouncycastle if it's installed.
      try {
        var loader = this.getClass.getClassLoader
        var clazz = loader.loadClass("org.bouncycastle.jce.provider.BouncyCastleProvider")
        val bouncycastle_provider = clazz.newInstance().asInstanceOf[Provider]
        Security.insertProviderAt(bouncycastle_provider, 2)
        Broker.info("Loaded the Bouncy Castle security provider.")
      } catch {
        case e:Throwable => // ignore, we can live without bouncycastle
      }

      broker = new Broker()
      broker.container = sc;

      val validation_messages = ListBuffer[String]()
      try {
        broker.config = ConfigStore.load(conf, validation_messages += _)
      } finally {
        if( !validation_messages.isEmpty && broker.config.validation != "hide") {
          Broker.warn("")
          Broker.warn("Broker configuration file failed the following validations:")
          validation_messages.foreach{ v =>
            Broker.warn("")
            Broker.warn("  "+v)
          }
          Broker.warn("")
        }
      }

      if( broker.config.validation == "strict" && !validation_messages.isEmpty) {
        Broker.error("Strict validation was configured, shutting down")
        return
      }

      broker.tmp = tmp
      ServiceControl.start(broker)

      val broker_config_monitor = new FileMonitor(conf,broker.dispatch_queue {
        broker.console_log.info("Reloading configuration file '%s'.".format(conf))
        broker.update(ConfigStore.load(conf, x=> broker.console_log.info(x) ), ^{
        })
      })

      val log4j_config_monitor = new FileMonitor(log4j_config, {
        PropertyConfigurator.configure(log4j_config.getCanonicalPath)
      })
  //
  //    var jul_config = etc / "jul.properties"
  //    val jul_config_monitor = if ( jul_config.exists()) {
  //      new FileMonitor(jul_config, {
  //        using(new FileInputStream(jul_config)) { is =>
  //          LogManager.getLogManager.readConfiguration(is)
  //        }
  //      })
  //    } else {
  //      null
  //    }
  //
  //    if(jul_config_monitor!=null) jul_config_monitor.start(NOOP)

      log4j_config_monitor.start(NOOP)
      broker_config_monitor.start(NOOP)
      broker_services = List(log4j_config_monitor, broker_config_monitor, broker)
    }
  }

  def destroy():Unit = {
    ServiceControl.stop(broker_services, "stopping broker")
  }

  def doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain):Unit = {
    request.setAttribute("APOLLO_BROKER", broker);
    chain.doFilter(request, response)
  }

}