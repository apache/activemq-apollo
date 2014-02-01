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
package org.apache.activemq.apollo.cli.commands

import io.airlift.command.Command
import org.apache.activemq.apollo.broker.{Broker, ConfigStore}
import org.apache.activemq.apollo.util.FileSupport._
import org.apache.activemq.apollo.cli.Apollo
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.util.{FileMonitor, ServiceControl}
import org.apache.log4j.PropertyConfigurator
import java.io.{PrintStream, InputStream, FileInputStream, File}
import java.util.logging.LogManager
import scala.collection.mutable.ListBuffer
import java.lang.Thread.UncaughtExceptionHandler
import java.security.{Security, Provider}
import scala._

/**
 * The apollo run command
 */
@Command(name = "run", description = "runs the broker instance")
class Run extends Action {

  def system_dir(name:String) = {
    val base_value = System.getProperty(name)
    if( base_value==null ) {
      sys.error("The the %s system property is not set.".format(name))
    }
    val file = new File(base_value)
    if( !file.isDirectory  ) {
      sys.error("The the %s system property is not set to valid directory path %s".format(name, base_value))
    }
    file
  }

  def execute(in: InputStream, out: PrintStream, err: PrintStream): Int = {
    try {

      // 
      // Install an UncaughtExceptionHandler so that all exceptions get properly logged.
      val exception_handler = new UncaughtExceptionHandler {
        def uncaughtException(t: Thread, error: Throwable) {
          Broker.warn(error)
        }
      }
      Thread.setDefaultUncaughtExceptionHandler(exception_handler)
      getGlobalQueue().sync(Thread.currentThread().setUncaughtExceptionHandler(exception_handler))
      
      val base = system_dir("apollo.base")
      val etc: File = base / "etc"

      val log4j_config = etc / "log4j.properties"
      PropertyConfigurator.configure(log4j_config.getCanonicalPath)

      val conf = etc / "apollo.xml"

      if( !conf.exists ) {
        sys.error("Configuration file'%s' does not exist.\n\nTry creating a broker instance using the 'apollo create' command.".format(conf));
      }

      if( System.getProperty("java.security.auth.login.config")==null ) {
        val login_config = etc / "login.config"
        if( login_config.exists ) {
          System.setProperty("java.security.auth.login.config", login_config.getCanonicalPath)
        }
      }

      val tmp = base / "tmp"
      tmp.mkdirs

      Apollo.print_banner(out)

      // Load the configs and start the brokers up.
      out.println("Loading configuration file '%s'.".format(conf))

      // Use bouncycastle if it's installed.
      try {
        var loader: ClassLoader = getClass.getClassLoader
        var clazz: Class[_] = loader.loadClass("org.bouncycastle.jce.provider.BouncyCastleProvider")
        val bouncycastle_provider = clazz.newInstance().asInstanceOf[Provider]
        Security.insertProviderAt(bouncycastle_provider, 2)
        Broker.info("Loaded the Bouncy Castle security provider.")
      } catch {
        case e:Throwable => // ignore, we can live without bouncycastle
      }

      val broker = new Broker()

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
        return 1
      }

      broker.tmp = tmp
      broker.start(NOOP)

      val broker_config_monitor = new FileMonitor(conf,broker.dispatch_queue {
        broker.console_log.info("Reloading configuration file '%s'.".format(conf))
        broker.update(ConfigStore.load(conf, x=> broker.console_log.info(x) ), ^{
        })
      })
      val log4j_config_monitor = new FileMonitor(log4j_config, {
        PropertyConfigurator.configure(log4j_config.getCanonicalPath)
      })

      var jul_config = etc / "jul.properties"
      val jul_config_monitor = if ( jul_config.exists()) {
        new FileMonitor(jul_config, {
          using(new FileInputStream(jul_config)) { is =>
            LogManager.getLogManager.readConfiguration(is)
          }
        })
      } else {
        null
      }
      
      if(jul_config_monitor!=null) jul_config_monitor.start(NOOP)
      log4j_config_monitor.start(NOOP)
      broker_config_monitor.start(NOOP)

      Runtime.getRuntime.addShutdownHook(new Thread(){
        override def run: Unit = {
          var services = List(log4j_config_monitor, broker_config_monitor, broker)
          if(jul_config_monitor!=null)
            services ::= jul_config_monitor
          ServiceControl.stop(services, "stopping broker")
        }
      })

      // wait forever...  broker will system exit.
      this.synchronized {
        this.wait
      }
      0
    } catch {
      case x:Helper.Failure=>
        err.print("Startup failed: "+x.getMessage)
        1
      case x:Throwable=>
        err.print("Startup failed: "+x)
        1
    }
  }

}
