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

import org.apache.felix.gogo.commands.{Action, Option => option, Argument => argument, Command => command}
import org.osgi.service.command.CommandSession
import java.io.File
import org.fusesource.jansi.Ansi
import org.fusesource.jansi.Ansi.Color._
import org.fusesource.jansi.Ansi.Attribute._

import org.apache.commons.logging.LogFactory
import org.apache.activemq.apollo.broker.{BrokerRegistry, Broker, ConfigStore, FileConfigStore}
import org.fusesource.hawtdispatch._
import Helper._
import org.apache.activemq.apollo.util.FileSupport._
import org.apache.activemq.apollo.util.OptionSupport._
import org.apache.activemq.apollo.cli.Apollo
import org.eclipse.jetty.server.{Connector, Handler, Server}
import org.eclipse.jetty.security._
import authentication.BasicAuthenticator
import org.eclipse.jetty.webapp.WebAppContext
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.plus.jaas.JAASLoginService
import org.eclipse.jetty.server.handler.HandlerCollection
import org.apache.activemq.apollo.util.{Logging, ServiceControl}
import org.apache.activemq.apollo.dto.{WebAdminDTO, PrincipalDTO}

/**
 * The apollo create command
 */
@command(scope="apollo", name = "run", description = "runs the broker instance")
class Run extends Action with Logging {

  @option(name = "--conf", description = "The Apollo configuration file.")
  var conf: File = _

  @option(name = "--tmp", description = "A temp directory.")
  var tmp: File = _

  def execute(session: CommandSession):AnyRef = {

    try {

      val base = system_dir("apollo.base")

      if( conf == null ) {
        conf = base / "etc" / "apollo.xml"
      }

      if( !conf.exists ) {
        error("Configuration file'%s' does not exist.\n\nTry creating a broker instance using the 'apollo create' command.".format(conf));
      }

      val login_config = conf.getParentFile / "login.config"
      if( login_config.exists ) {
        System.setProperty("java.security.auth.login.config", login_config.getCanonicalPath)
      }

      val webapp = {
        val x = System.getProperty("apollo.webapp")
        if( x != null ) {
          new File(x)
        } else {
          val home = system_dir("apollo.home")
          val lib = home / "lib"
          lib / lib.list.find( _.matches("""apollo-web-.+-slim.war""")).getOrElse(throw new Failure("war file not found.") )
        }
      }

      if( tmp == null ) {
        tmp = base / "tmp"
        tmp.mkdirs
      }

      Apollo.print_banner(session.getConsole)

      // Load the configs and start the brokers up.
      info("Loading configuration file '%s'.", conf);
      val store = new FileConfigStore
      store.file = conf
      ConfigStore() = store
      store.start
      val config = store.load(true)

      // Only start the broker up if it's enabled..
      if( config.enabled.getOrElse(true) ) {
        debug("Starting broker '%s'", config.id);
        val broker = new Broker()
        broker.config = config
        BrokerRegistry.add(config.id, broker)
        broker.start(^{
          info("Broker '%s' started", config.id);
        })
      }


      val web_admin = config.web_admin.getOrElse(new WebAdminDTO)
      if( web_admin.enabled.getOrElse(true) ) {

        val prefix = web_admin.prefix.getOrElse("/")
        val port = web_admin.port.getOrElse(8080)
        val host = web_admin.host.getOrElse("127.0.0.1")

        // Start up the admin interface...
        debug("Starting administration interface");

        System.setProperty("scalate.workdir", (tmp / "scalate").getCanonicalPath )

        var connector = new SelectChannelConnector
        connector.setHost(host)
        connector.setPort(port)


        def admin_app = {
          var app_context = new WebAppContext
          app_context.setContextPath(prefix)
          app_context.setWar(webapp.getCanonicalPath)
          app_context.setTempDirectory(tmp)
          app_context
        }

        def secured(handler:Handler) = {
          if( config.authentication!=null && config.acl!=null ) {
            import collection.JavaConversions._

            val security_handler = new ConstraintSecurityHandler
            val login_service = new JAASLoginService(config.authentication.domain)
            val role_class_names:List[String] = config.authentication.acl_principal_kinds().toList

            login_service.setRoleClassNames(role_class_names.toArray)
            security_handler.setLoginService(login_service)
            security_handler.setIdentityService(new DefaultIdentityService)
            security_handler.setAuthenticator(new BasicAuthenticator)

            val cm = new ConstraintMapping
            val c = new org.eclipse.jetty.http.security.Constraint()
            c.setName("BASIC")
            val admins:Set[PrincipalDTO] = config.acl.admins.toSet
            c.setRoles(admins.map(_.name).toArray)
            c.setAuthenticate(true)
            cm.setConstraint(c)
            cm.setPathSpec("/*")
            cm.setMethod("GET")
            security_handler.addConstraintMapping(cm)

            security_handler.setHandler(handler)
            security_handler
          } else {
            handler
          }
        }

        var server = new Server
        server.setHandler(secured(admin_app))
        server.setConnectors(Array[Connector](connector))
        server.start

        val localPort = connector.getLocalPort
        def url = "http://"+host+":" + localPort + prefix
        info("Administration interface available at: "+bold(url))

      }


      if(java.lang.Boolean.getBoolean("hawtdispatch.profile")) {
        monitor_hawtdispatch
      }

    } catch {
      case x:Helper.Failure=>
        error(x.getMessage)
    }
    null
  }


  def monitor_hawtdispatch = {
    new Thread("HawtDispatch Monitor") {
      setDaemon(true);
      override def run = {
        while(true) {
          Thread.sleep(1000);
          import collection.JavaConversions._

          // Only display is we see some long wait or run times..
          val m = Dispatch.metrics.toList.flatMap{x=>
            if( x.totalWaitTimeNS > 1000000 ||  x.totalRunTimeNS > 1000000 ) {
              Some(x)
            } else {
              None
            }
          }

          if( !m.isEmpty ) {
            info("-- hawtdispatch metrics -----------------------\n"+m.mkString("\n"))
          }
        }
      }
    }.start();
  }

}
