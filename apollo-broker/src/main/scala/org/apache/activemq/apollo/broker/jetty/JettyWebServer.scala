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
package org.apache.activemq.apollo.broker.jetty

import org.eclipse.jetty.server.{Connector, Handler, Server}
import org.apache.activemq.apollo.broker.Broker
import org.eclipse.jetty.webapp.WebAppContext
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.apache.activemq.apollo.util._
import org.fusesource.hawtdispatch._
import java.io.File
import java.lang.String
import org.apache.activemq.apollo.broker.web.{WebServer, WebServerFactory}
import java.net.URI
import org.eclipse.jetty.server.handler.HandlerList
import collection.mutable.HashMap
import org.eclipse.jetty.server.ssl.SslSelectChannelConnector
import javax.net.ssl.SSLContext
import org.eclipse.jetty.util.thread.ExecutorThreadPool
import org.apache.activemq.apollo.dto.WebAdminDTO

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object JettyWebServerFactory extends WebServerFactory.Provider {

  // Enabled this factory if we can load the jetty classes.
  val enabled = try {
    getClass.getClassLoader.loadClass(classOf[WebAppContext].getName)
    true
  } catch {
    case _ =>
    false
  }

  def create(broker:Broker): WebServer = {
    if( !enabled ) {
      return null
    }
    if( JettyWebServer.webapp==null ) {
      JettyWebServer.warn("The apollo.home or apollo.webapp system property must be set so that the webconsole can be started.")
      return null
    }
    new JettyWebServer(broker)
  }

}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object JettyWebServer extends Log {


  val webapp = {
    import FileSupport._

    var rc:File = null

    Option(System.getProperty("apollo.webapp")).foreach{ x=>
      rc = new File(x)
    }

    if( rc==null ) {
      Option(System.getProperty("apollo.home")).foreach { home=>
        val lib = new File(home) / "lib"
        rc = lib.list.find( _.matches("""apollo-web-.+-slim.war""")).map(lib / _).getOrElse(null)
      }
    }

    // the war might be on the classpath..
    if( rc==null ) {
      var url = JettyWebServer.getClass.getClassLoader.getResource("META-INF/apollo-web.txt")

      rc = if(url==null) {
        null
      } else {
        if( url.getProtocol == "jar") {

          // we are probably being run from a maven build..
          url = new java.net.URL( url.getFile.stripSuffix("!/META-INF/apollo-web.txt") )
          val jar = new File( url.getFile )
          if( jar.isFile ) {
            jar.getParentFile / (jar.getName.stripSuffix(".jar")+".war")
          } else {
            null
          }

        } else if( url.getProtocol == "file") {

          // we are probably being run from an IDE...
          val rc = new File( url.getFile.stripSuffix("/META-INF/apollo-web.txt") )
          if( rc.isDirectory ) {
            rc/".."/".."/"src"/"main"/"webapp"
          } else {
            null
          }
        } else {
          null
        }

      }
    }
    rc
  }

}

class JettyWebServer(val broker:Broker) extends WebServer with BaseService {
  import JettyWebServer._

  var server:Server = _

  override def toString: String = "jetty webserver"

  val dispatch_queue = createQueue()
  var web_admins = List[WebAdminDTO]()

  protected def _start(on_completed: Runnable) = Broker.BLOCKABLE_THREAD_POOL {
    this.synchronized {
      import OptionSupport._
      import FileSupport._
      import collection.JavaConversions._

      val config = broker.config

      // Start up the admin interface...
      debug("Starting administration interface");

      if( broker.tmp !=null ) {
        System.setProperty("scalate.workdir", (broker.tmp / "scalate").getCanonicalPath )
      }

      val contexts = HashMap[String, Handler]()
      val connectors = HashMap[String, Connector]()

      web_admins = config.web_admins.toList
      web_admins.foreach { web_admin =>

        val bind = web_admin.bind.getOrElse("http://127.0.0.1:61680")
        val bind_uri = new URI(bind)
        val prefix = "/"+bind_uri.getPath.stripPrefix("/")

        val scheme = bind_uri.getScheme
        val host = bind_uri.getHost
        var port = bind_uri.getPort

        scheme match {
          case "http" =>
            if (port == -1) {
              port = 80
            }
          case "https" =>
            if (port == -1) {
              port = 443
            }
          case _ => throw new Exception("Invalid 'web_admin' bind setting.  The protocol scheme must be http or https.")
        }

        // Only add the connector if not yet added..
        val connector_id = scheme+"://"+host+":"+port
        if ( !connectors.containsKey(connector_id) ) {

          val connector = scheme match {
            case "http" => new SelectChannelConnector
            case "https" =>
              val sslContext = if( broker.key_storage!=null ) {
                val protocol = "TLS"
                val sslContext = SSLContext.getInstance (protocol)
                sslContext.init(broker.key_storage.create_key_managers, broker.key_storage.create_trust_managers, null)
                sslContext
              } else {
                SSLContext.getDefault
              }

              val connector = new SslSelectChannelConnector
              connector.setSslContext(sslContext)
              connector.setWantClientAuth(true)
              connector
          }

          connector.setHost(host)
          connector.setPort(port)
          connectors.put(connector_id, connector)
        }

        // Only add the app context if not yet added..
        if ( !contexts.containsKey(prefix) ) {
          var context = new WebAppContext
          context.setContextPath(prefix)
          context.setWar(webapp.getCanonicalPath)
          context.setClassLoader(Broker.class_loader)
          if( broker.tmp !=null ) {
            context.setTempDirectory(broker.tmp)
          }
          contexts.put(prefix, context)
        }
      }


      val context_list = new HandlerList
      contexts.values.foreach(context_list.addHandler(_))

      server = new Server
      server.setHandler(context_list)
      server.setConnectors(connectors.values.toArray)
      server.setThreadPool(new ExecutorThreadPool(Broker.BLOCKABLE_THREAD_POOL))
      server.start

      for( connector <- connectors.values ; prefix <- contexts.keys ) {
        val localPort = connector.getLocalPort
        val scheme = connector match {
          case x:SslSelectChannelConnector => "https"
          case _ => "http"
        }

        def url = new URI(scheme, null, connector.getHost, localPort, prefix, null, null).toString
        broker.console_log.info("Administration interface available at: "+url)
      }
      on_completed.run
    }
  }

  protected def _stop(on_completed: Runnable) = Broker.BLOCKABLE_THREAD_POOL {
    this.synchronized {
      server.stop
      server = null
      on_completed.run
    }
  }

  def update(on_complete: Runnable) = dispatch_queue {
    import collection.JavaConversions._
    val new_list = broker.config.web_admins.toList
    if( new_list != web_admins ) {
      // restart to pickup the changes.
      stop(^{
        start(on_complete)
      })
    } else {
      on_complete.run()
    }
  }
}