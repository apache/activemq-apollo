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

import org.mortbay.jetty.Connector
import org.mortbay.jetty.Handler
import org.mortbay.jetty.Server
import org.mortbay.jetty.nio.SelectChannelConnector
import org.mortbay.jetty.webapp.WebAppContext
import org.apache.commons.logging.LogFactory
import java.io.File

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Main {

  println("log4j at: "+getClass.getClassLoader.getResource("log4j.properties"))
  

  @transient
  private final val LOG = LogFactory.getLog(this.getClass)

  var server = new Server
  var port: Int = 8080
  var localPort: Int = 0

  var webAppDir: String = "src/main/webapp"
  var context: String = "/"


 def main(args:Array[String]) = run

  def run:Unit = {

    var dir = new File(webAppDir);
    if( !dir.exists ) {
      webAppDir = "apollo-web/"+webAppDir
      dir = new File(webAppDir);
      if( !dir.exists ) {
        println("The directory: "+webAppDir+" does not exist.")
        return
      }
    }
    
    println("===========================")
    println("Starting up ActiveMQ Apollo");
    println("===========================")
    println("")
    println("Press ENTER to shutdown");
    println("")

    start
    println("")
    println("Web interface available at: "+url)
    println("")

    System.in.read

    println("Shutting down...")
    stop

    println("=======================")
    println("Shutdown");
    println("=======================")
  }

  def start: Unit = {
    var connector = new SelectChannelConnector
    connector.setPort(port)
    connector.setServer(server)

    var app_context = new WebAppContext
    app_context.setContextPath(context)

    app_context.setResourceBase(webAppDir)
    // context.setBaseResource(new ResourceCollection(Array(webAppDir, overlayWebAppDir)))

    app_context.setServer(server)

    server.setHandlers(Array[Handler](app_context))
    server.setConnectors(Array[Connector](connector))
    server.start

    localPort = connector.getLocalPort
  }


  def stop: Unit = {
    server.stop
  }

  def url = "http://localhost:" + localPort + context

  override def toString() = {
    "--------------------------------------\n"+
    "Configurable Properties\n"+
    "--------------------------------------\n"+
    "port             = "+port+"\n"+
    "context          = "+context+"\n"+
    ""
  }

}