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

import com.google.inject.servlet.GuiceServletContextListener
import org.fusesource.scalate.guice.ScalateModule
import javax.servlet.ServletContextEvent
import org.apache.activemq.transport.TransportFactory
import org.apache.activemq.apollo.broker.{LoggingTracker, Broker}
import org.apache.activemq.apollo.{FileConfigStore, ConfigStore, BrokerRegistry}
import java.io.File
import com.google.inject.{Inject, Provides, Guice, Singleton}
import org.fusesource.hawtdispatch.ScalaDispatch._

/**
 * A servlet context listener which registers
 * <a href="http://code.google.com/p/google-guice/wiki/Servlets">Guice Servlet</a>
 *
 * @version $Revision : 1.1 $
 */
class ServletContextListener extends GuiceServletContextListener {

  var broker:Broker = null

  override def contextInitialized(servletContextEvent: ServletContextEvent) = {

    try {
      BrokerRegistry.configStore = createConfigStore

      // Brokers startup async.
      BrokerRegistry.configStore.listBrokers { ids =>
        ids.foreach { id=>
          BrokerRegistry.configStore.getBroker(id, { x=>
            x match {
              case Some(config)=>
                // Only start the broker up if it's enabled..
                if( config.enabled ) {
                  val broker = new Broker()
                  broker.config = config
                  BrokerRegistry.add(broker)
                  broker.start()
                }
              case None =>
            }
          })
        }
      }

    }
    catch {
      case e:Exception =>
        broker = null
        e.printStackTrace
    }

    super.contextInitialized(servletContextEvent);
  }

  override def contextDestroyed(servletContextEvent: ServletContextEvent) = {
    super.contextDestroyed(servletContextEvent);

    // un-register/stop brokers we are managing.
    if( broker!=null ) {
      BrokerRegistry.remove(broker.id);
      LoggingTracker("broker shutdown") { tracker =>
        broker.stop(tracker.task(broker.id))
        BrokerRegistry.configStore.stop(tracker.task("config store"))
      }
    }
  }

  def getInjector = Guice.createInjector(new ScalateModule() {

    @Singleton
    @Provides
    def provideConfigStore:ConfigStore = createConfigStore

    // lets add any package names which contain JAXRS resources
    // https://jersey.dev.java.net/issues/show_bug.cgi?id=485
    override def resourcePackageNames =
      "org.apache.activemq.apollo.web.resources" ::
      "org.codehaus.jackson.jaxrs" ::
      super.resourcePackageNames
  })

  def createConfigStore():ConfigStore = {
    println("created store")
    val store = new FileConfigStore
    store.file = new File("activemq.xml")
    LoggingTracker("config store startup") { tracker=>
      println("starting store")
      store.start(tracker.task())
    }
    println("store started")
    store
  }


}