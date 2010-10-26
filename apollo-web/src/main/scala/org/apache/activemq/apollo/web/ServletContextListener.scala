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

import java.io.File
import org.apache.activemq.apollo.util._
import javax.servlet.{ServletContextListener, ServletContextEvent}
import org.apache.activemq.apollo.broker.{FileConfigStore, ConfigStore, BrokerRegistry, Broker}

/**
 * A servlet context listener which handles starting the
 * broker instance.
 *
 * @version $Revision : 1.1 $
 */
class ApolloListener extends ServletContextListener {

  var configStore:ConfigStore = null

  def contextInitialized(sce: ServletContextEvent) = {
    try {
      if( ConfigStore() == null ) {
        configStore = createConfigStore
        ConfigStore() = configStore

        // Brokers startup async.
        configStore.foreachBroker(true) { config=>
          println("Config store contained broker: "+config.id);

          // Only start the broker up if it's enabled..
          if( config.enabled ) {

            println("starting broker: "+config.id);
            val broker = new Broker()
            broker.config = config
            BrokerRegistry.add(config.id, broker)
            broker.start()

          }
        }
      }
    } catch {
      case e:Exception =>
        e.printStackTrace
    }
  }

  def contextDestroyed(sce: ServletContextEvent) = {
    val tracker = new LoggingTracker("webapp shutdown")
    if( configStore!=null ) {
      configStore.foreachBroker(false) { config=>
        // remove started brokers what we configured..
        val broker = BrokerRegistry.remove(config.id);
        if( broker!=null ) {
          tracker.stop(broker)
        }
      }
      tracker.stop(configStore)
      tracker.await
      configStore = null
    }
  }

  def createConfigStore():ConfigStore = {
    val store = new FileConfigStore
    store.file = new File("activemq.xml")
    LoggingTracker("config store startup") { tracker=>
      store.start(tracker.task())
    }
    store
  }


}