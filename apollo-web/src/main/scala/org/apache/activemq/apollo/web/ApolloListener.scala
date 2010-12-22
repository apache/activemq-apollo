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

object ApolloListener extends Log

/**
 * A servlet context listener which handles starting the
 * broker instance.
 *
 * @version $Revision : 1.1 $
 */
class ApolloListener extends ServletContextListener {
  import ApolloListener._

  var configStore:ConfigStore = null
  var broker:Broker = _

  def contextInitialized(sce: ServletContextEvent) = {
    try {
      if( ConfigStore() == null ) {
        configStore = createConfigStore
        ConfigStore() = configStore
        val config = configStore.load(true)

        // Only start the broker up if it's enabled..
        info("starting broker");
        broker = new Broker()
        broker.config = config
        BrokerRegistry.add(broker)
        broker.start()
      }
    } catch {
      case e:Exception =>
        error(e, "failed to start broker")
    }
  }

  def contextDestroyed(sce: ServletContextEvent) = {
    if( configStore!=null ) {
      if( broker!=null ) {
        BrokerRegistry.remove(broker);
        ServiceControl.stop(broker, "broker")
      }
      configStore.stop
      configStore = null
    }
  }

  def createConfigStore():ConfigStore = {
    val store = new FileConfigStore
    store.file = new File("apollo.xml")
    store.start
    store
  }


}
