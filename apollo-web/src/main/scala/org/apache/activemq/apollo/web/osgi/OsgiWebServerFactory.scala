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
package org.apache.activemq.apollo.web.osgi

import org.apache.activemq.apollo.broker.web.{WebServerFactory, WebServer}
import org.apache.activemq.apollo.util.{Reporter, ReporterLevel}
import org.apache.activemq.apollo.dto.WebAdminDTO
import org.apache.activemq.apollo.broker.Broker
import org.apache.activemq.apollo.broker.osgi.BrokerService

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class OsgiWebServerFactory  extends WebServerFactory.Provider {

  // So that the factory class does not load, if we cannot load OSGi
  private val broker_service = BrokerService

  def create(broker:Broker): WebServer = new WebServer {

    def start: Unit = {
      // TODO: see if we can poke around and get the endpoint address
      // of our deployment to pax web
    }

    def stop: Unit = {
    }

    def start(onComplete: Runnable): Unit = {
      start
      onComplete.run
    }
    def stop(onComplete: Runnable): Unit = {
      stop
      onComplete.run
    }

  }

  def validate(config: WebAdminDTO, reporter: Reporter): ReporterLevel.ReporterLevel = {
    if( broker_service.context == null ) {
      return null
    }
    return ReporterLevel.INFO
  }
}