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
package org.apache.activemq.apollo.broker.security

import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.broker.{VirtualHost, Broker}

trait SecurityFactory {
  def install(broker:Broker):Unit
  def install(virtual_host:VirtualHost):Unit
}

object DefaultSecurityFactory extends SecurityFactory {

  def install(broker:Broker) = {
    import OptionSupport._
    val config = broker.config
    if (config.authentication != null && config.authentication.enabled.getOrElse(true)) {
      broker.authenticator = new JaasAuthenticator(config.authentication, broker.security_log)
      broker.authorizer = Authorizer(broker)
    } else {
      broker.authenticator = null
      broker.authorizer = Authorizer()
    }
  }

  def install(host:VirtualHost) = {
    import OptionSupport._
    val config = host.config
    val broker = host.broker

    if (config.authentication != null) {
      if (config.authentication.enabled.getOrElse(true)) {
        // Virtual host has it's own settings.
        host.authenticator = new JaasAuthenticator(config.authentication, host.security_log)
      } else {
        // Don't use security on this host.
        host.authenticator = null
      }
    } else {
      // use the broker's settings..
      host.authenticator = broker.authenticator
    }
    if( host.authenticator !=null ) {
      host.authorizer = Authorizer(host)
    } else {
      host.authorizer = Authorizer()
    }
  }
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object SecurityFactory extends SecurityFactory {

  def install(broker: Broker) = {
    var factory_name = broker.config.security_factory
    if( factory_name == null ) {
      DefaultSecurityFactory.install(broker)
    } else {
      val sf = ClassFinder.default_loader.load(factory_name, classOf[SecurityFactory])
      sf.install(broker)
    }
  }

  def install(virtual_host: VirtualHost) = {
    var factory_name = virtual_host.broker.config.security_factory
    if( factory_name == null ) {
      DefaultSecurityFactory.install(virtual_host)
    } else {
      val sf = ClassFinder.default_loader.load(factory_name, classOf[SecurityFactory])
      sf.install(virtual_host)
    }
  }

}
