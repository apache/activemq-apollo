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
package org.apache.activemq.apollo.broker.jmx

import java.lang.management.ManagementFactory
import javax.management.ObjectName
import org.apache.activemq.apollo.broker.jmx.dto.JmxDTO
import org.apache.activemq.apollo.broker.{CustomServiceFactory, Broker}
import org.apache.activemq.apollo.dto.CustomServiceDTO
import org.apache.activemq.apollo.util.{Log, OptionSupport, BaseService, Service}
import org.fusesource.hawtdispatch._

/**
 */
class JMXSystemServiceFactory extends CustomServiceFactory {
  def create(broker: Broker, dto: CustomServiceDTO): Service = {
    dto match {
      case dto:JmxDTO => new JMXSystemService(broker, dto)
      case _ => null
    }
  }
}

object JMXSystemService extends Log
class JMXSystemService(val broker: Broker, val config:JmxDTO) extends BaseService {

  import JMXSystemService._

  val dispatch_queue: DispatchQueue = createQueue("JMXSystemService")

  def enabled = OptionSupport(config.enabled).getOrElse(true)
  def broker_object_name = new ObjectName("org.apache.apollo:type=broker,name="+ObjectName.quote(broker.id));

  protected def _start(on_completed: Task) = {
    if ( enabled && platform_mbean_server!=null ) {
      platform_mbean_server.registerMBean(new JmxBroker(broker, config), broker_object_name)
      info("Registered Broker in JMX")
    }
    on_completed.run()
  }

  protected def _stop(on_completed: Task) = {
    if ( enabled && platform_mbean_server!=null ) {
      platform_mbean_server.unregisterMBean(broker_object_name)
      info("Unregistered Broker from JMX")
    }
    on_completed.run()
  }

  def platform_mbean_server = ManagementFactory.getPlatformMBeanServer()

}

trait JmxBrokerMBean {
  def getVersion:String
  def getState:String
  def getWebAdminUrl:String
}

class JmxBroker(val broker: Broker, val config:JmxDTO) extends JmxBrokerMBean {
  def getVersion = Broker.version
  def getState = broker.service_state.toString
  def getWebAdminUrl = Option(config.admin_url).getOrElse(broker.web_admin_url)
}
