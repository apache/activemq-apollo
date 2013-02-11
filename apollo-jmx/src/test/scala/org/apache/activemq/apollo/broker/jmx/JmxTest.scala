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

import java.lang.String
import java.util.concurrent.TimeUnit._
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.util._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.ShouldMatchers
import java.lang.management.ManagementFactory
import javax.management.ObjectName
import org.apache.activemq.apollo.util.OptionSupport.AnyToOption

class JmxTest extends BrokerFunSuiteSupport with ShouldMatchers with BeforeAndAfterEach with Logging {

  override def broker_config_uri = "xml:classpath:apollo-jmx.xml"

  def platform_mbean_server = ManagementFactory.getPlatformMBeanServer()

  test("Broker registered") {
    val version = platform_mbean_server.getAttribute(new ObjectName("org.apache.apollo:type=broker,name=\"default\""), "Version")
    info should not be(null)
    info should not be(Broker.version)
  }
}
