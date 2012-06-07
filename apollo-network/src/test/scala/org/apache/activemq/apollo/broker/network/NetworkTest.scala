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

package org.apache.activemq.apollo.broker.network

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach
import org.apache.activemq.apollo.broker.MultiBrokerTestSupport


class NetworkTest extends MultiBrokerTestSupport with ShouldMatchers with BeforeAndAfterEach {

  override def broker_config_uris = Array(
    "xml:classpath:apollo-network-1.xml",
    "xml:classpath:apollo-network-2.xml"
  )

  test("basics") {
    admins(0).broker should not be(null)
    val config = admins(0).broker.config
    admins(1).broker should not be(null)
  }

}