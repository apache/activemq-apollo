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

package org.apache.activemq.apollo.openwire

import javax.jms.JMSException

class SecurityTest extends OpenwireTestSupport {

  override val broker_config_uri: String = "xml:classpath:apollo-openwire-secure.xml"

  override protected def beforeAll = {
    try {
      val login_file = new java.io.File(getClass.getClassLoader.getResource("login.config").getFile())
      System.setProperty("java.security.auth.login.config", login_file.getCanonicalPath)
    } catch {
      case x:Throwable => x.printStackTrace
    }
    super.beforeAll
  }
}

class ConnectionFailureWithValidCredentials extends SecurityTest {

  test("Connect with valid id password but can't connect") {

    val factory = create_connection_factory
    val connection = factory.createConnection("can_not_connect", "can_not_connect")

    intercept[JMSException] {
      connection.start()
    }
  }
}

class CoonectionFailsWhenNoCredentialsGiven extends SecurityTest {

  test("Connect with no id password") {

    val factory = create_connection_factory
    val connection = factory.createConnection()

    intercept[JMSException] {
      connection.start()
    }
  }
}

class ConnectionFailsWhenCredentialsAreInvlaid extends SecurityTest {

  test("Connect with invalid id password") {
    val factory = create_connection_factory
    val connection = factory.createConnection("foo", "bar")

    intercept[JMSException] {
      connection.start()
    }
  }
}