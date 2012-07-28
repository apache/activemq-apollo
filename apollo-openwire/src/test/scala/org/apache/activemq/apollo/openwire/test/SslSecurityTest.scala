/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.apollo.openwire.test

import javax.jms.{TextMessage, Session}

class SslSecurityTest extends OpenwireTestSupport {

  override val broker_config_uri: String = "xml:classpath:apollo-openwire-ssl-secure.xml"
  override val transport_scheme = "ssl"

  override def beforeAll = {
    // System.setProperty("javax.net.debug", "all")
    try {
      val login_file = new java.io.File(getClass.getClassLoader.getResource("login.config").getFile())
      System.setProperty("java.security.auth.login.config", login_file.getCanonicalPath)
      System.setProperty("javax.net.ssl.trustStore", basedir + "/src/test/resources/apollo.ks");
      System.setProperty("javax.net.ssl.trustStorePassword", "password");
      System.setProperty("javax.net.ssl.trustStoreType", "jks");
      System.setProperty("javax.net.ssl.keyStore", basedir + "/src/test/resources/client.ks");
      System.setProperty("javax.net.ssl.keyStorePassword", "password");
      System.setProperty("javax.net.ssl.keyStoreType", "jks");
    } catch {
      case x: Throwable => x.printStackTrace
    }
    super.beforeAll
  }

  ignore("Connect with cert and no id password") {

    connect()

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(topic("example"))
    def put(id: Int) {
      producer.send(session.createTextMessage("message:" + id))
    }

    put(1)

    val consumer = session.createConsumer(topic("example"))

    put(2)
    put(3)

    def get(id: Int) {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(topic("example"))
      m.getText should equal("message:" + id)
    }

    List(2, 3).foreach(get _)

  }

}