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
package org.apache.activemq.apollo.broker.jaxb

import org.apache.activemq.apollo.broker.BrokerFactory
import org.apache.activemq.apollo.util.FunSuiteSupport
import org.apache.activemq.apollo.dto.AcceptingConnectorDTO

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class XMLBrokerFactoryTest extends FunSuiteSupport {
  test("Simple Config") {
    val uri = "xml:classpath:org/apache/activemq/apollo/broker/jaxb/testSimpleConfig.xml"
    info("Loading broker configuration from the classpath with URI: " + uri)
    val broker = BrokerFactory.createBroker(uri)

    expect(1) {
      broker.config.connectors.size()
    }

    expect("pipe://test1") {
      broker.config.connectors.get(0).asInstanceOf[AcceptingConnectorDTO].bind
    }

    expect("tcp://127.0.0.1:61616") {
      broker.config.connectors.get(1).asInstanceOf[AcceptingConnectorDTO].bind
    }

    expect(2) {
      broker.config.virtual_hosts.size()
    }

  }

  def expectException(msg: String = "Expected exeception.")(func: => Unit) = {
    try {
      func
      fail(msg)
    } catch {
      case e: Exception =>
    }
  }

  test("Uris") {

    // non-existent classpath

    expectException("Creating broker from non-existing url does not throw an exception!") {
      val uri = "xml:classpath:org/apache/activemq/apollo/broker/jaxb/testUris-fail.xml"
      BrokerFactory.createBroker(uri)
    }

    //non-existent file
    expectException("Creating broker from non-existing url does not throw an exception!") {
      val uri = "xml:file:/org/apache/activemq/apollo/broker/jaxb/testUris-fail.xml"
      BrokerFactory.createBroker(uri)
    }

    //non-existent url
    expectException("Creating broker from non-existing url does not throw an exception!") {
      val uri = "xml:http://localhost/testUris.xml"
      BrokerFactory.createBroker(uri)
    }

    // regular file
    val uri = "xml:" + Thread.currentThread().getContextClassLoader().getResource("org/apache/activemq/apollo/broker/jaxb/testUris.xml")
    BrokerFactory.createBroker(uri)
  }

}
