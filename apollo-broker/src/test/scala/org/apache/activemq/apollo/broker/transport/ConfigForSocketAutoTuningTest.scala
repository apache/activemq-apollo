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
package org.apache.activemq.apollo.broker.transport

import org.apache.activemq.apollo.util.{ServiceControl, FunSuiteSupport}
import org.apache.activemq.apollo.broker.{Broker, AcceptingConnector, Connector, BrokerFactory}

/**
 *
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
class ConfigForSocketAutoTuningTest extends FunSuiteSupport{

  test("Config for socket receive buffers"){
    val uri = "xml:classpath:org/apache/activemq/apollo/broker/transport/auto-tune-config.xml"
    info("Loading broker configuration from the classpath with URI: " + uri)
    val broker = BrokerFactory.createBroker(uri)
    ServiceControl.start(broker, "broker")
    broker.connectors.foreach{ case (s, c) =>
      assert(!c.asInstanceOf[AcceptingConnector].receive_buffer_auto_tune)
      assert(!c.asInstanceOf[AcceptingConnector].send_buffer_auto_tune)
    }

    ServiceControl.stop(broker, "broker")

  }

  test("Default auto tune settings"){
    val broker = new Broker
    ServiceControl.start(broker, "broker")
    assert(broker.first_accepting_connector != null)
    broker.connectors.foreach{ case (s, c) =>
      assert(c.asInstanceOf[AcceptingConnector].receive_buffer_auto_tune)
      assert(c.asInstanceOf[AcceptingConnector].send_buffer_auto_tune)
    }
    ServiceControl.stop(broker, "broker")
  }

}
