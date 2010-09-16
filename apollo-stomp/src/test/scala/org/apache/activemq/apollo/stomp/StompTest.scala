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
package org.apache.activemq.apollo.stomp

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.apache.activemq.apollo.util.FunSuiteSupport
import org.apache.activemq.apollo.broker.{Broker, BrokerFactory}

class StompTest extends FunSuiteSupport with ShouldMatchers {

  var broker:Broker = null


  test("Stomp Connect") {
    val client = new StompClient
    client.open("localhost", 61613)
    client.send("""CONNECT

""")
   val frame = client.receive()
   frame should startWith ("CONNECTED")
  }

  override protected def beforeAll() = {
    val uri = "xml:classpath:activemq-stomp.xml"
    info("Loading broker configuration from the classpath with URI: " + uri)
    broker = BrokerFactory.createBroker(uri, true)
    Thread.sleep(1000); //TODO implement waitUntilStarted
  }

  override protected def afterAll() = {
    broker.stop
  }

}