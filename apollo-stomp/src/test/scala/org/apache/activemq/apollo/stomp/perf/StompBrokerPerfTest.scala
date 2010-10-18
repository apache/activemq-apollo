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
package org.apache.activemq.apollo.stomp.perf

import _root_.org.apache.activemq.apollo.broker.perf._
import java.io.File
import org.apache.activemq.apollo.dto.{BrokerDTO, HawtDBStoreDTO}
import org.apache.activemq.apollo.store.bdb.dto.BDBStoreDTO


class BasicNonPersistentTest extends BasicScenarios with StompScenario {
  override def description = "Using the STOMP protocol over TCP"
}

class BasicHawtDBTest extends BasicScenarios with PersistentScenario with HawtDBScenario with StompScenario {
  override def description = "Using the STOMP protocol over TCP"
}

class DeepQueueHawtDBTest extends DeepQueueScenarios with LargeInitialDB with HawtDBScenario with StompScenario {
  override def description = "Using the STOMP protocol over TCP persisting to the HawtDB store."
}

class DeepQueueBDBTest extends DeepQueueScenarios with LargeInitialDB with BDBScenario with StompScenario {
  override def description = "Using the STOMP protocol over TCP persisting to the BerkleyDB store."
}

trait StompScenario extends BrokerPerfSupport {
  override def createProducer() = new StompRemoteProducer()
  override def createConsumer() = new StompRemoteConsumer()
  override def getRemoteProtocolName() = "stomp"
}

trait HawtDBScenario extends PersistentScenario {
  override def createBrokerConfig(name: String, bindURI: String, connectUri: String): BrokerDTO = {
    val rc = super.createBrokerConfig(name, bindURI, connectUri)
    val store = new HawtDBStoreDTO
    storeDirectory = new File(new File(testDataDir, getClass.getName), name)
    store.directory = storeDirectory
    rc.virtual_hosts.get(0).store = store
    rc
  }
}

trait BDBScenario extends PersistentScenario {
  override def createBrokerConfig(name: String, bindURI: String, connectUri: String): BrokerDTO = {
    val rc = super.createBrokerConfig(name, bindURI, connectUri)
    val store = new BDBStoreDTO
    storeDirectory = new File(new File(testDataDir, getClass.getName), name)
    store.directory = storeDirectory
    rc.virtual_hosts.get(0).store = store
    rc
  }
}

