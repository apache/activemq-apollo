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

package org.apache.activemq.apollo.mqtt.test

import org.fusesource.mqtt.client._
import org.apache.activemq.apollo.broker.Broker
import org.fusesource.hawtdispatch._
import java.util.concurrent.{TimeUnit, CountDownLatch}
import java.util.concurrent.atomic.AtomicReference

class MqttLoadTest extends MqttTestSupport {

  for( prefix <- List("normal", "queued") ) {

    test("Load on: "+prefix) {

      val topic = prefix+"/load"

      val receiver = create_client
      val error = new AtomicReference[Throwable]()
      connect(receiver)
      subscribe(topic, QoS.AT_LEAST_ONCE, receiver)

      val done = new CountDownLatch(1)
      Broker.BLOCKABLE_THREAD_POOL {
        try {
          for (i <- 1 to 1000) {
            should_receive("%0256d".format(i), topic, receiver)
          }
        } catch {
          case e:Throwable => error.set(e)
        } finally {
          done.countDown()
        }
      }

      connect()
      for(i <- 1 to 1000) {
        publish(topic, "%0256d".format(i), QoS.AT_LEAST_ONCE, false)
      }

      done.await(30, TimeUnit.SECONDS) should be(true)
      if( error.get() != null ) {
        throw error.get()
      }
    }

  }
}

//This test is failing with: java.lang.AssertionError: assertion failed: locator_based.unary_$bang.$bar$bar(uow.have_locators)
class MqttLoadLevelDBTest extends MqttLoadTest {
  override def broker_config_uri = "xml:classpath:apollo-mqtt-leveldb.xml"
}
