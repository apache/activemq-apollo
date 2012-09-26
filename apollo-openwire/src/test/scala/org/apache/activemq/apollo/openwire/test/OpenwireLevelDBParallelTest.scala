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

package org.apache.activemq.apollo.openwire.test

import javax.jms.{DeliveryMode, Session}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

class OpenwireLevelDBParallelTest extends OpenwireParallelTest {
  override def broker_config_uri = "xml:classpath:apollo-openwire-leveldb.xml"

  test("Queue Prefetch and Client Ack") {

    connect("?jms.useAsyncSend=true")
    var dest = queue(next_id("prefetch"))

    val session = default_connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
    val producer = session.createProducer(dest)
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT)
    def put(id: Int) {
      val msg = session.createBytesMessage()
      msg.writeBytes(new Array[Byte](1024 * 4))
      producer.send(msg)
    }

    for (i <- 1 to 1000) {
      put(i)
    }

    val consumer = session.createConsumer(dest)
    def get(id: Int) {
      val m = consumer.receive()
      expect(true, "Did not get message: " + id)(m != null)
    }
    for (i <- 1 to 1000) {
      get(i)
    }
    default_connection.close()
    default_connection = null

    // All those messages should get redelivered since they were not previously
    // acked.
    connect()
    val session2 = default_connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
    val consumer2 = session2.createConsumer(dest)
    def get2(id: Int) {
      val m = consumer2.receive()
      expect(true, "Did not get message: " + id)(m != null)
    }
    for (i <- 1 to 1000) {
      get2(i)
    }
  }

}