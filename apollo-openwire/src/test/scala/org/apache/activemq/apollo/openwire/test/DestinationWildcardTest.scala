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

import javax.jms.{MessageConsumer, MessageProducer, TextMessage, Session}
import org.apache.activemq.apollo.broker.BrokerParallelTestExecution

class DestinationWildcardTest extends OpenwireTestSupport with BrokerParallelTestExecution {

  def path_separator = "."

  test("Wildcard subscription") {
    connect()

    val common_prefix = next_id()
    val prefix = common_prefix + path_separator

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer1 = session.createProducer(queue(prefix + "A"))
    val producer2 = session.createProducer(queue(prefix + "B"))
    val producer3 = session.createProducer(queue(common_prefix+ "bar.A"))
    def put(producer: MessageProducer, id: Int) {
      producer.send(session.createTextMessage("message:" + id))
    }

    val consumer = session.createConsumer(queue(prefix + "*"))
    def get(id: Int) {
      receive_text(consumer) should equal("message:" + id)
    }

    List(1, 2, 3).foreach(put(producer1, _))
    List(1, 2, 3).foreach(get _)

    producer3.send(session.createTextMessage("This one shouldn't get consumed."))

    List(1, 2, 3).foreach(put(producer2, _))
    List(1, 2, 3).foreach(get _)

    put(producer1, -1)
    get(-1)
  }

  test("Wildcard subscription with multiple path sections") {
    connect()

    val common_prefix = next_id()
    val prefix1 = common_prefix+"foo" + path_separator + "bar" + path_separator
    val prefix2 = common_prefix+"foo" + path_separator + "bar" + path_separator + "cheeze" + path_separator
    val prefix3 = common_prefix+"foo" + path_separator + "bar" + path_separator + "cracker" + path_separator

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer1 = session.createProducer(topic(prefix1 + "A"))
    val producer2 = session.createProducer(topic(prefix2 + "B"))
    val producer3 = session.createProducer(topic(prefix3 + "C"))

    def put(producer: MessageProducer, id: Int) {
      producer.send(session.createTextMessage("message:" + id))
    }
    def get(consumer: MessageConsumer, id: Int) {
      receive_text(consumer) should equal("message:" + id)
    }

    val consumer1 = session.createConsumer(topic(prefix1 + "*"))
    val consumer2 = session.createConsumer(topic(prefix2 + "*"))
    val consumer3 = session.createConsumer(topic(prefix3 + "*"))


    put(producer1, 1)
    List(producer1, producer2, producer3).foreach(put(_, -1))

    get(consumer1, 1)
    List(consumer1, consumer2, consumer3).foreach(get(_, -1))


    put(producer2, 2)
    List(producer1, producer2, producer3).foreach(put(_, -1))

    get(consumer2, 2)
    List(consumer1, consumer2, consumer3).foreach(get(_, -1))


    put(producer3, 3)
    List(producer1, producer2, producer3).foreach(put(_, -1))

    get(consumer3, 3)
    List(consumer1, consumer2, consumer3).foreach(get(_, -1))

  }

}