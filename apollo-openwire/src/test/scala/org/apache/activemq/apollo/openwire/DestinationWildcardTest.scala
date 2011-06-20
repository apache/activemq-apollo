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

import javax.jms.{MessageConsumer, MessageProducer, TextMessage, Session}

class DestinationWildcardTest extends OpenwireTestSupport {

  def path_separator = "."

  test("Wildcard subscription") {
    connect()

    val prefix = "foo" + path_separator

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer1 = session.createProducer(queue(prefix + "A"))
    val producer2 = session.createProducer(queue(prefix + "B"))
    val producer3 = session.createProducer(queue("bar.A"))
    def put(producer:MessageProducer, id:Int) {
      producer.send(session.createTextMessage("message:"+id))
    }

    List(1,2,3).foreach(put(producer1, _))
    producer3.send(session.createTextMessage("This one shouldn't get consumed."))
    List(1,2,3).foreach(put(producer2, _))

    val consumer = session.createConsumer(queue(prefix + "*"))

    def get(id:Int) {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getText should equal ("message:"+id)
    }

    List(1,2,3).foreach(get _)
    List(1,2,3).foreach(get _)

    consumer.receive(1000) should be(null)
  }

  test("Wildcard subscription with multiple path sections") {
    connect()

    val prefix1 = "foo" + path_separator + "bar" + path_separator
    val prefix2 = "foo" + path_separator + "bar" + path_separator + "cheeze" + path_separator
    val prefix3 = "foo" + path_separator + "bar" + path_separator + "cracker" + path_separator

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer1 = session.createProducer(topic(prefix1 + "A"))
    val producer2 = session.createProducer(topic(prefix2 + "B"))
    val producer3 = session.createProducer(topic(prefix3 + "C"))

    def put(producer:MessageProducer, id:Int) {
      producer.send(session.createTextMessage("message:"+id))
    }

    val consumer1 = session.createConsumer(topic(prefix1 + "*"))
    val consumer2 = session.createConsumer(topic(prefix2 + "*"))
    val consumer3 = session.createConsumer(topic(prefix3 + "*"))

    def get(consumer:MessageConsumer, id:Int) {
      val m = consumer.receive(2000).asInstanceOf[TextMessage]
      m should not be(null)
      m.getText should equal ("message:"+id)
    }

    // They only consumer one should see these.
    List(1,2,3).foreach(put(producer1, _))

    List(1,2,3).foreach(get(consumer1, _))
    consumer2.receive(2000) should be(null)
    consumer3.receive(2000) should be(null)

    // They only consumer two should see these.
    List(1,2,3).foreach(put(producer2, _))

    List(1,2,3).foreach(get(consumer2, _))
    consumer1.receive(2000) should be(null)
    consumer3.receive(2000) should be(null)

    // They only consumer two should see these.
    List(1,2,3).foreach(put(producer3, _))

    List(1,2,3).foreach(get(consumer3, _))
    consumer1.receive(2000) should be(null)
    consumer2.receive(2000) should be(null)
  }

}