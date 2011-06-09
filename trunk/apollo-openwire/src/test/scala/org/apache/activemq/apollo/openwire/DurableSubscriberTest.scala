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

import javax.jms.{TextMessage, Session}

class DurableSubscriberTest extends OpenwireTestSupport {

  test("Topic /w Durable sub retains messages.") {

    def create_durable_sub() {
      val connection = connect(false)
      connection.setClientID("test")
      connection.start()
      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val subscriber = session.createDurableSubscriber(topic("example"), "test")
      session.close()
      connection.close()
      if (default_connection == connection) {
        default_connection = null
      }
    }

    create_durable_sub()

    connect(false)
    default_connection.setClientID("test")
    default_connection.start()
    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(topic("example"))
    def put(id:Int) {
      producer.send(session.createTextMessage("message:"+id))
    }

    List(1,2,3).foreach(put _)

    val subscriber = session.createDurableSubscriber(topic("example"), "test")

    def get(id:Int) {
      val m = subscriber.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(topic("example"))
      m.getText should equal ("message:"+id)
    }

    List(1,2,3).foreach(get _)
  }

}