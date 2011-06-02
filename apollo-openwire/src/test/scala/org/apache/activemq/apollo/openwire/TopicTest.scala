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

class TopicTest extends OpenwireTestSupport {

  test("Topic drops messages sent before before subscription is established") {
    connect()
    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(topic("example"))
    def put(id:Int) {
      producer.send(session.createTextMessage("message:"+id))
    }

    put(1)

    val consumer = session.createConsumer(topic("example"))

    put(2)
    put(3)

    def get(id:Int) {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(topic("example"))
      m.getText should equal ("message:"+id)
    }

    List(2,3).foreach(get _)
  }

}