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

package org.apache.activemq.apollo.broker.network

import org.scalatest.matchers.ShouldMatchers
import org.scalatest.BeforeAndAfterEach
import javax.jms.Session._
import org.fusesource.stomp.jms.{StompJmsDestination, StompJmsConnectionFactory}
import collection.mutable.ListBuffer
import javax.jms.{Message, TextMessage, Connection, ConnectionFactory}
import java.util.concurrent.TimeUnit._
import org.apache.activemq.apollo.broker.{Broker, MultiBrokerTestSupport}
import org.fusesource.hawtdispatch._

class NetworkTest extends MultiBrokerTestSupport with ShouldMatchers with BeforeAndAfterEach {

  override def broker_config_uris = Array(
    "xml:classpath:apollo-network-1.xml",
    "xml:classpath:apollo-network-2.xml"
  )

  val connections = ListBuffer[Connection]()

  override protected def afterEach() = {
    for( c <- connections ) {
      try {
        c.close()
      } catch {
        case ignore:Throwable =>
      }
    }
    connections.clear()
  }

  def create_connection(factory:ConnectionFactory) = {
    val rc = factory.createConnection()
    rc.start()
    connections += rc
    rc
  }

  def connection_factories = admins.map { admin =>
    val rc = new StompJmsConnectionFactory();
    rc.setBrokerURI("tcp://localhost:"+admin.port);
    rc
  }

  def create_connections = connection_factories.map(create_connection(_))

  def test_destination(kind:String="queue", name:String=testName) =
    new StompJmsDestination("/"+kind+"/"+name.replaceAll("[^a-zA-Z0-9._-]", "_"))

  def text(msg:Message) = msg match {
    case msg:TextMessage => Some(msg.getText)
    case _ => None
  }

  test("forward 10000 messages") {
    val connections = create_connections
    val message_count = 10000;

    var dest = test_destination()
    val data = "x" * 1024

    Broker.BLOCKABLE_THREAD_POOL {
      val s0 = connections(0).createSession(false, AUTO_ACKNOWLEDGE)
      val p0 = s0.createProducer(dest)
      for( i <- 0 until message_count ) {
        p0.send(s0.createTextMessage(i+":"+data))
      }
    }

    val s1 = connections(1).createSession(false, AUTO_ACKNOWLEDGE)
    val c1 = s1.createConsumer(dest)
    within(30, SECONDS) {
      for( i <- 0 until message_count ) {
        text(c1.receive()) should be(Some(i+":"+data))
      }
    }
  }


}