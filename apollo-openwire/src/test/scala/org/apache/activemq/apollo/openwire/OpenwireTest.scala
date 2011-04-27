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

/**
 * Copyright (C) 2010, FuseSource Corp. All rights reserved.
 */
import java.lang.String
import org.apache.activemq.apollo.broker.{Broker, BrokerFactory}
import org.apache.activemq.apollo.util.{Logging, FunSuiteSupport, ServiceControl}
import javax.jms._
import org.apache.activemq.ActiveMQConnectionFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.ShouldMatchers
import org.apache.activemq.command.{ActiveMQTopic, ActiveMQQueue}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class OpenwireTest extends FunSuiteSupport with ShouldMatchers with BeforeAndAfterEach with Logging {

  var broker: Broker = null
  var port = 0

  val broker_config_uri = "xml:classpath:apollo-openwire.xml"

  override protected def beforeAll() = {
    info("Loading broker configuration from the classpath with URI: " + broker_config_uri)
    broker = BrokerFactory.createBroker(broker_config_uri)
    ServiceControl.start(broker, "Starting broker")
    port = broker.get_socket_address.getPort
  }

  var default_connection:Connection = _
  var connections = List[Connection]()

  override protected def afterAll() = {
    broker.stop
  }

  override protected def afterEach() = {
    super.afterEach
    connections.foreach(_.close)
    connections = Nil
    default_connection = null
  }

  def create_connection_factory = new ActiveMQConnectionFactory("tcp://localhost:%d?wireFormat.maxInactivityDuration=1000000&wireFormat.maxInactivityDurationInitalDelay=1000000".format(port))
  def create_connection: Connection = create_connection_factory.createConnection
  def queue(value:String) = new ActiveMQQueue(value);
  def topic(value:String) = new ActiveMQTopic(value);

  def connect(start:Boolean=true) = {
    val connection = create_connection
    connections ::= connection
    if(default_connection==null) {
      default_connection = connection
    }
    if( start ) {
      connection.start
    }
    connection
  }

  test("Queue order preserved") {x}
  def x = {
    println("connecting")
    connect()
    println("connectted")
    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue("example"))
    def put(id:Int) = {
      producer.send(session.createTextMessage("message:"+id))
    }

    List(1,2,3).foreach(put _)

    val consumer = session.createConsumer(queue("example"))

    def get(id:Int) = {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(queue("example"))
      m.getText should equal ("message:"+id)
    }

    List(1,2,3).foreach(get _)
  }

  test("Topic drops messages sent before before subscription is established") {
    connect()
    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(topic("example"))
    def put(id:Int) = {
      producer.send(session.createTextMessage("message:"+id))
    }

    put(1)

    val consumer = session.createConsumer(topic("example"))

    put(2)
    put(3)

    def get(id:Int) = {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(topic("example"))
      m.getText should equal ("message:"+id)
    }

    List(2,3).foreach(get _)
  }


  test("Topic /w Durable sub retains messages.") {

    def create_durable_sub() = {
      val connection = connect(false)
      connection.setClientID("test")
      connection.start
      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val subscriber = session.createDurableSubscriber(topic("example"), "test")
      session.close
      connection.close
      if (default_connection == connection) {
        default_connection = null
      }
    }

    create_durable_sub

    connect(false)
    default_connection.setClientID("test")
    default_connection.start
    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(topic("example"))
    def put(id:Int) = {
      producer.send(session.createTextMessage("message:"+id))
    }

    List(1,2,3).foreach(put _)

    val subscriber = session.createDurableSubscriber(topic("example"), "test")

    def get(id:Int) = {
      val m = subscriber.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(topic("example"))
      m.getText should equal ("message:"+id)
    }

    List(1,2,3).foreach(get _)
  }

  test("Queue and a selector") {
    connect()
    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue("example"))

    def put(id:Int, color:String) = {
      val message = session.createTextMessage("message:" + id)
      message.setStringProperty("color", color)
      producer.send(message)
    }

    List((1, "red"), (2, "blue"), (3, "red")).foreach {
      case (id, color) => put(id, color)
    }

    val consumer = session.createConsumer(queue("example"), "color='red'")

    def get(id:Int) = {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(queue("example"))
      m.getText should equal ("message:"+id)
    }

    get(1)
    get(3)
  }
}