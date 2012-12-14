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

package org.apache.activemq.apollo.amqp.test

import org.apache.qpid.amqp_1_0.jms.impl.{TopicImpl, ConnectionFactoryImpl, QueueImpl}
import javax.jms._





import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

object QpidJmsTest {
  def enableJMSFrameTracing {
    val out = System.out // new PrintStream(new FileOutputStream(new File("/tmp/amqp-trace.txt")))
    val handler = new Handler {
      setLevel(Level.ALL)
      def publish(r: LogRecord)  = out.println(String.format("%s:%s", r.getLoggerName, r.getMessage))
      def flush = out.flush
      def close {}
    }
    var log = Logger.getLogger("FRM")
    log.setLevel(Level.ALL)
    log.addHandler(handler)

//    log = Logger.getLogger("RAW")
//    log.setLevel(Level.ALL)
//    log.addHandler(handler)
  }
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class QpidJmsTest extends AmqpTestSupport {

  def createConnection(clientId:String=null): Connection = {
    val factory = new ConnectionFactoryImpl("localhost", port, "admin", "password", clientId)
    val connection = factory.createConnection
    connection.setExceptionListener(new ExceptionListener {
      def onException(exception: JMSException) {
        exception.printStackTrace
      }
    })
    connection.start
    return connection
  }

  test("Transaction Test") {

    val default_connection = createConnection()
    val destination = new QueueImpl("queue://txtest")
    val session = default_connection.createSession(true, Session.SESSION_TRANSACTED)
    var consumer = session.createConsumer(destination)
    var producer = session.createProducer(destination)

    var msg = session.createTextMessage("1")
    producer.send(msg)
    session.commit()

    msg.setText("2")
    producer.send(msg)
    session.rollback()

    msg.setText("3")
    producer.send(msg)
    session.commit()

    receive_text(consumer) should equal("1")
    session.commit()
    receive_text(consumer) should equal("3")
    session.rollback()
    receive_text(consumer) should equal("3")
    session.commit()

  }

  test("NoLocal Test") {

    val default_connection = createConnection("clientid")
    val destination = new TopicImpl("topic://example")
    val localSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    var localConsumer = localSession.createConsumer(destination, null, true)
    var localProducer = localSession.createProducer(destination)

    val remoteConnection = createConnection()
    val remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    var remoteProducer = remoteSession.createProducer(destination)

    remoteProducer.send(localSession.createTextMessage("1"))
    localProducer.send(localSession.createTextMessage("2"))
    remoteProducer.send(localSession.createTextMessage("3"))

    receive_text(localConsumer) should equal("1")
    receive_text(localConsumer) should equal("3")
  }

  test("NoLocal Durable Sub Test") {

    val default_connection = createConnection("clientid")
    val destination = new TopicImpl("topic://example2")
    val localSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    var localConsumer = localSession.createDurableSubscriber(destination, "A", null, true)
    var localProducer = localSession.createProducer(destination)

    val remoteConnection = createConnection()
    val remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    var remoteProducer = remoteSession.createProducer(destination)

    remoteProducer.send(localSession.createTextMessage("1"))
    localProducer.send(localSession.createTextMessage("2"))
    remoteProducer.send(localSession.createTextMessage("3"))

    receive_text(localConsumer) should equal("1")
    receive_text(localConsumer) should equal("3")
  }

  def receive_text(consumer:MessageConsumer) = consumer.receive().asInstanceOf[TextMessage].getText

  test("unsubscribe invalid dest") {
    val queue = new QueueImpl("queue://txqueue")
    val connection = createConnection()
    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    try {
      session.unsubscribe("does not exist")
      fail("expected an InvalidDestinationException")
    } catch {
      case e:InvalidDestinationException =>
    }
    connection.close
  }

  ignore("browse") {
    val queue = new QueueImpl("queue://txqueue")
    val connection = createConnection()
    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val p = session.createProducer(queue)
    val msg = session.createTextMessage("Hello World")
    msg.setObjectProperty("x", 1)
    p.send(msg)
    val browser = session.createBrowser(queue)
    val enumeration = browser.getEnumeration
    while (enumeration.hasMoreElements) {
      System.out.println("BROWSE " + enumeration.nextElement)
    }
    connection.close
  }

  ignore("Send Nack Receive") {
    // enableJMSFrameTracing
    val queue = new QueueImpl("/queue/testqueue")
    val nMsgs = 1
    val dataFormat: String = "%01024d"

    var connection = createConnection()
    var session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val p = session.createProducer(queue)
    var i = 0
    while (i < nMsgs) {
      System.out.println("Sending " + i)
      p.send(session.createTextMessage(dataFormat.format(i)))
      i += 1
    }
    connection.close

    System.out.println("=======================================================================================")
    System.out.println(" failing a receive ")
    System.out.println("=======================================================================================")
    connection = createConnection()
    session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
    var c = session.createConsumer(queue)
    i = 0
    while (i < 1) {
      val msg: TextMessage = c.receive.asInstanceOf[TextMessage]
      if (msg != null) {
        val s: String = msg.getText
        s should  be(dataFormat.format(i))
        System.out.println("Received: " + i)
        i += 1
      }
    }
    connection.close

    System.out.println("=======================================================================================")
    System.out.println(" receiving ")
    System.out.println("=======================================================================================")
    connection = createConnection()
    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    c = session.createConsumer(queue)
    i = 0
    while (i < nMsgs) {
      val msg = c.receive.asInstanceOf[TextMessage]
      if (msg != null) {
        val s = msg.getText
        s should  be(dataFormat.format(i))
        System.out.println("Received: " + i)
        i += 1
      }
    }
    connection.close
  }


}