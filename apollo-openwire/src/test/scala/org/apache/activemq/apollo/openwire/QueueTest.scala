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

import javax.jms.{DeliveryMode, Message, TextMessage, Session}


class BDBQueueTest extends OpenwireTestSupport {

  override def broker_config_uri = "xml:classpath:apollo-openwire-bdb.xml"

  test("Queue Prefetch and Client Ack") {

    connect("?jms.useAsyncSend=true")

    val session = default_connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
    val producer = session.createProducer(queue("prefetch"))
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT)
    def put(id:Int) {
      val msg = session.createBytesMessage()
      msg.writeBytes(new Array[Byte](1024*4))
      producer.send(msg)
    }

    for(i <- 1 to 1000) {
      put(i)
    }

    val consumer = session.createConsumer(queue("prefetch"))
    def get(id:Int) {
      val m = consumer.receive()
      expect(true, "Did not get message: "+id)(m!=null)
    }
    for(i <- 1 to 1000) {
      get(i)
    }
    default_connection.close()
    default_connection = null

    // All those messages should get redelivered since they were not previously
    // acked.
    connect()
    val session2 = default_connection.createSession(false, Session.CLIENT_ACKNOWLEDGE)
    val consumer2 = session2.createConsumer(queue("prefetch"))
    def get2(id:Int) {
      val m = consumer2.receive()
      expect(true, "Did not get message: "+id)(m!=null)
    }
    for(i <- 1 to 1000) {
      get2(i)
    }


  }
}

class QueueTest extends OpenwireTestSupport {
  
  test("Queue Message Cached") {

    connect("?wireFormat.cacheEnabled=false&wireFormat.tightEncodingEnabled=false")

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue("example"))
    def put(id:Int) {
      producer.send(session.createTextMessage("message:"+id))
    }

    List(1,2,3).foreach(put _)

    val consumer = session.createConsumer(queue("example"))

    def get(id:Int) {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(queue("example"))
      m.getText should equal ("message:"+id)
    }

    List(1,2,3).foreach(get _)
  }

  test("Queue order preserved") {

    connect()

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue("example"))
    def put(id:Int) {
      producer.send(session.createTextMessage("message:"+id))
    }

    List(1,2,3).foreach(put _)

    val consumer = session.createConsumer(queue("example"))

    def get(id:Int) {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(queue("example"))
      m.getText should equal ("message:"+id)
    }

    List(1,2,3).foreach(get _)
  }

  test("Test that messages are consumed ") {

    connect()
    var session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val queue = session.createQueue("test");
    val producer = session.createProducer(queue);
    producer.send(session.createTextMessage("Hello"));

    // Consume the message...
    var consumer = session.createConsumer(queue)
    var msg = consumer.receive(1000)
    msg should not be(null)

    Thread.sleep(1000)

    session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    // Attempt to Consume the message...check if message was acknowledge
    consumer = session.createConsumer(queue)
    msg = consumer.receive(1000)
    msg should be(null)

    session.close()
  }

  test("Queue and a selector") {
    connect()
    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(queue("example"))

    def put(id:Int, color:String) {
      val message = session.createTextMessage("message:" + id)
      message.setStringProperty("color", color)
      producer.send(message)
    }

    List((1, "red"), (2, "blue"), (3, "red")).foreach {
      case (id, color) => put(id, color)
    }

    val consumer = session.createConsumer(queue("example"), "color='red'")

    def get(id:Int) {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(queue("example"))
      m.getText should equal ("message:"+id)
    }

    get(1)
    get(3)
  }

  test("Receive then Browse and then Receive again") {
    connect()

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val producer = session.createProducer(queue("BROWSER.TEST.RB"))
    var consumer = session.createConsumer(queue("BROWSER.TEST.RB"))

    val outbound = List(session.createTextMessage("First Message"),
                        session.createTextMessage("Second Message"),
                        session.createTextMessage("Third Message"))

    // lets consume any outstanding messages from previous test runs
    while (consumer.receive(1000) != null) {
    }

    producer.send(outbound(0));
    producer.send(outbound(1));
    producer.send(outbound(2));

    consumer.receive(200) should be(outbound(0))

    consumer.close();

    val browser = session.createBrowser(queue("BROWSER.TEST.RB"))
    val enumeration = browser.getEnumeration

    // browse the second
    enumeration.hasMoreElements should be(true)
    enumeration.nextElement() should be(outbound(1))

    // browse the third.
    enumeration.hasMoreElements should be(true)
    enumeration.nextElement() should be(outbound(2))

    // There should be no more.
    var tooMany = false;
    while (enumeration.hasMoreElements) {
        debug("Got extra message: %s", enumeration.nextElement());
        tooMany = true;
    }
    tooMany should be(false)
    browser.close()

    // Re-open the consumer.
    consumer = session.createConsumer(queue("BROWSER.TEST.RB"));
    // Receive the second.
    consumer.receive(200) should be(outbound(1))
    // Receive the third.
    consumer.receive(200) should be(outbound(2))
    consumer.close()
  }

  test("Browse Queue then Receive messages") {
    connect()

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val producer = session.createProducer(queue("BROWSER.TEST.BR"))

    val outbound = List(session.createTextMessage("First Message"),
                        session.createTextMessage("Second Message"),
                        session.createTextMessage("Third Message"))

    producer.send(outbound(0))

    // create browser first
    val browser = session.createBrowser(queue("BROWSER.TEST.BR"))
    val enumeration = browser.getEnumeration

    // create consumer
    val consumer = session.createConsumer(queue("BROWSER.TEST.BR"))

    // browse the first message
    enumeration.hasMoreElements should be(true)
    enumeration.nextElement() should be(outbound(0))

    // Receive the first message.
    consumer.receive(100) should be(outbound(0))
  }

//  test("Queue Browser With 2 Consumers") {
//    val numMessages = 1000;
//
//    connect()
//
//    default_connection.setAlwaysSyncSend(false);
//
//    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
//
//    val destination = queue("BROWSER.TEST")
//
//    val destinationPrefetch10 = queue("TEST?jms.prefetchSize=10")
//    val destinationPrefetch1 = queue("TEST?jms.prefetchsize=1")
//
//    val connection2 = create_connection
//    connection2.start()
//    connections.add(connection2)
//    val session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE)
//
//    val producer = session.createProducer(destination)
//    val consumer = session.createConsumer(destinationPrefetch10)
//
//    for (i <- 1 to 10) {
//        val message = session.createTextMessage("Message: " + i)
//        producer.send(message)
//    }
//
//    val browser = session2.createBrowser(destinationPrefetch1)
//    val browserView = browser.getEnumeration()
//
//    val messages = List[Message]
//    for (i <- 0toInt numMessages) {
//      val m1 = consumer.receive(5000)
//      m1 shoulld not be(null)
//      messages += m1
//    }
//
//    val i = 0;
//    for (;i < numMessages && browserView.hasMoreElements(); i++) {
//        Message m1 = messages.get(i);
//        Message m2 = browserView.nextElement();
//        assertNotNull("m2 is null for index: " + i, m2);
//        assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());
//    }
//
//    // currently browse max page size is ignored for a queue browser consumer
//    // only guarantee is a page size - but a snapshot of pagedinpending is
//    // used so it is most likely more
//    assertTrue("got at least our expected minimum in the browser: ", i > BaseDestination.MAX_PAGE_SIZE);
//
//    assertFalse("nothing left in the browser", browserView.hasMoreElements());
//    assertNull("consumer finished", consumer.receiveNoWait());
//  }

  test("Browse Close") {
    connect()
    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val destination = queue("BROWSER.TEST.BC")

    val outbound = List(session.createTextMessage("First Message"),
                        session.createTextMessage("Second Message"),
                        session.createTextMessage("Third Message"))

    val producer = session.createProducer(destination)
    producer.send(outbound(0))
    producer.send(outbound(1))
    producer.send(outbound(2))

    // create browser first
    val browser = session.createBrowser(destination)
    val enumeration = browser.getEnumeration

    // browse some messages
    enumeration.nextElement() should equal(outbound(0))
    enumeration.nextElement() should equal(outbound(1))

    browser.close()

    // create consumer
    val consumer = session.createConsumer(destination)

    // Receive the first message.
    consumer.receive(1000) should equal(outbound(0))
    consumer.receive(1000) should equal(outbound(1))
    consumer.receive(1000) should equal(outbound(2))
  }
}