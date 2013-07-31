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

import javax.jms._
import org.apache.activemq.apollo.broker.BrokerParallelTestExecution
import org.apache.activemq.{ActiveMQConnection, RedeliveryPolicy}

class OpenwireParallelTest extends OpenwireTestSupport with BrokerParallelTestExecution {

  def path_separator = "."

  test("Topic /w Durable sub retains messages.") {

    val dest = topic(next_id("example"))

    def create_durable_sub() {
      val connection = connect("", false)
      connection.setClientID("test")
      connection.start()
      val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
      val subscriber = session.createDurableSubscriber(dest, "test")
      session.close()
      connection.close()
      if (default_connection == connection) {
        default_connection = null
      }
    }

    create_durable_sub()

    connect("", false)
    default_connection.setClientID("test")
    default_connection.start()
    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val producer = session.createProducer(dest)
    def put(id: Int) {
      producer.send(session.createTextMessage("message:" + id))
    }

    List(1, 2, 3).foreach(put _)

    val subscriber = session.createDurableSubscriber(dest, "test")

    def get(id: Int) {
      val m = subscriber.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(dest)
      m.getText should equal("message:" + id)
    }

    List(1, 2, 3).foreach(get _)
  }


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

  test("Wildcard subscription recursive"){
    connect()

    val common_prefix = next_id() + path_separator

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)


    val producer1_dest = common_prefix + "A"
    val producer1 = session.createProducer(queue(producer1_dest))

    val producer2_dest = producer1_dest + path_separator + "bar"
    val producer2 = session.createProducer(queue(producer2_dest))


    def put(producer: MessageProducer, id: Int) {
      producer.send(session.createTextMessage("message:" + id))
    }

    val subscribe_dest = common_prefix + "A" + path_separator + ">"
    val consumer = session.createConsumer(queue(subscribe_dest))

    def get(id: Int) {
      receive_text(consumer) should equal("message:" + id)
    }

    // put messages onto the queues and consume them
    List(1, 2, 3).foreach(put(producer1, _))
    List(1, 2, 3).foreach(get _)

    List(4, 5, 6).foreach(put(producer2, _))
    List(4, 5, 6).foreach(get _)
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

  test("Exclusive Consumer Selected when created first") {

    val dest_name = next_id("TEST.QUEUE")

    connect()

    val exclusiveSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val exclusiveConsumer = exclusiveSession.createConsumer(queue(dest_name+"?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue(dest_name))
    val producer = senderSession.createProducer(queue(dest_name))

    producer.send(senderSession.createTextMessage("Exclusive Consumer Selected when created first - 1"))

    Thread.sleep(100)

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(receive_timeout) should not be (null)
    fallbackConsumer.receive(100) should be(null)
  }

  test("Exclusive Consumer Selected when Created After Non-Exclusive Consumer") {
    connect()

    val dest_name = next_id("TEST.QUEUE")

    val exclusiveSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val fallbackConsumer = fallbackSession.createConsumer(queue(dest_name))
    val exclusiveConsumer = exclusiveSession.createConsumer(queue(dest_name+"?consumer.exclusive=true"))
    val producer = senderSession.createProducer(queue(dest_name))

    val msg = senderSession.createTextMessage("test")
    producer.send(msg);

    Thread.sleep(100)

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(receive_timeout) should not be (null)
    fallbackConsumer.receive(100) should be(null)
  }

  test("Failover To Another Exclusive Consumer Created First") {

    connect()

    val dest_name = next_id("TEST.QUEUE")

    val exclusiveSession1 = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val exclusiveSession2 = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val exclusiveConsumer1 = exclusiveSession1.createConsumer(queue(dest_name+"?consumer.exclusive=true"))
    val exclusiveConsumer2 = exclusiveSession2.createConsumer(queue(dest_name+"?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue(dest_name))
    val producer = senderSession.createProducer(queue(dest_name))

    producer.send(senderSession.createTextMessage("Failover To Another Exclusive Consumer Created First - 1"))

    Thread.sleep(100)

    // Verify exclusive consumer receives the message.
    exclusiveConsumer1.receive(receive_timeout) should not be (null)
    exclusiveConsumer2.receive(100) should be(null)
    fallbackConsumer.receive(100) should be(null)

    // Close the exclusive consumer to verify the non-exclusive consumer
    // takes over
    exclusiveConsumer1.close()

    producer.send(senderSession.createTextMessage("Failover To Another Exclusive Consumer Created First - 2"))
    producer.send(senderSession.createTextMessage("Failover To Another Exclusive Consumer Created First - 3"))

    exclusiveConsumer2.receive(receive_timeout) should not be (null)
    fallbackConsumer.receive(100) should be(null)
  }

  test("Failover To Another Exclusive Consumer Created After a non-exclusive Consumer") {

    connect()

    val dest_name = next_id("TEST.QUEUE")

    val exclusiveSession1 = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val exclusiveSession2 = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val exclusiveConsumer1 = exclusiveSession1.createConsumer(queue(dest_name+"?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue(dest_name))
    val exclusiveConsumer2 = exclusiveSession2.createConsumer(queue(dest_name+"?consumer.exclusive=true"))
    val producer = senderSession.createProducer(queue(dest_name))

    producer.send(senderSession.createTextMessage("Failover To Another Exclusive Consumer Created After - 1"));

    Thread.sleep(100)

    // Verify exclusive consumer receives the message.
    exclusiveConsumer1.receive(receive_timeout) should not be (null)
    exclusiveConsumer2.receive(100) should be(null)
    fallbackConsumer.receive(100) should be(null)

    // Close the exclusive consumer to verify the non-exclusive consumer
    // takes over
    exclusiveConsumer1.close()

    producer.send(senderSession.createTextMessage("Failover To Another Exclusive Consumer Created After - 2"))
    producer.send(senderSession.createTextMessage("Failover To Another Exclusive Consumer Created After - 3"))

    exclusiveConsumer2.receive(receive_timeout) should not be (null)
    fallbackConsumer.receive(100) should be(null)
  }

  test("Failover To NonExclusive Consumer") {

    info("*** Running Test: Failover To NonExclusive Consumer")

    connect()
    val dest_name = next_id("TEST.QUEUE")

    val exclusiveSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val exclusiveConsumer = exclusiveSession.createConsumer(queue(dest_name+"?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue(dest_name))
    val producer = senderSession.createProducer(queue(dest_name))

    producer.send(senderSession.createTextMessage("Failover To NonExclusive Consumer - 1"))

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(receive_timeout) should not be (null)
    fallbackConsumer.receive(100) should be(null)

    // Close the exclusive consumer to verify the non-exclusive consumer
    // takes over
    exclusiveConsumer.close()

    producer.send(senderSession.createTextMessage("Failover To NonExclusive Consumer - 2"))
    fallbackConsumer.receive(receive_timeout) should not be (null)
    fallbackConsumer.close()
  }

  test("Fallback To Exclusive Consumer") {
    connect()
    val dest_name = next_id("TEST.QUEUE")

    val exclusiveSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    var exclusiveConsumer = exclusiveSession.createConsumer(queue(dest_name+"?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue(dest_name))
    val producer = senderSession.createProducer(queue(dest_name))

    producer.send(senderSession.createTextMessage("Fallback To Exclusive Consumer - 1"))

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(receive_timeout) should not be (null)
    fallbackConsumer.receive(200) should be(null)

    Thread.sleep(100)

    // Close the exclusive consumer to verify the non-exclusive consumer
    // takes over
    exclusiveConsumer.close()

    producer.send(senderSession.createTextMessage("Fallback To Exclusive Consumer - 2"))
    fallbackConsumer.receive(receive_timeout) should not be (null)

    // Create exclusive consumer to determine if it will start receiving
    // the messages.
    exclusiveConsumer = exclusiveSession.createConsumer(queue(dest_name+"?consumer.exclusive=true"))

    producer.send(senderSession.createTextMessage("Fallback To Exclusive Consumer - 3"))

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(receive_timeout) should not be (null)
    fallbackConsumer.receive(100) should be(null)
  }

  test("Temp Queue Destinations") {
    test_temp_destination((session: Session) => session.createTemporaryQueue())
  }

  test("Temp Topic Destinations") {
    test_temp_destination((session: Session) => session.createTemporaryTopic())
  }

  def test_temp_destination(func: (Session) => Destination) = {
    connect()

    val connection2 = connect("", true)
    val session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val dest = func(session);
    val consumer = session.createConsumer(dest)

    val producer2 = session2.createProducer(dest)

    def put(id: Int) = producer2.send(session.createTextMessage("message:" + id))
    def get(id: Int) = {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(dest)
      m.getText should equal("message:" + id)
    }

    Thread.sleep(1000);
    List(1, 2, 3).foreach(put _)
    List(1, 2, 3).foreach(get _)

    // A different connection should not be able to consume from it.
    try {
      session2.createConsumer(dest)
      fail("expected jms exception")
    } catch {
      case e: JMSException => println(e)
    }

    // delete the temporary destination.
    consumer.close()
    dest match {
      case dest: TemporaryQueue => dest.delete()
      case dest: TemporaryTopic => dest.delete()
    }

    // The producer should no longer be able to send to it.
    Thread.sleep(1000);
    try {
      put(4)
      fail("expected jms exception")
    } catch {
      case e: JMSException => println(e)
    }

  }

  test("Topic drops messages sent before before subscription is established") {
    connect()
    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val dest = topic(next_id("example"))

    val producer = session.createProducer(dest)
    def put(id: Int) {
      producer.send(session.createTextMessage("message:" + id))
    }

    put(1)

    val consumer = session.createConsumer(dest)

    put(2)
    put(3)

    def get(id: Int) {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(dest)
      m.getText should equal("message:" + id)
    }

    List(2, 3).foreach(get _)
  }

  test("Queue Message Cached") {

    connect("?wireFormat.cacheEnabled=false&wireFormat.tightEncodingEnabled=false")

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val dest = queue(next_id("example"))
    val producer = session.createProducer(dest)
    def put(id: Int) {
      producer.send(session.createTextMessage("message:" + id))
    }

    List(1, 2, 3).foreach(put _)

    val consumer = session.createConsumer(dest)

    def get(id: Int) {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(dest)
      m.getText should equal("message:" + id)
    }

    List(1, 2, 3).foreach(get _)
  }

  test("Queue order preserved") {

    connect()

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val dest = queue(next_id("example"))
    val producer = session.createProducer(dest)
    def put(id: Int) {
      producer.send(session.createTextMessage("message:" + id))
    }

    List(1, 2, 3).foreach(put _)

    val consumer = session.createConsumer(dest)

    def get(id: Int) {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(dest)
      m.getText should equal("message:" + id)
    }

    List(1, 2, 3).foreach(get _)
  }

  test("Test that messages are consumed ") {

    connect()
    var session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val queue = session.createQueue(next_id("test"));
    val producer = session.createProducer(queue);
    producer.send(session.createTextMessage("Hello"));

    // Consume the message...
    var consumer = session.createConsumer(queue)
    var msg = consumer.receive(receive_timeout)
    msg should not be (null)

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
    var dest = queue(next_id("example"))
    val producer = session.createProducer(dest)

    def put(id: Int, color: String) {
      val message = session.createTextMessage("message:" + id)
      message.setStringProperty("color", color)
      producer.send(message)
    }

    List((1, "red"), (2, "blue"), (3, "red")).foreach {
      case (id, color) => put(id, color)
    }

    val consumer = session.createConsumer(dest, "color='red'")

    def get(id: Int) {
      val m = consumer.receive().asInstanceOf[TextMessage]
      m.getJMSDestination should equal(dest)
      m.getText should equal("message:" + id)
    }

    get(1)
    get(3)
  }

  test("Receive then Browse and then Receive again") {
    connect()

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val dest = queue(next_id("BROWSER.TEST.RB"))
    val producer = session.createProducer(dest)
    var consumer = session.createConsumer(dest)

    val outbound = List(session.createTextMessage("First Message"),
      session.createTextMessage("Second Message"),
      session.createTextMessage("Third Message"))

    // lets consume any outstanding messages from previous test runs
    while (consumer.receive(1000) != null) {
    }

    producer.send(outbound(0));
    producer.send(outbound(1));
    producer.send(outbound(2));

    consumer.receive(receive_timeout) should be(outbound(0))

    consumer.close();

    val browser = session.createBrowser(dest)
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
    consumer = session.createConsumer(dest);
    // Receive the second.
    consumer.receive(receive_timeout) should be(outbound(1))
    // Receive the third.
    consumer.receive(receive_timeout) should be(outbound(2))
    consumer.close()
  }

  test("Browse Queue then Receive messages") {
    connect()

    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val dest = queue(next_id("BROWSER.TEST.BR"))
    val producer = session.createProducer(dest)

    val outbound = List(session.createTextMessage("First Message"),
      session.createTextMessage("Second Message"),
      session.createTextMessage("Third Message"))

    producer.send(outbound(0))

    // create browser first
    val browser = session.createBrowser(dest)
    val enumeration = browser.getEnumeration

    // create consumer
    val consumer = session.createConsumer(dest)

    // browse the first message
    enumeration.hasMoreElements should be(true)
    enumeration.nextElement() should be(outbound(0))

    // Receive the first message.
    consumer.receive(receive_timeout) should be(outbound(0))
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
    val destination = queue(next_id("BROWSER.TEST.BC"))

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
    consumer.receive(receive_timeout) should equal(outbound(0))
    consumer.receive(receive_timeout) should equal(outbound(1))
    consumer.receive(receive_timeout) should equal(outbound(2))
  }

  test("Unsubscribe invalid durable sub") {
    connect()
    val session = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    try {
      session.unsubscribe("does not exist")
      fail("Expecting JMS Exception")
    } catch {
      case e:JMSException =>
      case e:Throwable => fail("Expecting JMS Exception")
    }
 }

  test("NoLocal Test") {
    connect()
    val destination = topic(next_id("NOLOCAL"))
    val localSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    var localConsumer = localSession.createConsumer(destination, null, true)
    var localProducer = localSession.createProducer(destination)

    val remoteConnection = connect("")
    val remoteSession = remoteConnection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    var remoteProducer = remoteSession.createProducer(destination)

    remoteProducer.send(localSession.createTextMessage("1"))
    localProducer.send(localSession.createTextMessage("2"))
    remoteProducer.send(localSession.createTextMessage("3"))

    receive_text(localConsumer) should equal("1")
    receive_text(localConsumer) should equal("3")
 }

  test("Rollback moves messages to DLQ"){
    connect()
    val redelivery_policy = new RedeliveryPolicy
    redelivery_policy.setMaximumRedeliveries(1)
    default_connection.asInstanceOf[ActiveMQConnection].setRedeliveryPolicy(redelivery_policy)

    // send our message
    val producerSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val destination = producerSession.createQueue("nacker.test")
    val producer = producerSession.createProducer(destination)
    producer.send(producerSession.createTextMessage("yo"))
    producerSession.close()

    // consume and rollback
    val consumerSession = default_connection.createSession(true, Session.SESSION_TRANSACTED)
    val destination2 = consumerSession.createQueue("nacker.test")
    val consumer = consumerSession.createConsumer(destination2)

    var msg = consumer.receive(1000)
    msg should not be null
    consumerSession.rollback()

    msg = consumer.receive(1500)
    msg should not be null
    consumerSession.rollback()

    consumerSession.close()

    val dlqSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val dlq = dlqSession.createQueue("dlq.nacker.test")
    val dlqConsumer = dlqSession.createConsumer(dlq)
    val dlqMsg = dlqConsumer.receive(1000)
    dlqMsg should not be null
  }
}