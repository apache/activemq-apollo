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

import javax.jms.{TextMessage, Session}

abstract class ExclusiveConsumerTest extends OpenwireTestSupport {

}

class ExclusiveConsumerSelectedWhenCreatedFirst extends ExclusiveConsumerTest {

  test("Exclusive Consumer Selected when created first") {

    connect()

    val exclusiveSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val exclusiveConsumer = exclusiveSession.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue("TEST.QUEUE1"))
    val producer = senderSession.createProducer(queue("TEST.QUEUE1"))

    producer.send(senderSession.createTextMessage("Exclusive Consumer Selected when created first - 1"))

    Thread.sleep(100)

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(100) should not be (null)
    fallbackConsumer.receive(100) should be(null)
  }
}

class ExclusiveConsumerSelectedWhenCreatedAfterNonExclusive extends ExclusiveConsumerTest {

  test("Exclusive Consumer Selected when Created After Non-Exclusive Consumer") {
    connect()

    val exclusiveSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val fallbackConsumer = fallbackSession.createConsumer(queue("TEST.QUEUE1"))
    val exclusiveConsumer = exclusiveSession.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val producer = senderSession.createProducer(queue("TEST.QUEUE1"))

    val msg = senderSession.createTextMessage("test")
    producer.send(msg);

    Thread.sleep(100)

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(100) should not be (null)
    fallbackConsumer.receive(100) should be(null)
  }
}

class FailoverToAnotherExclusiveConsumerCreateFirst extends ExclusiveConsumerTest {

  test("Failover To Another Exclusive Consumer Created First") {

    connect()

    val exclusiveSession1 = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val exclusiveSession2 = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val exclusiveConsumer1 = exclusiveSession1.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val exclusiveConsumer2 = exclusiveSession2.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue("TEST.QUEUE1"))
    val producer = senderSession.createProducer(queue("TEST.QUEUE1"))

    producer.send(senderSession.createTextMessage("Failover To Another Exclusive Consumer Created First - 1"))

    Thread.sleep(100)

    // Verify exclusive consumer receives the message.
    exclusiveConsumer1.receive(100) should not be (null)
    exclusiveConsumer2.receive(100) should be(null)
    fallbackConsumer.receive(100) should be(null)

    // Close the exclusive consumer to verify the non-exclusive consumer
    // takes over
    exclusiveConsumer1.close()

    producer.send(senderSession.createTextMessage("Failover To Another Exclusive Consumer Created First - 2"))
    producer.send(senderSession.createTextMessage("Failover To Another Exclusive Consumer Created First - 3"))

    exclusiveConsumer2.receive(100) should not be (null)
    fallbackConsumer.receive(100) should be(null)
  }
}

class FailoverToAnotherExclusiveConsumerCreateAfterNonExclusive extends ExclusiveConsumerTest {

  test("Failover To Another Exclusive Consumer Created After a non-exclusive Consumer") {

    connect()

    val exclusiveSession1 = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val exclusiveSession2 = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val exclusiveConsumer1 = exclusiveSession1.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue("TEST.QUEUE1"))
    val exclusiveConsumer2 = exclusiveSession2.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val producer = senderSession.createProducer(queue("TEST.QUEUE1"))

    producer.send(senderSession.createTextMessage("Failover To Another Exclusive Consumer Created After - 1"));

    Thread.sleep(100)

    // Verify exclusive consumer receives the message.
    exclusiveConsumer1.receive(100) should not be (null)
    exclusiveConsumer2.receive(100) should be(null)
    fallbackConsumer.receive(100) should be(null)

    // Close the exclusive consumer to verify the non-exclusive consumer
    // takes over
    exclusiveConsumer1.close()

    producer.send(senderSession.createTextMessage("Failover To Another Exclusive Consumer Created After - 2"))
    producer.send(senderSession.createTextMessage("Failover To Another Exclusive Consumer Created After - 3"))

    exclusiveConsumer2.receive(100) should not be (null)
    fallbackConsumer.receive(100) should be(null)
  }
}

class FailoverToNonExclusiveConsumer extends ExclusiveConsumerTest {

  test("Failover To NonExclusive Consumer") {

    info("*** Running Test: Failover To NonExclusive Consumer")

    connect()

    val exclusiveSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val exclusiveConsumer = exclusiveSession.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue("TEST.QUEUE1"))
    val producer = senderSession.createProducer(queue("TEST.QUEUE1"))

    producer.send(senderSession.createTextMessage("Failover To NonExclusive Consumer - 1"))

    Thread.sleep(100)

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(100) should not be (null)
    fallbackConsumer.receive(100) should be(null)

    // Close the exclusive consumer to verify the non-exclusive consumer
    // takes over
    exclusiveConsumer.close()

    producer.send(senderSession.createTextMessage("Failover To NonExclusive Consumer - 2"))
    fallbackConsumer.receive(100) should not be (null)
    fallbackConsumer.close()
  }
}

class FallbackToAnotherExclusiveConsumer extends ExclusiveConsumerTest {

  test("Fallback To Exclusive Consumer") {
    connect()

    val exclusiveSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    var exclusiveConsumer = exclusiveSession.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue("TEST.QUEUE1"))
    val producer = senderSession.createProducer(queue("TEST.QUEUE1"))

    producer.send(senderSession.createTextMessage("Fallback To Exclusive Consumer - 1"))

    Thread.sleep(100)

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(200) should not be (null)
    fallbackConsumer.receive(200) should be(null)

    Thread.sleep(100)

    // Close the exclusive consumer to verify the non-exclusive consumer
    // takes over
    exclusiveConsumer.close()

    producer.send(senderSession.createTextMessage("Fallback To Exclusive Consumer - 2"))
    fallbackConsumer.receive(100) should not be (null)

    // Create exclusive consumer to determine if it will start receiving
    // the messages.
    exclusiveConsumer = exclusiveSession.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))

    producer.send(senderSession.createTextMessage("Fallback To Exclusive Consumer - 3"))

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(100) should not be (null)
    fallbackConsumer.receive(100) should be(null)
  }

}