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
import org.apache.activemq.apollo.openwire.command.ActiveMQQueue

class ExclusiveConsumerTest extends OpenwireTestSupport {

  test("Exclusive Consumer Selected when created first") {

    connect();

    val exclusiveSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val exclusiveConsumer = exclusiveSession.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue("TEST.QUEUE1"));
    val producer = senderSession.createProducer(queue("TEST.QUEUE1"));

    val msg = senderSession.createTextMessage("test");
    producer.send(msg);

    Thread.sleep(100);

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(100) should not be(null)
    fallbackConsumer.receive(100) should be(null)
  }

  test("Exclusive Consumer Selected when Created After Non-Exclusive Consumer") {

    connect();

    val exclusiveSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val fallbackConsumer = fallbackSession.createConsumer(queue("TEST.QUEUE1"));
    val exclusiveConsumer = exclusiveSession.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val producer = senderSession.createProducer(queue("TEST.QUEUE1"));

    val msg = senderSession.createTextMessage("test");
    producer.send(msg);

    Thread.sleep(100);

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(100) should not be(null)
    fallbackConsumer.receive(100) should be(null)
  }

  test("Failover To Another Exclusive Consumer Created First") {
    connect();

    val exclusiveSession1 = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val exclusiveSession2 = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val exclusiveConsumer1 = exclusiveSession1.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val exclusiveConsumer2 = exclusiveSession2.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue("TEST.QUEUE1"));
    val producer = senderSession.createProducer(queue("TEST.QUEUE1"));

    val msg = senderSession.createTextMessage("test");
    producer.send(msg);

    Thread.sleep(100);

    // Verify exclusive consumer receives the message.
    exclusiveConsumer1.receive(100) should not be(null)
    exclusiveConsumer2.receive(100) should be(null)
    fallbackConsumer.receive(100) should be(null)

    // Close the exclusive consumer to verify the non-exclusive consumer
    // takes over
    exclusiveConsumer1.close()

    producer.send(msg);
    producer.send(msg);

    exclusiveConsumer2.receive(100) should not be(null)
    fallbackConsumer.receive(100) should be(null)
  }

  test("Failover To Another Exclusive Consumer Created After a non-exclusive Consumer") {

    connect();

    val exclusiveSession1 = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val exclusiveSession2 = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val exclusiveConsumer1 = exclusiveSession1.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue("TEST.QUEUE1"));
    val exclusiveConsumer2 = exclusiveSession2.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val producer = senderSession.createProducer(queue("TEST.QUEUE1"));

    val msg = senderSession.createTextMessage("test");
    producer.send(msg);

    Thread.sleep(100);

    // Verify exclusive consumer receives the message.
    exclusiveConsumer1.receive(100) should not be(null)
    exclusiveConsumer2.receive(100) should be(null)
    fallbackConsumer.receive(100) should be(null)

    // Close the exclusive consumer to verify the non-exclusive consumer
    // takes over
    exclusiveConsumer1.close()

    producer.send(msg);
    producer.send(msg);

    exclusiveConsumer2.receive(100) should not be(null)
    fallbackConsumer.receive(100) should be(null)
  }

  test("Failover To NonExclusive Consumer") {

    connect();

    val exclusiveSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    val exclusiveConsumer = exclusiveSession.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue("TEST.QUEUE1"));
    val producer = senderSession.createProducer(queue("TEST.QUEUE1"));

    val msg = senderSession.createTextMessage("test");
    producer.send(msg);

    Thread.sleep(100);

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(100) should not be(null)
    fallbackConsumer.receive(100) should be(null)

    // Close the exclusive consumer to verify the non-exclusive consumer
    // takes over
    exclusiveConsumer.close()

    producer.send(msg);
    fallbackConsumer.receive(100) should not be(null)
  }

  test("Fallback To Exclusive Consumer") {
    connect();

    val exclusiveSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val fallbackSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val senderSession = default_connection.createSession(false, Session.AUTO_ACKNOWLEDGE)

    var exclusiveConsumer = exclusiveSession.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))
    val fallbackConsumer = fallbackSession.createConsumer(queue("TEST.QUEUE1"));
    val producer = senderSession.createProducer(queue("TEST.QUEUE1"));

    val msg = senderSession.createTextMessage("test");
    producer.send(msg);

    Thread.sleep(100);

    // Verify exclusive consumer receives the message.
    assert(exclusiveConsumer.receive(100) != null, "The exclusive consumer should have got a Message");
    assert(fallbackConsumer.receive(100) == null, "The non-exclusive consumer shouldn't have a message");

    // Close the exclusive consumer to verify the non-exclusive consumer
    // takes over
    exclusiveConsumer.close()

    producer.send(msg)
    assert(fallbackConsumer.receive(100) != null, "The non-exclusive consumer should have a message");

    // Create exclusive consumer to determine if it will start receiving
    // the messages.
    exclusiveConsumer = exclusiveSession.createConsumer(queue("TEST.QUEUE1?consumer.exclusive=true"))

    producer.send(msg)

    // Verify exclusive consumer receives the message.
    exclusiveConsumer.receive(100) should not be(null)
//    fallbackConsumer.receive(100) should be(null)
  }

}