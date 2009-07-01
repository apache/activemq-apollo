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
package org.apache.activemq.legacy.openwireprotocol;

import java.util.ArrayList;

import javax.jms.DeliveryMode;

import junit.framework.Test;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.openwire.BrokerTestSupport;

public class BrokerTest extends BrokerTestSupport {

    public ActiveMQDestination destination;
    public int deliveryMode;
    public int prefetch;
    public byte destinationType;
    public boolean durableConsumer;
    protected static final int MAX_NULL_WAIT=500;
    
    public void initCombosForTestGroupedMessagesDeliveredToOnlyOneConsumer() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
    }

    public void testGroupedMessagesDeliveredToOnlyOneConsumer() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(1);
        connection1.request(consumerInfo1);
        
        ArrayList<Message> msgs = new ArrayList<Message>(4);

        // Send the messages.
        for (int i = 0; i < 1; i++) {
            Message message = createMessage(producerInfo, destination, deliveryMode);
            message.setGroupID("TEST-GROUP");
            message.setGroupSequence(i + 1);
            msgs.add(i, message);
            connection1.request(message);
        }
        
        // Setup a second connection
        StubConnection connection2 = createConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);

        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(1);
        connection2.request(consumerInfo2);
        
        // Send the messages.
        for (int i = 1; i < 4; i++) {
            Message message = createMessage(producerInfo, destination, deliveryMode);
            message.setGroupID("TEST-GROUP");
            message.setGroupSequence(i + 1);
            msgs.add(i, message);
            connection1.request(message);
        }

        // All the messages should have been sent down connection 1.. just get
        // the first 3
        for (int i = 0; i < 3; i++) {
            Message m1 = receiveMessage(connection1);
            assertEquals("Unexpected Message Receipt: " + m1, m1.getMessageId(), msgs.get(i).getMessageId());
            connection1.send(createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        // Close the first consumer.
        connection1.request(closeConsumerInfo(consumerInfo1));

        // The last messages should now go the the second consumer.
        for (int i = 3; i < 4; i++) {
            Message m1 = receiveMessage(connection2);
            assertEquals("Unexpected Message Receipt: " + m1, m1.getMessageId(), msgs.get(i).getMessageId());
            connection2.request(createAck(consumerInfo2, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        assertNoMessagesLeft(connection2);
    }

    public void initCombosForTestTransactedAckWithPrefetchOfOne() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testTransactedAckWithPrefetchOfOne() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        destination = createDestinationInfo(connection1, connectionInfo1, destinationType);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(1);
        connection1.send(consumerInfo1);

        // Send the messages
        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo1, destination, deliveryMode);
            connection1.send(message);
        }

       

        // Now get the messages.
        for (int i = 0; i < 4; i++) {
            // Begin the transaction.
            LocalTransactionId txid = createLocalTransaction(sessionInfo1);
            connection1.send(createBeginTransaction(connectionInfo1, txid));
            Message m1 = receiveMessage(connection1);
            assertNotNull(m1);
            MessageAck ack = createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection1.send(ack);
         // Commit the transaction.
            connection1.send(createCommitTransaction1Phase(connectionInfo1, txid));
        }
        assertNoMessagesLeft(connection1);
    }

    public void initCombosForTestTransactedSend() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TOPIC_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)});
    }

    public void testTransactedSend() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        destination = createDestinationInfo(connection1, connectionInfo1, destinationType);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // Begin the transaction.
        LocalTransactionId txid = createLocalTransaction(sessionInfo1);
        connection1.send(createBeginTransaction(connectionInfo1, txid));

        // Send the messages
        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo1, destination, deliveryMode);
            message.setTransactionId(txid);
            connection1.request(message);
        }

        // The point of this test is that message should not be delivered until
        // send is committed.
        assertNull(receiveMessage(connection1,MAX_NULL_WAIT));

        // Commit the transaction.
        connection1.send(createCommitTransaction1Phase(connectionInfo1, txid));

        // Now get the messages.
        for (int i = 0; i < 4; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull(m1);
        }

        assertNoMessagesLeft(connection1);
    }

    public void initCombosForTestQueueTransactedAck() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType",
                             new Object[] {Byte.valueOf(ActiveMQDestination.QUEUE_TYPE),
                                           Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE)});
    }

    public void testQueueTransactedAck() throws Exception {

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        destination = createDestinationInfo(connection1, connectionInfo1, destinationType);

        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // Send the messages
        for (int i = 0; i < 4; i++) {
            Message message = createMessage(producerInfo1, destination, deliveryMode);
            connection1.send(message);
        }

        // Begin the transaction.
        LocalTransactionId txid = createLocalTransaction(sessionInfo1);
        connection1.send(createBeginTransaction(connectionInfo1, txid));

        // Acknowledge the first 2 messages.
        for (int i = 0; i < 2; i++) {
            Message m1 = receiveMessage(connection1);
            assertNotNull("m1 is null for index: " + i, m1);
            MessageAck ack = createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection1.request(ack);
        }

        // Commit the transaction.
        connection1.send(createCommitTransaction1Phase(connectionInfo1, txid));

        // The queue should now only have the remaining 2 messages
        assertEquals(2, countMessagesInQueue(connection1, connectionInfo1, destination));
    }
    
    public void testTopicRetroactiveConsumerSeeMessagesBeforeCreation() throws Exception {

        ActiveMQDestination destination = new ActiveMQTopic("TEST");

        // Setup a first connection
        StubConnection connection1 = createConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        connectionInfo1.setClientId("A");
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        // Send the messages
        Message m = createMessage(producerInfo1, destination, deliveryMode);
        connection1.send(m);

        // Create the durable subscription.
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        if (durableConsumer) {
            consumerInfo1.setSubscriptionName("test");
        }
        consumerInfo1.setPrefetchSize(100);
        consumerInfo1.setRetroactive(true);
        connection1.send(consumerInfo1);

        connection1.send(createMessage(producerInfo1, destination, deliveryMode));
        connection1.request(createMessage(producerInfo1, destination, deliveryMode));

        // the behavior is VERY dependent on the recovery policy used.
        // But the default broker settings try to make it as consistent as
        // possible

        // Subscription should see all messages sent.
        Message m2 = receiveMessage(connection1);
        assertNotNull(m2);
        assertEquals(m.getMessageId(), m2.getMessageId());
        for (int i = 0; i < 2; i++) {
            m2 = receiveMessage(connection1);
            assertNotNull(m2);
        }

        assertNoMessagesLeft(connection1);
    }

    public static Test suite() {
        return suite(BrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
