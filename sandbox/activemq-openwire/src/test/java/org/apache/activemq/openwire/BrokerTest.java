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
package org.apache.activemq.openwire;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.jms.DeliveryMode;

import org.apache.activemq.apollo.Combinator;
import org.apache.activemq.apollo.Combinator.BeanFactory;
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
import org.apache.activemq.legacy.openwireprotocol.StubConnection;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@RunWith(Theories.class)
public class BrokerTest  {

    public static @DataPoints
    BrokerTestScenario[] SCENARIOS;
    
    public static final ThreadLocal<BrokerTestScenario> CURRENT_SCENERIO = new ThreadLocal<BrokerTestScenario>();

    public static BeanFactory<BrokerTestScenario> scenarioFactory() {
        return new BeanFactory<BrokerTestScenario>() {
            public BrokerTestScenario createBean() throws Exception {
                return new BrokerTestScenario();
            }

            public Class<BrokerTestScenario> getBeanClass() {
                return BrokerTestScenario.class;
            }
        };
    }
    
    @BeforeClass
    static public void createScenarios() throws Exception {
        SCENARIOS = combinations().asBeans(scenarioFactory());
    }

    public static Combinator combinations() {
        return new Combinator().put("deliveryMode", 
              DeliveryMode.PERSISTENT, 
              DeliveryMode.NON_PERSISTENT)
        .put("destinationType", 
              ActiveMQDestination.QUEUE_TYPE, 
              ActiveMQDestination.TOPIC_TYPE, 
              ActiveMQDestination.TEMP_QUEUE_TYPE, 
              ActiveMQDestination.TEMP_TOPIC_TYPE)
        .put("durableConsumer", false)
        // Add in the durable consumer combinations..
        .and()
        .put("deliveryMode", 
              DeliveryMode.PERSISTENT, 
              DeliveryMode.NON_PERSISTENT)
        .put("destinationType", 
              ActiveMQDestination.TOPIC_TYPE)
        .put("durableConsumer", true);
    }

    private void start(BrokerTestScenario scenario) throws Exception {
        // Start the scenario and store it in a thread local so that it can
        // be cleaned up automatically,
        // in the after method.
        CURRENT_SCENERIO.set(scenario);
        scenario.start();
    }

    @After
    public void after() {
        // If the CURRENT_SCENERIO is set, then we need to clean it up.
        BrokerTestScenario scenario = CURRENT_SCENERIO.get();
        if (scenario != null) {
            CURRENT_SCENERIO.set(null);
            try {
                scenario.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    

	
	
//	
//	@DataProvider(name = "default")
//	public Object[][] createData0(Method method) throws Exception {
//		return combinator()
//			.put("deliveryMode", 0)
//			.combinationsAsBeans(factory());
//	}
//
    private void assumeDefault(BrokerTestScenario scenario) {
        assumeQueueDestination(scenario);
        assumeThat(scenario.deliveryMode, is(DeliveryMode.NON_PERSISTENT));
    }

    //	@DataProvider(name = "deliveryMode-combinations")
//	public Object[][] createData1(Method method) throws Exception {
//		return combinator()
//			.put("deliveryMode", DeliveryMode.PERSISTENT, DeliveryMode.NON_PERSISTENT)
//			.combinationsAsBeans(factory());
//	}
    
    
    private void assumeQueueDestination(BrokerTestScenario scenario) {
        assumeThat(scenario.destinationType, is(ActiveMQDestination.QUEUE_TYPE));
    }
    
//	
//	@DataProvider(name = "deliveryMode-queue-combinations")
//	public Object[][] createData2(Method method) throws Exception {
//		return combinator()
//			.put("deliveryMode", DeliveryMode.PERSISTENT, DeliveryMode.NON_PERSISTENT)
//			.put("destinationType", ActiveMQDestination.QUEUE_TYPE, ActiveMQDestination.TEMP_QUEUE_TYPE)
//			.combinationsAsBeans(factory());
//	}
    
    @SuppressWarnings("unchecked")
    private void assumeDeliveryModeQueue(BrokerTestScenario scenario) {
        assumeThat(scenario.destinationType, anyOf(
                is(ActiveMQDestination.QUEUE_TYPE),
                is(ActiveMQDestination.TEMP_QUEUE_TYPE)
                ));
    }
    
//	
//	@DataProvider(name = "deliveryMode-perm-destinations-combinations")
//	public Object[][] createData3(Method method) throws Exception {
//		return combinator()
//			.put("deliveryMode", DeliveryMode.PERSISTENT, DeliveryMode.NON_PERSISTENT)
//			.put("destinationType", ActiveMQDestination.QUEUE_TYPE, ActiveMQDestination.TOPIC_TYPE)
//			.combinationsAsBeans(factory());
//	}	
    
    @SuppressWarnings("unchecked")
    private void assumeDeliveryPermDest(BrokerTestScenario scenario) {
        assumeThat(scenario.destinationType, anyOf(
                is(ActiveMQDestination.QUEUE_TYPE),
                is(ActiveMQDestination.TOPIC_TYPE)
                ));
        assumeThat(scenario.durableConsumer, is(false));
    }
    
//
//	@DataProvider(name = "deliveryMode-all-destinations-combinations")
//	public Object[][] createData4(Method method) throws Exception {
//		return combinator()
//			.put("deliveryMode", DeliveryMode.PERSISTENT, DeliveryMode.NON_PERSISTENT)
//			.put("destinationType", ActiveMQDestination.QUEUE_TYPE, ActiveMQDestination.TOPIC_TYPE, 
//									ActiveMQDestination.TEMP_QUEUE_TYPE, ActiveMQDestination.TEMP_TOPIC_TYPE)
//			.combinationsAsBeans(factory());
//	}
//	
    private void assumeDeliveryAllDest(BrokerTestScenario scenario) {
        assumeThat(scenario.durableConsumer, is(false));
    }

//	@DataProvider(name = "deliveryMode-durableConsumer-combinations")
//	public Object[][] createData5(Method method) throws Exception {
//		return combinator()
//			.put("deliveryMode", DeliveryMode.PERSISTENT, DeliveryMode.NON_PERSISTENT)
//			.put("durableConsumer", true, false)
//			.combinationsAsBeans(factory());
//	}	
	
    private void assumeDeliveryDurableConsumer(BrokerTestScenario scenario) {
        assumeThat(scenario.destinationType, is(ActiveMQDestination.TOPIC_TYPE));
    }

//	@Test(dataProvider = "deliveryMode-combinations")
    @Theory
    public void testTopicNoLocal(BrokerTestScenario scenario) throws Exception {
        assumeQueueDestination(scenario);
        start(scenario);
        

        ActiveMQDestination destination = new ActiveMQTopic("TEST");

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setRetroactive(true);
        consumerInfo1.setPrefetchSize(100);
        consumerInfo1.setNoLocal(true);
        connection1.send(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = scenario.createConnection();
        ConnectionInfo connectionInfo2 = scenario.createConnectionInfo();
        SessionInfo sessionInfo2 = scenario.createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = scenario.createProducerInfo(sessionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(producerInfo2);

        ConsumerInfo consumerInfo2 = scenario.createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setRetroactive(true);
        consumerInfo2.setPrefetchSize(100);
        consumerInfo2.setNoLocal(true);
        connection2.request(consumerInfo2);

        // Send the messages
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));

        // The 2nd connection should get the messages.
        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection2);
            assertNotNull(("Message: "+i), m1);
        }

        // Send a message with the 2nd connection
        Message message = scenario.createMessage(producerInfo2, destination, scenario.deliveryMode);
        connection2.send(message);

        // The first connection should not see the initial 4 local messages sent
        // but should
        // see the messages from connection 2.
        Message m = scenario.receiveMessage(connection1);
        assertNotNull(m);
        assertEquals(m.getMessageId(), message.getMessageId());

        scenario.assertNoMessagesLeft(connection1);
        scenario.assertNoMessagesLeft(connection2);
    }

//	@Test(dataProvider = "deliveryMode-queue-combinations")
    @Theory
    public void testQueueSendThenAddConsumer(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryModeQueue(scenario);
        start(scenario);

        // Start a producer
        StubConnection connection = scenario.createConnection();
        ConnectionInfo connectionInfo = scenario.createConnectionInfo();
        SessionInfo sessionInfo = scenario.createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        ActiveMQDestination destination = scenario.createDestinationInfo(connection, connectionInfo, scenario.destinationType);

        // Send a message to the broker.
        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        // Start the consumer
        ConsumerInfo consumerInfo = scenario.createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        // Make sure the message was delivered.
        Message m = scenario.receiveMessage(connection);
        assertNotNull(m);

    }

//	@Test(dataProvider = "deliveryMode-perm-destinations-combinations")
    @Theory
    public void testCompositeSend(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryPermDest(scenario);
        start(scenario);

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        ActiveMQDestination destinationA = ActiveMQDestination.createDestination("A", scenario.destinationType);
        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destinationA);
        consumerInfo1.setRetroactive(true);
        consumerInfo1.setPrefetchSize(100);
        connection1.request(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = scenario.createConnection();
        ConnectionInfo connectionInfo2 = scenario.createConnectionInfo();
        SessionInfo sessionInfo2 = scenario.createSessionInfo(connectionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);

        ActiveMQDestination destinationB = ActiveMQDestination.createDestination("B", scenario.destinationType);
        ConsumerInfo consumerInfo2 = scenario.createConsumerInfo(sessionInfo2, destinationB);
        consumerInfo2.setRetroactive(true);
        consumerInfo2.setPrefetchSize(100);
        connection2.request(consumerInfo2);

        // Send the messages to the composite scenario.destination.
        ActiveMQDestination compositeDestination = ActiveMQDestination.createDestination("A,B",
                                                                                         scenario.destinationType);
        for (int i = 0; i < 4; i++) {
            connection1.request(scenario.createMessage(producerInfo1, compositeDestination, scenario.deliveryMode));
        }

        // The messages should have been delivered to both the A and B
        // scenario.destination.
        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            Message m2 = scenario.receiveMessage(connection2);

            assertNotNull(m1);
            assertNotNull(m2);

            assertEquals(m2.getMessageId(), m1.getMessageId());
            assertEquals(m1.getOriginalDestination(), compositeDestination);
            assertEquals(m2.getOriginalDestination(), compositeDestination);

            connection1.request(scenario.createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
            connection2.request(scenario.createAck(consumerInfo2, m2, 1, MessageAck.STANDARD_ACK_TYPE));

        }

        scenario.assertNoMessagesLeft(connection1);
        scenario.assertNoMessagesLeft(connection2);

        connection1.send(scenario.closeConnectionInfo(connectionInfo1));
        connection2.send(scenario.closeConnectionInfo(connectionInfo2));
    }

//	@Test(dataProvider = "deliveryMode-combinations")
    @Theory
    public void testQueueOnlyOnceDeliveryWith2Consumers(BrokerTestScenario scenario) throws Exception {
        assumeQueueDestination(scenario);
        start(scenario);

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(1);
        connection1.request(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = scenario.createConnection();
        ConnectionInfo connectionInfo2 = scenario.createConnectionInfo();
        SessionInfo sessionInfo2 = scenario.createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = scenario.createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(1);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(consumerInfo2);

        // Send the messages
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection1.request(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        for (int i = 0; i < 2; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            Message m2 = scenario.receiveMessage(connection2);

            assertNotNull(("m1 is null for index: " + i), m1);
            assertNotNull(("m2 is null for index: " + i), m2);

            assertNotSame(m1.getMessageId(), m2.getMessageId());
            connection1.send(scenario.createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
            connection2.send(scenario.createAck(consumerInfo2, m2, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        scenario.assertNoMessagesLeft(connection1);
        scenario.assertNoMessagesLeft(connection2);
    }

//	@Test(dataProvider = "deliveryMode-combinations")
    @Theory
    public void testQueueBrowserWith2Consumers(BrokerTestScenario scenario) throws Exception {
        assumeQueueDestination(scenario);
        start(scenario);

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(10);
        connection1.request(consumerInfo1);

        // Send the messages
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        //as the messages are sent async - need to synchronize the last
        //one to ensure they arrive in the order we want
        connection1.request(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        // Setup a second connection with a queue browser.
        StubConnection connection2 = scenario.createConnection();
        ConnectionInfo connectionInfo2 = scenario.createConnectionInfo();
        SessionInfo sessionInfo2 = scenario.createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = scenario.createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(1);
        consumerInfo2.setBrowser(true);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(consumerInfo2);

        List<Message> messages = new ArrayList<Message>();

        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(("m1 is null for index: " + i), m1);
            messages.add(m1);
        }

        for (int i = 0; i < 4; i++) {
            Message m1 = messages.get(i);
            Message m2 = scenario.receiveMessage(connection2);
            assertNotNull(("m2 is null for index: " + i), m2);
            assertEquals(m2.getMessageId(), m1.getMessageId());
            connection2.send(scenario.createAck(consumerInfo2, m2, 1, MessageAck.DELIVERED_ACK_TYPE));
        }

        scenario.assertNoMessagesLeft(connection1);
        scenario.assertNoMessagesLeft(connection2);
    }

    
    /*
     * change the order of the above test
     */
//	@Test(dataProvider = "default")
    @Theory
    public void testQueueBrowserWith2ConsumersBrowseFirst(BrokerTestScenario scenario) throws Exception {
        assumeDefault(scenario);
        start(scenario);

        ActiveMQDestination destination = new ActiveMQQueue("TEST");
        scenario.deliveryMode = DeliveryMode.NON_PERSISTENT;
        
        
        // Setup a second connection with a queue browser.
        StubConnection connection2 = scenario.createConnection();
        ConnectionInfo connectionInfo2 = scenario.createConnectionInfo();
        SessionInfo sessionInfo2 = scenario.createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = scenario.createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(10);
        consumerInfo2.setBrowser(true);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(consumerInfo2);

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(10);
        connection1.request(consumerInfo1);

        // Send the messages
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        //as the messages are sent async - need to synchronize the last
        //one to ensure they arrive in the order we want
        connection1.request(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));


        List<Message> messages = new ArrayList<Message>();

        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(("m1 is null for index: " + i), m1);
            messages.add(m1);
        }

        // no messages present in queue browser as there were no messages when it
        // was created
        scenario.assertNoMessagesLeft(connection1);
        scenario.assertNoMessagesLeft(connection2);
    }

//	@Test(dataProvider = "default")
    @Theory
    public void testQueueBrowserWith2ConsumersInterleaved(BrokerTestScenario scenario) throws Exception {
        assumeDefault(scenario);
        start(scenario);

        ActiveMQDestination destination = new ActiveMQQueue("TEST");
        scenario.deliveryMode = DeliveryMode.NON_PERSISTENT;
        
        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(10);
        connection1.request(consumerInfo1);

        // Send the messages
        connection1.request(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        
        // Setup a second connection with a queue browser.
        StubConnection connection2 = scenario.createConnection();
        ConnectionInfo connectionInfo2 = scenario.createConnectionInfo();
        SessionInfo sessionInfo2 = scenario.createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = scenario.createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(1);
        consumerInfo2.setBrowser(true);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(consumerInfo2);

        
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        //as the messages are sent async - need to synchronize the last
        //one to ensure they arrive in the order we want
        connection1.request(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        
        List<Message> messages = new ArrayList<Message>();

        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(("m1 is null for index: " + i), m1);
            messages.add(m1);
        }

        for (int i = 0; i < 1; i++) {
            Message m1 = messages.get(i);
            Message m2 = scenario.receiveMessage(connection2);
            assertNotNull(("m2 is null for index: " + i), m2);
            assertEquals(m2.getMessageId(), m1.getMessageId());
            connection2.send(scenario.createAck(consumerInfo2, m2, 1, MessageAck.DELIVERED_ACK_TYPE));
        }

        scenario.assertNoMessagesLeft(connection1);
        scenario.assertNoMessagesLeft(connection2);
    }

//	@Test(dataProvider = "deliveryMode-all-destinations-combinations")
    @Theory
    public void testConsumerPrefetchAndStandardAck(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryAllDest(scenario);
        start(scenario);

        // Start a producer and consumer
        StubConnection connection = scenario.createConnection();
        ConnectionInfo connectionInfo = scenario.createConnectionInfo();
        SessionInfo sessionInfo = scenario.createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        ActiveMQDestination destination = scenario.createDestinationInfo(connection, connectionInfo, scenario.destinationType);

        ConsumerInfo consumerInfo = scenario.createConsumerInfo(sessionInfo, destination);
        consumerInfo.setPrefetchSize(1);
        connection.send(consumerInfo);

        // Send 3 messages to the broker.
        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection.request(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        // Make sure only 1 message was delivered.
        Message m1 = scenario.receiveMessage(connection);
        assertNotNull(m1);
        scenario.assertNoMessagesLeft(connection);

        // Acknowledge the first message. This should cause the next message to
        // get dispatched.
        connection.send(scenario.createAck(consumerInfo, m1, 1, MessageAck.STANDARD_ACK_TYPE));

        Message m2 = scenario.receiveMessage(connection);
        assertNotNull(m2);
        connection.send(scenario.createAck(consumerInfo, m2, 1, MessageAck.STANDARD_ACK_TYPE));

        Message m3 = scenario.receiveMessage(connection);
        assertNotNull(m3);
        connection.send(scenario.createAck(consumerInfo, m3, 1, MessageAck.STANDARD_ACK_TYPE));

        connection.send(scenario.closeConnectionInfo(connectionInfo));
    }

    public void testConsumerCloseCausesRedelivery(BrokerTestScenario scenario) throws Exception {
        assumeQueueDestination(scenario);
        start(scenario);

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        ActiveMQDestination destination = new ActiveMQQueue("TEST");
        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        connection1.request(consumerInfo1);

        // Send the messages
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));

        // Receive the messages.
        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(("m1 is null for index: " + i), m1);
            assertFalse(m1.isRedelivered());
        }

        // Close the consumer without acking.. this should cause re-delivery of
        // the messages.
        connection1.send(consumerInfo1.createRemoveCommand());

        // Create another consumer that should get the messages again.
        ConsumerInfo consumerInfo2 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo2.setPrefetchSize(100);
        connection1.request(consumerInfo2);

        // Receive the messages.
        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(("m1 is null for index: " + i), m1);
            assertTrue(m1.isRedelivered());
        }
        scenario.assertNoMessagesLeft(connection1);

    }

//	@Test(dataProvider = "default")
    @Theory
    public void testTopicDurableSubscriptionCanBeRestored(BrokerTestScenario scenario) throws Exception {
        assumeDefault(scenario);
        start(scenario);

        ActiveMQDestination destination = new ActiveMQTopic("TEST");

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        connectionInfo1.setClientId("clientid1");
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        consumerInfo1.setSubscriptionName("test");
        connection1.send(consumerInfo1);

        // Send the messages
        connection1.send(scenario.createMessage(producerInfo1, destination, DeliveryMode.PERSISTENT));
        connection1.send(scenario.createMessage(producerInfo1, destination, DeliveryMode.PERSISTENT));
        connection1.send(scenario.createMessage(producerInfo1, destination, DeliveryMode.PERSISTENT));
        connection1.request(scenario.createMessage(producerInfo1, destination, DeliveryMode.PERSISTENT));

        // Get the messages
        Message m = null;
        for (int i = 0; i < 2; i++) {
            m = scenario.receiveMessage(connection1);
            assertNotNull(m);
        }
        // Ack the last message.
        connection1.send(scenario.createAck(consumerInfo1, m, 2, MessageAck.STANDARD_ACK_TYPE));
        // Close the connection.
        connection1.request(scenario.closeConnectionInfo(connectionInfo1));
        connection1.stop();

        // Setup a second connection
        StubConnection connection2 = scenario.createConnection();
        ConnectionInfo connectionInfo2 = scenario.createConnectionInfo();
        connectionInfo2.setClientId("clientid1");
        SessionInfo sessionInfo2 = scenario.createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = scenario.createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(100);
        consumerInfo2.setSubscriptionName("test");

        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(consumerInfo2);

        // Get the rest of the messages
        for (int i = 0; i < 2; i++) {
            Message m1 = scenario.receiveMessage(connection2);
            assertNotNull(("m1 is null for index: " + i), m1);
        }
        scenario.assertNoMessagesLeft(connection2);
    }

//	@Test(dataProvider = "deliveryMode-durableConsumer-combinations")
    @Theory
    public void testTopicConsumerOnlySeeMessagesAfterCreation(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryDurableConsumer(scenario);
        start(scenario);

        ActiveMQDestination destination = new ActiveMQTopic("TEST");

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        connectionInfo1.setClientId("A");
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        // Send the 1st message
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));

        // Create the durable subscription.
        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        if (scenario.durableConsumer) {
            consumerInfo1.setSubscriptionName("test");
        }
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        Message m = scenario.createMessage(producerInfo1, destination, scenario.deliveryMode);
        connection1.send(m);
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));

        // Subscription should skip over the first message
        Message m2 = scenario.receiveMessage(connection1);
        assertNotNull(m2);
        assertEquals(m2.getMessageId(), m.getMessageId());
        m2 = scenario.receiveMessage(connection1);
        assertNotNull(m2);

        scenario.assertNoMessagesLeft(connection1);
    }

//    public void initCombosForTestTopicRetroactiveConsumerSeeMessagesBeforeCreation() {
//        addCombinationValues("scenario.deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
//                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
//        addCombinationValues("scenario.durableConsumer", new Object[] {Boolean.TRUE, Boolean.FALSE});
//    }

    //
    // TODO: need to reimplement this since we don't fail when we send to a
    // non-existant
    // scenario.destination. But if we can access the Region directly then we should be
    // able to
    // check that if the scenario.destination was removed.
    // 
    // public void initCombosForTestTempDestinationsRemovedOnConnectionClose() {
    // addCombinationValues( "scenario.deliveryMode", new Object[]{
    // Integer.valueOf(DeliveryMode.NON_PERSISTENT),
    // Integer.valueOf(DeliveryMode.PERSISTENT)} );
    // addCombinationValues( "scenario.destinationType", new Object[]{
    // Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
    // Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)} );
    // }
    //    
    // public void testTempDestinationsRemovedOnConnectionClose() throws
    // Exception {
    //        
    // // Setup a first connection
    // StubConnection connection1 = scenario.createConnection();
    // ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
    // SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
    // ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
    // connection1.send(connectionInfo1);
    // connection1.send(sessionInfo1);
    // connection1.send(producerInfo1);
    //
    // scenario.destination = scenario.createDestinationInfo(connection1, connectionInfo1,
    // scenario.destinationType);
    //        
    // StubConnection connection2 = scenario.createConnection();
    // ConnectionInfo connectionInfo2 = scenario.createConnectionInfo();
    // SessionInfo sessionInfo2 = scenario.createSessionInfo(connectionInfo2);
    // ProducerInfo producerInfo2 = scenario.createProducerInfo(sessionInfo2);
    // connection2.send(connectionInfo2);
    // connection2.send(sessionInfo2);
    // connection2.send(producerInfo2);
    //
    // // Send from connection2 to connection1's temp scenario.destination. Should
    // succeed.
    // connection2.send(scenario.createMessage(producerInfo2, scenario.destination,
    // scenario.deliveryMode));
    //        
    // // Close connection 1
    // connection1.request(scenario.closeConnectionInfo(connectionInfo1));
    //        
    // try {
    // // Send from connection2 to connection1's temp scenario.destination. Should not
    // succeed.
    // connection2.request(scenario.createMessage(producerInfo2, scenario.destination,
    // scenario.deliveryMode));
    // fail("Expected JMSException.");
    // } catch ( JMSException success ) {
    // }
    //        
    // }

    // public void initCombosForTestTempDestinationsAreNotAutoCreated() {
    // addCombinationValues( "scenario.deliveryMode", new Object[]{
    // Integer.valueOf(DeliveryMode.NON_PERSISTENT),
    // Integer.valueOf(DeliveryMode.PERSISTENT)} );
    // addCombinationValues( "scenario.destinationType", new Object[]{
    // Byte.valueOf(ActiveMQDestination.TEMP_QUEUE_TYPE),
    // Byte.valueOf(ActiveMQDestination.TEMP_TOPIC_TYPE)} );
    // }
    //    
    //   

    // We create temp scenario.destination on demand now so this test case is no longer
    // valid.
    //    
    // public void testTempDestinationsAreNotAutoCreated() throws Exception {
    //        
    // // Setup a first connection
    // StubConnection connection1 = scenario.createConnection();
    // ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
    // SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
    // ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
    // connection1.send(connectionInfo1);
    // connection1.send(sessionInfo1);
    // connection1.send(producerInfo1);
    //
    // scenario.destination =
    // ActiveMQDestination.createDestination(connectionInfo1.getConnectionId()+":1",
    // scenario.destinationType);
    //            
    // // Should not be able to send to a non-existant temp scenario.destination.
    // try {
    // connection1.request(scenario.createMessage(producerInfo1, scenario.destination,
    // scenario.deliveryMode));
    // fail("Expected JMSException.");
    // } catch ( JMSException success ) {
    // }
    //        
    // }

    
//    public void initCombosForTestExclusiveQueueDeliversToOnlyOneConsumer() {
//        addCombinationValues("scenario.deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
//                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
//    }

//	@Test(dataProvider = "deliveryMode-combinations")
    @Theory
    public void testExclusiveQueueDeliversToOnlyOneConsumer(BrokerTestScenario scenario) throws Exception {
        assumeQueueDestination(scenario);
        start(scenario);

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(1);
        consumerInfo1.setExclusive(true);
        connection1.send(consumerInfo1);

        // Send a message.. this should make consumer 1 the exclusive owner.
        connection1.request(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        // Setup a second connection
        StubConnection connection2 = scenario.createConnection();
        ConnectionInfo connectionInfo2 = scenario.createConnectionInfo();
        SessionInfo sessionInfo2 = scenario.createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = scenario.createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setPrefetchSize(1);
        consumerInfo2.setExclusive(true);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(consumerInfo2);

        // Second message should go to consumer 1 even though consumer 2 is
        // ready for dispatch.
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        // Acknowledge the first 2 messages
        for (int i = 0; i < 2; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(m1);
            connection1.send(scenario.createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        // Close the first consumer.
        connection1.send(scenario.closeConsumerInfo(consumerInfo1));

        // The last two messages should now go the the second consumer.
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        for (int i = 0; i < 2; i++) {
            Message m1 = scenario.receiveMessage(connection2);
            assertNotNull(m1);
            connection2.send(scenario.createAck(consumerInfo2, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        scenario.assertNoMessagesLeft(connection2);
    }

//	@Test(dataProvider = "deliveryMode-perm-destinations-combinations")
    @Theory
    public void testWildcardConsume(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryPermDest(scenario);
        start(scenario);

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        // setup the wildcard consumer.
        ActiveMQDestination compositeDestination = ActiveMQDestination.createDestination("WILD.*.TEST",
                                                                                         scenario.destinationType);
        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, compositeDestination);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // These two message should NOT match the wild card.
        connection1.send(scenario.createMessage(producerInfo1, ActiveMQDestination.createDestination("WILD.CARD",
                                                                                            scenario.destinationType),
                                       scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo1, ActiveMQDestination.createDestination("WILD.TEST",
                                                                                            scenario.destinationType),
                                       scenario.deliveryMode));

        // These two message should match the wild card.
        ActiveMQDestination d1 = ActiveMQDestination.createDestination("WILD.CARD.TEST", scenario.destinationType);
        connection1.send(scenario.createMessage(producerInfo1, d1, scenario.deliveryMode));
        
        Message m = scenario.receiveMessage(connection1);
        assertNotNull(m);
        assertEquals(m.getDestination(), d1);

        ActiveMQDestination d2 = ActiveMQDestination.createDestination("WILD.FOO.TEST", scenario.destinationType);
        connection1.request(scenario.createMessage(producerInfo1, d2, scenario.deliveryMode));
        m = scenario.receiveMessage(connection1);
        assertNotNull(m);
        assertEquals(m.getDestination(), d2);

        scenario.assertNoMessagesLeft(connection1);
        connection1.send(scenario.closeConnectionInfo(connectionInfo1));
    }

//	@Test(dataProvider = "deliveryMode-perm-destinations-combinations")
    @Theory
    public void testCompositeConsume(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryPermDest(scenario);
        start(scenario);

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        // setup the composite consumer.
        ActiveMQDestination compositeDestination = ActiveMQDestination.createDestination("A,B",
                                                                                         scenario.destinationType);
        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, compositeDestination);
        consumerInfo1.setRetroactive(true);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // Publish to the two destinations
        ActiveMQDestination destinationA = ActiveMQDestination.createDestination("A", scenario.destinationType);
        ActiveMQDestination destinationB = ActiveMQDestination.createDestination("B", scenario.destinationType);

        // Send a message to each scenario.destination .
        connection1.send(scenario.createMessage(producerInfo1, destinationA, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo1, destinationB, scenario.deliveryMode));

        // The consumer should get both messages.
        for (int i = 0; i < 2; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(m1);
        }

        scenario.assertNoMessagesLeft(connection1);
        connection1.send(scenario.closeConnectionInfo(connectionInfo1));
    }

//	@Test(dataProvider = "deliveryMode-perm-destinations-combinations")
    @Theory
    public void testConnectionCloseCascades(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryPermDest(scenario);
        start(scenario);

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);
        ActiveMQDestination destination = ActiveMQDestination.createDestination("TEST",  scenario.destinationType);
        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        consumerInfo1.setNoLocal(true);
        connection1.request(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = scenario.createConnection();
        ConnectionInfo connectionInfo2 = scenario.createConnectionInfo();
        SessionInfo sessionInfo2 = scenario.createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = scenario.createProducerInfo(sessionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(producerInfo2);

        // Send the messages
        connection2.send(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));
        connection2.send(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));
        connection2.send(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));
        connection2.send(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));

        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(m1);
            connection1.send(scenario.createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        // give the async ack a chance to perculate and validate all are currently consumed
        assertNull(connection1.getDispatchQueue().poll(scenario.MAX_NULL_WAIT, TimeUnit.MILLISECONDS));

        // Close the connection, this should in turn close the consumer.
        connection1.request(scenario.closeConnectionInfo(connectionInfo1));

        // Send another message, connection1 should not get the message.
        connection2.request(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));

        assertNull(connection1.getDispatchQueue().poll(scenario.MAX_NULL_WAIT, TimeUnit.MILLISECONDS));
    }

//	@Test(dataProvider = "deliveryMode-perm-destinations-combinations")
    @Theory
    public void testSessionCloseCascades(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryPermDest(scenario);
        start(scenario);

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);
        ActiveMQDestination destination = ActiveMQDestination.createDestination("TEST",  scenario.destinationType);
        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        consumerInfo1.setNoLocal(true);
        connection1.request(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = scenario.createConnection();
        ConnectionInfo connectionInfo2 = scenario.createConnectionInfo();
        SessionInfo sessionInfo2 = scenario.createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = scenario.createProducerInfo(sessionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(producerInfo2);

        // Send the messages
        connection2.send(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));
        connection2.send(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));
        connection2.send(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));
        connection2.send(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));

        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(m1);
            connection1.send(scenario.createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        // Close the session, this should in turn close the consumer.
        connection1.request(scenario.closeSessionInfo(sessionInfo1));

        // Send another message, connection1 should not get the message.
        connection2.request(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));

        assertNull(connection1.getDispatchQueue().poll(scenario.MAX_NULL_WAIT, TimeUnit.MILLISECONDS));
    }

//    public void initCombosForTestConsumerClose() {
//        addCombinationValues("scenario.deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT),
//                                                           Integer.valueOf(DeliveryMode.PERSISTENT)});
//        addCombinationValues("scenario.destination", new Object[] {new ActiveMQTopic("TEST"),
//                                                          new ActiveMQQueue("TEST")});
//    }

//	@Test(dataProvider = "deliveryMode-perm-destinations-combinations")
    @Theory
    public void testConsumerClose(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryPermDest(scenario);
        start(scenario);

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);
        ActiveMQDestination destination = ActiveMQDestination.createDestination("TEST",  scenario.destinationType);
        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        consumerInfo1.setNoLocal(true);
        connection1.request(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = scenario.createConnection();
        ConnectionInfo connectionInfo2 = scenario.createConnectionInfo();
        SessionInfo sessionInfo2 = scenario.createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = scenario.createProducerInfo(sessionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.send(producerInfo2);

        // Send the messages
        connection2.send(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));
        connection2.send(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));
        connection2.send(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));
        connection2.send(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));

        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(m1);
            connection1.send(scenario.createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));
        }

        // give the async ack a chance to perculate and validate all are currently consumed
        assertNull(connection1.getDispatchQueue().poll(scenario.MAX_NULL_WAIT, TimeUnit.MILLISECONDS));
 
        // Close the consumer.
        connection1.request(scenario.closeConsumerInfo(consumerInfo1));

        // Send another message, connection1 should not get the message.
        connection2.request(scenario.createMessage(producerInfo2, destination, scenario.deliveryMode));

        assertNull(connection1.getDispatchQueue().poll(scenario.MAX_NULL_WAIT, TimeUnit.MILLISECONDS));
    }

//	@Test(dataProvider = "deliveryMode-combinations")
    @Theory
    public void testTopicDispatchIsBroadcast(BrokerTestScenario scenario) throws Exception {
        assumeQueueDestination(scenario);
        start(scenario);

        ActiveMQDestination destination = new ActiveMQTopic("TEST");

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setRetroactive(true);
        consumerInfo1.setPrefetchSize(100);
        connection1.request(consumerInfo1);

        // Setup a second connection
        StubConnection connection2 = scenario.createConnection();
        ConnectionInfo connectionInfo2 = scenario.createConnectionInfo();
        SessionInfo sessionInfo2 = scenario.createSessionInfo(connectionInfo2);
        ConsumerInfo consumerInfo2 = scenario.createConsumerInfo(sessionInfo2, destination);
        consumerInfo2.setRetroactive(true);
        consumerInfo2.setPrefetchSize(100);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(consumerInfo2);

        // Send the messages
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo1, destination, scenario.deliveryMode));

        // Get the messages
        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(m1);
            m1 = scenario.receiveMessage(connection2);
            assertNotNull(m1);
        }
    }

//	@Test(dataProvider = "deliveryMode-queue-combinations")
    @Theory
    public void testQueueDispatchedAreRedeliveredOnConsumerClose(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryModeQueue(scenario);
        start(scenario);

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        ActiveMQDestination destination = scenario.createDestinationInfo(connection1, connectionInfo1, scenario.destinationType);

        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // Send the messages
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection1.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        // Get the messages
        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(m1);
            assertFalse(m1.isRedelivered());
        }
        // Close the consumer without sending any ACKS.
        connection1.send(scenario.closeConsumerInfo(consumerInfo1));

        // Drain any in flight messages..
        while (connection1.getDispatchQueue().poll(0, TimeUnit.MILLISECONDS) != null) {
        }

        // Add the second consumer
        ConsumerInfo consumerInfo2 = scenario.createConsumerInfo(sessionInfo1, destination);
        consumerInfo2.setPrefetchSize(100);
        connection1.send(consumerInfo2);

        // Make sure the messages were re delivered to the 2nd consumer.
        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(m1);
            assertTrue(m1.isRedelivered());
        }
    }

//	@Test(dataProvider = "deliveryMode-queue-combinations")
    @Theory
    public void testQueueBrowseMessages(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryModeQueue(scenario);
        start(scenario);

        // Start a producer and consumer
        StubConnection connection = scenario.createConnection();
        ConnectionInfo connectionInfo = scenario.createConnectionInfo();
        SessionInfo sessionInfo = scenario.createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        ActiveMQDestination destination = scenario.createDestinationInfo(connection, connectionInfo, scenario.destinationType);

        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        // Use selector to skip first message.
        ConsumerInfo consumerInfo = scenario.createConsumerInfo(sessionInfo, destination);
        consumerInfo.setBrowser(true);
        connection.send(consumerInfo);

        for (int i = 0; i < 4; i++) {
            Message m = scenario.receiveMessage(connection);
            assertNotNull(m);
            connection.send(scenario.createAck(consumerInfo, m, 1, MessageAck.DELIVERED_ACK_TYPE));
        }

        scenario.assertNoMessagesLeft(connection);
    }

//	@Test(dataProvider = "deliveryMode-queue-combinations")
    @Theory
    public void testQueueAckRemovesMessage(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryModeQueue(scenario);
        start(scenario);

        // Start a producer and consumer
        StubConnection connection = scenario.createConnection();
        ConnectionInfo connectionInfo = scenario.createConnectionInfo();
        SessionInfo sessionInfo = scenario.createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        ActiveMQDestination destination = scenario.createDestinationInfo(connection, connectionInfo, scenario.destinationType);

        Message message1 = scenario.createMessage(producerInfo, destination, scenario.deliveryMode);
        Message message2 = scenario.createMessage(producerInfo, destination, scenario.deliveryMode);
        connection.send(message1);
        connection.send(message2);

        // Make sure the message was delivered.
        ConsumerInfo consumerInfo = scenario.createConsumerInfo(sessionInfo, destination);
        connection.request(consumerInfo);
        Message m = scenario.receiveMessage(connection);
        assertNotNull(m);
        assertEquals(message1.getMessageId(), m.getMessageId());

        assertEquals(2, scenario.countMessagesInQueue(connection, connectionInfo, destination));
        connection.send(scenario.createAck(consumerInfo, m, 1, MessageAck.DELIVERED_ACK_TYPE));
        assertEquals(2, scenario.countMessagesInQueue(connection, connectionInfo, destination));
        connection.send(scenario.createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE));
        assertEquals(1, scenario.countMessagesInQueue(connection, connectionInfo, destination));

    }

//	@Test(dataProvider = "deliveryMode-all-destinations-combinations")
    @Theory
    public void testSelectorSkipsMessages(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryAllDest(scenario);
        start(scenario);

        // Start a producer and consumer
        StubConnection connection = scenario.createConnection();
        ConnectionInfo connectionInfo = scenario.createConnectionInfo();
        SessionInfo sessionInfo = scenario.createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        ActiveMQDestination destination = scenario.createDestinationInfo(connection, connectionInfo, scenario.destinationType);

        ConsumerInfo consumerInfo = scenario.createConsumerInfo(sessionInfo, destination);
        consumerInfo.setSelector("JMSType='last'");
        connection.send(consumerInfo);

        Message message1 = scenario.createMessage(producerInfo, destination, scenario.deliveryMode);
        message1.setType("first");
        Message message2 = scenario.createMessage(producerInfo, destination, scenario.deliveryMode);
        message2.setType("last");
        connection.send(message1);
        connection.send(message2);

        // Use selector to skip first message.
        Message m = scenario.receiveMessage(connection);
        assertNotNull(m);
        assertEquals(message2.getMessageId(), m.getMessageId());
        connection.send(scenario.createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE));
        connection.send(scenario.closeConsumerInfo(consumerInfo));

        scenario.assertNoMessagesLeft(connection);
    }

//	@Test(dataProvider = "deliveryMode-all-destinations-combinations")
    @Theory
    public void testAddConsumerThenSend(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryAllDest(scenario);
        start(scenario);

        // Start a producer and consumer
        StubConnection connection = scenario.createConnection();
        ConnectionInfo connectionInfo = scenario.createConnectionInfo();
        SessionInfo sessionInfo = scenario.createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        ActiveMQDestination destination = scenario.createDestinationInfo(connection, connectionInfo, scenario.destinationType);

        ConsumerInfo consumerInfo = scenario.createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        // Make sure the message was delivered.
        Message m = scenario.receiveMessage(connection);
        assertNotNull(m);
    }

//	@Test(dataProvider = "deliveryMode-all-destinations-combinations")
    @Theory
    public void testConsumerPrefetchAtOne(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryAllDest(scenario);
        start(scenario);

        // Start a producer and consumer
        StubConnection connection = scenario.createConnection();
        ConnectionInfo connectionInfo = scenario.createConnectionInfo();
        SessionInfo sessionInfo = scenario.createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        ActiveMQDestination destination = scenario.createDestinationInfo(connection, connectionInfo, scenario.destinationType);

        ConsumerInfo consumerInfo = scenario.createConsumerInfo(sessionInfo, destination);
        consumerInfo.setPrefetchSize(1);
        connection.send(consumerInfo);

        // Send 2 messages to the broker.
        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        // Make sure only 1 message was delivered.
        Message m = scenario.receiveMessage(connection);
        assertNotNull(m);
        scenario.assertNoMessagesLeft(connection);

    }

//	@Test(dataProvider = "deliveryMode-all-destinations-combinations")
    @Theory
    public void testConsumerPrefetchAtTwo(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryAllDest(scenario);
        start(scenario);

        // Start a producer and consumer
        StubConnection connection = scenario.createConnection();
        ConnectionInfo connectionInfo = scenario.createConnectionInfo();
        SessionInfo sessionInfo = scenario.createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        ActiveMQDestination destination = scenario.createDestinationInfo(connection, connectionInfo, scenario.destinationType);

        ConsumerInfo consumerInfo = scenario.createConsumerInfo(sessionInfo, destination);
        consumerInfo.setPrefetchSize(2);
        connection.send(consumerInfo);

        // Send 3 messages to the broker.
        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        // Make sure only 1 message was delivered.
        Message m = scenario.receiveMessage(connection);
        assertNotNull(m);
        m = scenario.receiveMessage(connection);
        assertNotNull(m);
        scenario.assertNoMessagesLeft(connection);

    }

//	@Test(dataProvider = "deliveryMode-all-destinations-combinations")
    @Theory
    public void testConsumerPrefetchAndDeliveredAck(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryAllDest(scenario);
        start(scenario);

        // Start a producer and consumer
        StubConnection connection = scenario.createConnection();
        ConnectionInfo connectionInfo = scenario.createConnectionInfo();
        SessionInfo sessionInfo = scenario.createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = scenario.createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        ActiveMQDestination destination = scenario.createDestinationInfo(connection, connectionInfo, scenario.destinationType);

        ConsumerInfo consumerInfo = scenario.createConsumerInfo(sessionInfo, destination);
        consumerInfo.setPrefetchSize(1);
        connection.request(consumerInfo);

        // Send 3 messages to the broker.
        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection.send(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));
        connection.request(scenario.createMessage(producerInfo, destination, scenario.deliveryMode));

        // Make sure only 1 message was delivered.
        Message m1 = scenario.receiveMessage(connection);
        assertNotNull(m1);

        scenario.assertNoMessagesLeft(connection);

        // Acknowledge the first message. This should cause the next message to
        // get dispatched.
        connection.request(scenario.createAck(consumerInfo, m1, 1, MessageAck.DELIVERED_ACK_TYPE));

        Message m2 = scenario.receiveMessage(connection);
        assertNotNull(m2);
        connection.request(scenario.createAck(consumerInfo, m2, 1, MessageAck.DELIVERED_ACK_TYPE));

        Message m3 = scenario.receiveMessage(connection);
        assertNotNull(m3);
        connection.request(scenario.createAck(consumerInfo, m3, 1, MessageAck.DELIVERED_ACK_TYPE));
    }
	
//	@Test(dataProvider = "deliveryMode-all-destinations-combinations")
    @Theory
    @Ignore // Failing.. 
	public void testTransactedSend(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryAllDest(scenario);
        start(scenario);

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        scenario.destination = scenario.createDestinationInfo(connection1, connectionInfo1, scenario.destinationType);

        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, scenario.destination);
        consumerInfo1.setPrefetchSize(100);
        connection1.send(consumerInfo1);

        // Begin the transaction.
        LocalTransactionId txid = scenario.createLocalTransaction(sessionInfo1);
        connection1.send(scenario.createBeginTransaction(connectionInfo1, txid));

        // Send the messages
        for (int i = 0; i < 4; i++) {
            Message message = scenario.createMessage(producerInfo1, scenario.destination, scenario.deliveryMode);
            message.setTransactionId(txid);
            connection1.request(message);
        }

        // The point of this test is that message should not be delivered until
        // send is committed.
        assertNull(scenario.receiveMessage(connection1,scenario.MAX_NULL_WAIT));

        // Commit the transaction.
        connection1.send(scenario.createCommitTransaction1Phase(connectionInfo1, txid));

        // Now get the messages.
        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(m1);
        }

        scenario.assertNoMessagesLeft(connection1);
    }

//	@Test(dataProvider = "deliveryMode-all-destinations-combinations")
    @Theory
	public void testTransactedAckWithPrefetchOfOne(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryAllDest(scenario);
        start(scenario);

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        scenario.destination = scenario.createDestinationInfo(connection1, connectionInfo1, scenario.destinationType);

        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, scenario.destination);
        consumerInfo1.setPrefetchSize(1);
        connection1.send(consumerInfo1);

        // Send the messages
        for (int i = 0; i < 4; i++) {
            Message message = scenario.createMessage(producerInfo1, scenario.destination, scenario.deliveryMode);
            connection1.send(message);
        }

       

        // Now get the messages.
        for (int i = 0; i < 4; i++) {
            // Begin the transaction.
            LocalTransactionId txid = scenario.createLocalTransaction(sessionInfo1);
            connection1.send(scenario.createBeginTransaction(connectionInfo1, txid));
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(m1);
            MessageAck ack = scenario.createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection1.send(ack);
         // Commit the transaction.
            connection1.send(scenario.createCommitTransaction1Phase(connectionInfo1, txid));
        }
        scenario.assertNoMessagesLeft(connection1);
    }
	
    @Theory
    @Ignore
    public void testTransactedAckRollbackWithPrefetchOfOne(BrokerTestScenario scenario) throws Exception {
        assumeDeliveryAllDest(scenario);
        start(scenario);

        // Setup a first connection
        StubConnection connection1 = scenario.createConnection();
        ConnectionInfo connectionInfo1 = scenario.createConnectionInfo();
        SessionInfo sessionInfo1 = scenario.createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo1 = scenario.createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo1);

        scenario.destination = scenario.createDestinationInfo(connection1, connectionInfo1, scenario.destinationType);

        ConsumerInfo consumerInfo1 = scenario.createConsumerInfo(sessionInfo1, scenario.destination);
        consumerInfo1.setPrefetchSize(1);
        connection1.send(consumerInfo1);

        // Send the messages
        for (int i = 0; i < 4; i++) {
            Message message = scenario.createMessage(producerInfo1, scenario.destination, scenario.deliveryMode);
            connection1.send(message);
        }

        // Now get the messages.
        LocalTransactionId txid = scenario.createLocalTransaction(sessionInfo1);
        // Begin the transaction.
        connection1.send(scenario.createBeginTransaction(connectionInfo1, txid));
        for (int i = 0; i < 4; i++) {
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(m1);
            MessageAck ack = scenario.createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection1.send(ack);
        }
        
        // Rollback the transaction:
        connection1.send(scenario.createRollbackTransaction(connectionInfo1, txid));
        
        for (int i = 0; i < 4; i++) {
            // Begin the transaction.
            txid = scenario.createLocalTransaction(sessionInfo1);
            connection1.send(scenario.createBeginTransaction(connectionInfo1, txid));
            Message m1 = scenario.receiveMessage(connection1);
            assertNotNull(m1);
            MessageAck ack = scenario.createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE);
            ack.setTransactionId(txid);
            connection1.send(ack);
        }
        // Commit the transaction.
        connection1.send(scenario.createCommitTransaction1Phase(connectionInfo1, txid));
        
        
        scenario.assertNoMessagesLeft(connection1);
    }
	

}
