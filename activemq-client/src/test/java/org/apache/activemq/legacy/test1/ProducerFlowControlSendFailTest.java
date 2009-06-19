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
package org.apache.activemq.legacy.test1;

import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.apollo.broker.Broker;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;

public class ProducerFlowControlSendFailTest extends ProducerFlowControlTest {

    protected Broker createBroker() throws Exception {
        Broker broker = super.createBroker();
        broker.addTransportServer(TransportFactory.bind(new URI("tcp://localhost:0")));

        // Setup a destination policy where it takes only 1 message at a time.
// TODO:        
//        PolicyMap policyMap = new PolicyMap();
//        PolicyEntry policy = new PolicyEntry();
//        policy.setMemoryLimit(1);
//        policy.setPendingSubscriberPolicy(new VMPendingSubscriberMessageStoragePolicy());
//        policy.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
//        policyMap.setDefaultEntry(policy);
//        service.setDestinationPolicy(policyMap);
//        
//        service.getSystemUsage().setSendFailIfNoSpace(true);

        return broker;
    }
    
    @Override
    public void test2ndPubisherWithStandardConnectionThatIsBlocked() throws Exception {
        // with sendFailIfNoSpace set, there is no blocking of the connection
    }
    
    @Override
    public void testPubisherRecoverAfterBlock() throws Exception {
        ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory)createConnectionFactory();
        // with sendFail, there must be no flowControllwindow
        // sendFail is an alternative flow control mechanism that does not block
        factory.setUseAsyncSend(true);
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);
        connection.start();

        final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final MessageProducer producer = session.createProducer(queueA);
        
        final AtomicBoolean keepGoing = new AtomicBoolean(true);
   
        Thread thread = new Thread("Filler") {
            @Override
            public void run() {
                while (keepGoing.get()) {
                    try {
                        producer.send(session.createTextMessage("Test message"));
                        if (gotResourceException.get()) {
                            // do not flood the broker with requests when full as we are sending async and they 
                            // will be limited by the network buffers
                            Thread.sleep(200);
                        }
                    } catch (Exception e) {
                        // with async send, there will be no exceptions
                        e.printStackTrace();
                    }
                }
            }
        };
        thread.start();
        waitForBlockedOrResourceLimit(new AtomicBoolean(false));

        // resourceException on second message, resumption if we
        // can receive 10
        MessageConsumer consumer = session.createConsumer(queueA);
        TextMessage msg;
        for (int idx = 0; idx < 10; ++idx) {
            msg = (TextMessage) consumer.receive(1000);
            msg.acknowledge();
        }
        keepGoing.set(false);
    }

    
	@Override
	protected ConnectionFactory createConnectionFactory() throws Exception {
    	TransportServer server = broker.getTransportServers().get(1);
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(server.getConnectURI());
		connectionFactory.setExceptionListener(new ExceptionListener() {
				public void onException(JMSException arg0) {
					if (arg0 instanceof ResourceAllocationException) {
						gotResourceException.set(true);
					}
				}
	        });
		return connectionFactory;
	}
}
