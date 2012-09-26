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
package example.topic.durable;

import example.util.Util;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.concurrent.CountDownLatch;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class Subscriber implements MessageListener {
    private static final Logger LOG = LoggerFactory.getLogger(Subscriber.class);

    private static final String BROKER_HOST = "tcp://localhost:%d";
    private static final int BROKER_PORT = Util.getBrokerPort();
    private static final String BROKER_URL = String.format(BROKER_HOST, BROKER_PORT);
    private static final Boolean NON_TRANSACTED = false;


    private final CountDownLatch countDownLatch;
    public Subscriber(CountDownLatch latch) {
        countDownLatch = latch;
    }

    public static void main(String[] args) {
        LOG.info("\nWaiting to receive messages... Either waiting for END message or press Ctrl+C to exit");
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("admin", "password", BROKER_URL);
        Connection connection = null;
        final CountDownLatch latch = new CountDownLatch(1);

        try {

            connection = connectionFactory.createConnection();
            String clientId = System.getProperty("clientId");
            connection.setClientID(clientId);

            connection.start();

            Session session = connection.createSession(NON_TRANSACTED, Session.AUTO_ACKNOWLEDGE);
            Topic destination = session.createTopic("test-topic");

            MessageConsumer consumer = session.createDurableSubscriber(destination, clientId) ;
            consumer.setMessageListener(new Subscriber(latch));

            latch.await();
            consumer.close();
            session.close();

        } catch (Exception e) {
            LOG.error("Caught exception!", e);
        }
        finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (JMSException e) {
                    LOG.error("Could not close an open connection...", e);
                }
            }
        }
    }

    @Override
    public void onMessage(Message message) {
        try {
            if (message instanceof TextMessage) {
                String text = ((TextMessage) message).getText();
                if ("END".equalsIgnoreCase(text)) {
                    LOG.info("Received END message!");
                    countDownLatch.countDown();
                }
                else {
                    LOG.info("Received message:" +text);
                }
            }
        } catch (JMSException e) {
            LOG.error("Got a JMS Exception!", e);
        }
    }
}
