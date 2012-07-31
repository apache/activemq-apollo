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
package org.apache.activemq.apollo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test cases used to test the JMS message consumer.
 * 
 * 
 */
public class JmsTestBase extends CombinationTestSupport {
    protected static final Logger LOG = LoggerFactory.getLogger(BrokerProtocol.class);
    public BrokerProtocol protocol;

    public void initCombos() {
        ArrayList<Object> protocols = new ArrayList<Object>();
        protocols.add(new StompBrokerProtocol());
        try {
            Class.forName("org.apache.activemq.apollo.openwire.OpenwireProtocolHandler", false, JmsTestBase.class.getClassLoader());
            protocols.add(new OpenwireBrokerProtocol());
        } catch (ClassNotFoundException e) {
        }
        addCombinationValues("protocol", protocols.toArray());
    }

    public String brokerConfig = "xml:classpath:apollo.xml";

    public Object broker;
    protected ConnectionFactory factory;
    protected Connection connection;
    protected List<Connection> connections = Collections.synchronizedList(new ArrayList<Connection>());

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        startBroker();
        factory = protocol.getConnectionFactory(broker);

        connection = factory.createConnection(userName, password);
        connections.add(connection);
    }

    public void startBroker() {
        if (System.getProperty("basedir") == null) {
            File file = new File(".");
            System.setProperty("basedir", file.getAbsolutePath());
        }
        broker = protocol.create(brokerConfig);
        protocol.start(broker);
    }

    @Override
    protected void tearDown() throws Exception {
        for (Iterator iter = connections.iterator(); iter.hasNext();) {
            Connection conn = (Connection)iter.next();
            try {
                conn.close();
            } catch (Throwable e) {
            }
            iter.remove();
        }

        connection = null;
        stopBroker();
        super.tearDown();
    }

    public void stopBroker() {
        if(broker!=null) {
            protocol.stop(broker);
            broker = null;
        }
    }

    public ConnectionFactory getConnectionFactory() throws Exception {
        return factory;
    }

    /**
     * Factory method to create a new connection
     */
    public Connection createConnection() throws Exception {
        return getConnectionFactory().createConnection();
    }

    /**
     * @param messsage
     * @param firstSet
     * @param secondSet
     */
    protected void assertTextMessagesEqual(String messsage, Message[] firstSet, Message[] secondSet)
        throws JMSException {
        assertEquals("Message count does not match: " + messsage, firstSet.length, secondSet.length);
        for (int i = 0; i < secondSet.length; i++) {
            TextMessage m1 = (TextMessage)firstSet[i];
            TextMessage m2 = (TextMessage)secondSet[i];
            if (m1 != null) {
                m1.getText();
            }
            if (m2 != null) {
                m2.getText();
            }
            assertFalse("Message " + (i + 1) + " did not match : " + messsage + ": expected {" + m1
                        + "}, but was {" + m2 + "}", m1 == null ^ m2 == null);
            assertEquals("Message " + (i + 1) + " did not match: " + messsage + ": expected {" + m1
                         + "}, but was {" + m2 + "}", m1.getText(), m2.getText());
        }
    }


    protected String getConsumerDestinationName() {
        return getDestinationName();
    }
    protected String getProducerDestinationName() {
        return getDestinationName();
    }
    protected String getDestinationName() {
        return getName().replaceAll("[{}= @\\.]+", "_");
    }

    protected boolean topic = true;

    protected Destination createDestination(String subject) {
        return null;
    }
    protected Destination createDestination() {
        return createDestination(getDestinationString());
    }

    /**
     * Returns the name of the destination used in this test case
     */
    protected String getDestinationString() {
        return getClass().getName() + "." + getName(true);
    }


    public static void recursiveDelete(File f) {
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            for (int i = 0; i < files.length; i++) {
                recursiveDelete(files[i]);
            }
        }
        f.delete();
    }


    static final private AtomicLong TEST_COUNTER = new AtomicLong();
    public String userName;
    public String password;
    public String messageTextPrefix = "";


    // /////////////////////////////////////////////////////////////////
    //
    // Test support methods.
    //
    // /////////////////////////////////////////////////////////////////
    protected Destination createDestination(Session session, DestinationType type) throws JMSException {
        return createDestination(session, type, false);
    }

    protected Destination createDestination(Session session, DestinationType type, boolean exclusive) throws JMSException {
        String testMethod = getName();
        if( testMethod.indexOf(" ")>0 ) {
            testMethod = testMethod.substring(0, testMethod.indexOf(" "));
        }
        String name = "TEST." + getClass().getName() + "." + testMethod + "." + TEST_COUNTER.getAndIncrement();
        switch (type) {
        case QUEUE_TYPE:
            return makeExclusive(session.createQueue(name), exclusive);
        case TOPIC_TYPE:
            return makeExclusive(session.createTopic(name), exclusive);
        case TEMP_QUEUE_TYPE:
            return makeExclusive(session.createTemporaryQueue(), exclusive);
        case TEMP_TOPIC_TYPE:
            return makeExclusive(session.createTemporaryTopic(), exclusive);
        default:
            throw new IllegalArgumentException("type: " + type);
        }
    }

    private Destination makeExclusive(Destination dest, boolean exclusive) {
        if( exclusive ) {
            dest = protocol.addExclusiveOptions(dest);
        }
        return dest;
    }

    protected void sendMessages(Destination destination, int count) throws Exception {
        Connection connection = getConnectionFactory().createConnection();
        connection.start();
        sendMessages(connection, destination, count);
        connection.close();
    }

    protected void sendMessages(Connection connection, Destination destination, int count) throws JMSException {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        sendMessages(session, destination, count);
        session.close();
    }

    protected void sendMessages(Session session, Destination destination, int count) throws JMSException {
        MessageProducer producer = session.createProducer(destination);
        sendMessages(session, producer, count);
        producer.close();
    }

    protected void sendMessages(Session session, MessageProducer producer, int count) throws JMSException {
        for (int i = 0; i < count; i++) {
            producer.send(session.createTextMessage(messageTextPrefix  + i));
        }
    }


    protected void safeClose(Connection c) {
        try {
            c.close();
        } catch (Throwable e) {
        }
    }

    protected void safeClose(Session s) {
        try {
            s.close();
        } catch (Throwable e) {
        }
    }

    protected void safeClose(MessageConsumer c) {
        try {
            c.close();
        } catch (Throwable e) {
        }
    }

    protected void safeClose(MessageProducer p) {
        try {
            p.close();
        } catch (Throwable e) {
        }
    }

    protected void profilerPause(String prompt) throws IOException {
        if (System.getProperty("profiler") != null) {
            pause(prompt);
        }
    }

    protected void pause(String prompt) throws IOException {
        System.out.println();
        System.out.println(prompt + "> Press enter to continue: ");
        while (System.in.read() != '\n') {
        }
    }

}
