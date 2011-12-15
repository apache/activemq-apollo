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

import java.io.File;
import javax.jms.*;

/**
 * Useful base class for unit test cases
 * 
 * 
 */
public abstract class TestSupport extends CombinationTestSupport {

    protected BrokerService broker;
    protected ConnectionFactory connectionFactory;
    protected boolean topic = true;

    public void initCombos() {
        Object[] brokers;
        // TODO - until openwire is built normally do a quick/dirty check
        boolean openwireEnabled = false;
        try {
            Class.forName("org.apache.activemq.apollo.openwire.OpenwireProtocolHandler", false, TestSupport.class.getClassLoader());
            openwireEnabled = true;
        } catch (ClassNotFoundException e) {

        }

        if (openwireEnabled) {
            brokers = new Object[] { new StompBroker(), new OpenwireBroker() };
        } else {
            brokers = new Object[] { new StompBroker() };
        }
        addCombinationValues("broker", brokers);
    }

    /*
    public PersistenceAdapterChoice defaultPersistenceAdapter = PersistenceAdapterChoice.KahaDB;
    */

    /*
    protected Message createMessage() {
        return new ActiveMQMessage();
    }
    */

    /*
    protected Destination createDestination(String subject) {
        if (topic) {
            return new ActiveMQTopic(subject);
        } else {
            return new ActiveMQQueue(subject);
        }
    }
    */

    public void setBroker(BrokerService broker) {
        this.broker = broker;
    }

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

    public ConnectionFactory createConnectionFactory() throws Exception {
        return broker.getConnectionFactory();
    }

    /**
     * Factory method to create a new connection
     */
    public Connection createConnection() throws Exception {
        return getConnectionFactory().createConnection();
    }

    public ConnectionFactory getConnectionFactory() throws Exception {
        if (connectionFactory == null) {
            connectionFactory = createConnectionFactory();
            assertTrue("Should have created a connection factory!", connectionFactory != null);
        }
        return connectionFactory;
    }

    protected String getConsumerSubject() {
        return getSubject();
    }

    protected String getProducerSubject() {
        return getSubject();
    }

    protected String getSubject() {
        return getName().replaceAll("[{}= @\\.]+", "_");
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

    public static void removeMessageStore() {
        if (System.getProperty("activemq.store.dir") != null) {
            recursiveDelete(new File(System.getProperty("activemq.store.dir")));
        }
        if (System.getProperty("derby.system.home") != null) {
            recursiveDelete(new File(System.getProperty("derby.system.home")));
        }
    }

    /*
    public static DestinationStatistics getDestinationStatistics(BrokerService broker, ActiveMQDestination destination) {
        DestinationStatistics result = null;
        org.apache.activemq.broker.region.Destination dest = getDestination(broker, destination);
        if (dest != null) {
            result = dest.getDestinationStatistics();
        }
        return result;
    }
    */

    /*
    public static org.apache.activemq.broker.region.Destination getDestination(BrokerService target, ActiveMQDestination destination) {
        org.apache.activemq.broker.region.Destination result = null;
        for (org.apache.activemq.broker.region.Destination dest : getDestinationMap(target, destination).values()) {
            if (dest.getName().equals(destination.getPhysicalName())) {
                result = dest;
                break;
            }
        }
        return result;
    }
    */

    /*
    private static Map<ActiveMQDestination, org.apache.activemq.broker.region.Destination> getDestinationMap(BrokerService target,
            ActiveMQDestination destination) {
        RegionBroker regionBroker = (RegionBroker) target.getRegionBroker();
        return destination.isQueue() ?
                    regionBroker.getQueueRegion().getDestinationMap() :
                        regionBroker.getTopicRegion().getDestinationMap();
    }
    */


    //public static enum PersistenceAdapterChoice {KahaDB, AMQ, JDBC, MEM };

    /*
    public PersistenceAdapter setDefaultPersistenceAdapter(BrokerService broker) throws IOException {
        return setPersistenceAdapter(broker, defaultPersistenceAdapter);
    }
    */

    /*
    public PersistenceAdapter setPersistenceAdapter(BrokerService broker, PersistenceAdapterChoice choice) throws IOException {
        PersistenceAdapter adapter = null;
        switch (choice) {
        case AMQ:
            adapter = new AMQPersistenceAdapter();
            break;
        case JDBC:
            adapter = new JDBCPersistenceAdapter();
            break;
        case KahaDB:
            adapter = new KahaDBPersistenceAdapter();
            break;
        case MEM:
            adapter = new MemoryPersistenceAdapter();
            break;
        }
        broker.setPersistenceAdapter(adapter);
        return adapter;
    }
    */

    /**
     * Test if base directory contains spaces
     */
    protected void assertBaseDirectoryContainsSpaces() {
    	assertFalse("Base directory cannot contain spaces.", new File(System.getProperty("basedir", ".")).getAbsoluteFile().toString().contains(" "));
    }

}
