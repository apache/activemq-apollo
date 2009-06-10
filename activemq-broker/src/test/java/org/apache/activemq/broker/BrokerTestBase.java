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
package org.apache.activemq.broker;

import java.beans.ExceptionListener;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.apache.activemq.apollo.broker.Destination;
import org.apache.activemq.apollo.broker.MessageBroker;
import org.apache.activemq.apollo.broker.Router;
import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.StoreFactory;
import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.dispatch.PriorityDispatcher;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.Period;
import org.apache.activemq.protobuf.AsciiBuffer;

public abstract class BrokerTestBase extends TestCase {

    protected static final int PERFORMANCE_SAMPLES = 5;

    protected static final int IO_WORK_AMOUNT = 0;
    protected static final int FANIN_COUNT = 10;
    protected static final int FANOUT_COUNT = 10;

    protected static final int PRIORITY_LEVELS = 10;
    protected static final boolean USE_INPUT_QUEUES = true;

    protected final boolean USE_KAHA_DB = true;
    protected final boolean PURGE_STORE = true;
    protected final boolean PERSISTENT = false;
    protected final boolean DURABLE = true;

    // Set to put senders and consumers on separate brokers.
    protected boolean multibroker = false;

    // Set to mockup up ptp:
    protected boolean ptp = false;

    // Set to use tcp IO
    protected boolean tcp = true;
    // set to force marshalling even in the NON tcp case.
    protected boolean forceMarshalling = false;

    protected String sendBrokerBindURI;
    protected String receiveBrokerBindURI;
    protected String sendBrokerConnectURI;
    protected String receiveBrokerConnectURI;

    // Set's the number of threads to use:
    protected final int asyncThreadPoolSize = Runtime.getRuntime().availableProcessors();
    protected boolean usePartitionedQueue = false;

    protected int producerCount;
    protected int consumerCount;
    protected int destCount;

    protected MetricAggregator totalProducerRate = new MetricAggregator().name("Aggregate Producer Rate").unit("items");
    protected MetricAggregator totalConsumerRate = new MetricAggregator().name("Aggregate Consumer Rate").unit("items");

    protected MessageBroker sendBroker;
    protected MessageBroker rcvBroker;
    protected ArrayList<MessageBroker> brokers = new ArrayList<MessageBroker>();
    protected IDispatcher dispatcher;
    protected final AtomicLong msgIdGenerator = new AtomicLong();
    protected final AtomicBoolean stopping = new AtomicBoolean();

    final ArrayList<RemoteProducer> producers = new ArrayList<RemoteProducer>();
    final ArrayList<RemoteConsumer> consumers = new ArrayList<RemoteConsumer>();

    @Override
    protected void setUp() throws Exception {
        dispatcher = createDispatcher();
        dispatcher.start();
        if (tcp) {
            sendBrokerBindURI = "tcp://localhost:10000?wireFormat=multi";
            receiveBrokerBindURI = "tcp://localhost:20000?wireFormat=multi";
            sendBrokerConnectURI = "tcp://localhost:10000";
            receiveBrokerConnectURI = "tcp://localhost:20000";
        } else {
            if (forceMarshalling) {
                sendBrokerBindURI = "pipe://SendBroker";
                receiveBrokerBindURI = "pipe://ReceiveBroker";
            } else {
                sendBrokerBindURI = "pipe://SendBroker";
                receiveBrokerBindURI = "pipe://ReceiveBroker";
            }
            sendBrokerConnectURI = sendBrokerBindURI;
            receiveBrokerConnectURI = receiveBrokerBindURI;
        }
    }

    protected IDispatcher createDispatcher() {
        return PriorityDispatcher.createPriorityDispatchPool("BrokerDispatcher", MessageBroker.MAX_PRIORITY, asyncThreadPoolSize);
    }

    public void test_1_1_0() throws Exception {
        if (ptp) {
            return;
        }
        producerCount = 1;
        destCount = 1;

        createConnections();

        // Start 'em up.
        startClients();
        try {
            reportRates();
        } finally {
            stopServices();
        }
    }

    public void test_1_1_1() throws Exception {
        producerCount = 1;
        destCount = 1;
        consumerCount = 1;

        createConnections();

        // Start 'em up.
        startClients();
        try {
            reportRates();
        } finally {
            stopServices();
        }
    }

    public void test_10_1_10() throws Exception {
        producerCount = FANIN_COUNT;
        consumerCount = FANOUT_COUNT;
        destCount = 1;

        createConnections();

        // Start 'em up.
        startClients();
        try {
            reportRates();
        } finally {
            stopServices();
        }
    }

    public void test_10_1_1() throws Exception {
        producerCount = FANIN_COUNT;
        destCount = 1;
        consumerCount = 1;

        createConnections();

        // Start 'em up.
        startClients();
        try {
            reportRates();
        } finally {
            stopServices();
        }
    }

    public void test_1_1_10() throws Exception {
        producerCount = 1;
        destCount = 1;
        consumerCount = FANOUT_COUNT;

        createConnections();

        // Start 'em up.
        startClients();
        try {
            reportRates();
        } finally {
            stopServices();
        }
    }

    public void test_2_2_2() throws Exception {
        producerCount = 2;
        destCount = 2;
        consumerCount = 2;

        createConnections();

        // Start 'em up.
        startClients();
        try {
            reportRates();
        } finally {
            stopServices();
        }
    }

    public void test_10_10_10() throws Exception {
        producerCount = 10;
        destCount = 10;
        consumerCount = 10;

        createConnections();

        // Start 'em up.
        startClients();
        try {
            reportRates();
        } finally {
            stopServices();
        }
    }

    /**
     * Tests 2 producers sending to 1 destination with 2 consumres, but with
     * consumers set to select only messages from each producer. 1 consumers is
     * set to slow, the other producer should be able to send quickly.
     * 
     * @throws Exception
     */
    public void test_2_2_2_SlowConsumer() throws Exception {
        producerCount = 2;
        destCount = 2;
        consumerCount = 2;

        createConnections();
        consumers.get(0).setThinkTime(50);

        // Start 'em up.
        startClients();
        try {
            reportRates();
        } finally {
            stopServices();
        }
    }

    public void test_2_2_2_Selector() throws Exception {
        producerCount = 2;
        destCount = 2;
        consumerCount = 2;

        createConnections();

        // Add properties to match producers to their consumers
        for (int i = 0; i < consumerCount; i++) {
            String property = "match" + i;
            consumers.get(i).setSelector(property);
            producers.get(i).setProperty(property);
        }

        // Start 'em up.
        startClients();
        try {
            reportRates();
        } finally {
            stopServices();
        }
    }

    /**
     * Test sending with 1 high priority sender. The high priority sender should
     * have higher throughput than the other low priority senders.
     * 
     * @throws Exception
     */
    public void test_2_1_1_HighPriorityProducer() throws Exception {

        producerCount = 2;
        destCount = 1;
        consumerCount = 1;

        createConnections();
        RemoteProducer producer = producers.get(0);
        producer.setPriority(1);
        producer.getRate().setName("High Priority Producer Rate");

        consumers.get(0).setThinkTime(1);

        // Start 'em up.
        startClients();
        try {

            System.out.println("Checking rates for test: " + getName());
            for (int i = 0; i < PERFORMANCE_SAMPLES; i++) {
                Period p = new Period();
                Thread.sleep(1000 * 5);
                System.out.println(producer.getRate().getRateSummary(p));
                System.out.println(totalProducerRate.getRateSummary(p));
                System.out.println(totalConsumerRate.getRateSummary(p));
                totalProducerRate.reset();
                totalConsumerRate.reset();
            }

        } finally {
            stopServices();
        }
    }

    /**
     * Test sending with 1 high priority sender. The high priority sender should
     * have higher throughput than the other low priority senders.
     * 
     * @throws Exception
     */
    public void test_2_1_1_MixedHighPriorityProducer() throws Exception {
        producerCount = 2;
        destCount = 1;
        consumerCount = 1;

        createConnections();
        RemoteProducer producer = producers.get(0);
        producer.setPriority(1);
        producer.setPriorityMod(3);
        producer.getRate().setName("High Priority Producer Rate");

        consumers.get(0).setThinkTime(1);

        // Start 'em up.
        startClients();
        try {

            System.out.println("Checking rates for test: " + getName());
            for (int i = 0; i < PERFORMANCE_SAMPLES; i++) {
                Period p = new Period();
                Thread.sleep(1000 * 5);
                System.out.println(producer.getRate().getRateSummary(p));
                System.out.println(totalProducerRate.getRateSummary(p));
                System.out.println(totalConsumerRate.getRateSummary(p));
                totalProducerRate.reset();
                totalConsumerRate.reset();
            }

        } finally {
            stopServices();
        }
    }

    private void reportRates() throws InterruptedException {
        System.out.println("Checking rates for test: " + getName() + ", " + (ptp ? "ptp" : "topic"));
        for (int i = 0; i < PERFORMANCE_SAMPLES; i++) {
            Period p = new Period();
            Thread.sleep(1000 * 5);
            System.out.println(totalProducerRate.getRateSummary(p));
            System.out.println(totalConsumerRate.getRateSummary(p));
            totalProducerRate.reset();
            totalConsumerRate.reset();
        }
    }

    private void createConnections() throws Exception, IOException, URISyntaxException {

        if (multibroker) {
            sendBroker = createBroker("SendBroker", sendBrokerBindURI, sendBrokerConnectURI);
            rcvBroker = createBroker("RcvBroker", receiveBrokerBindURI, receiveBrokerConnectURI);
            brokers.add(sendBroker);
            brokers.add(rcvBroker);
        } else {
            sendBroker = rcvBroker = createBroker("Broker", sendBrokerBindURI, sendBrokerConnectURI);
            brokers.add(sendBroker);
        }

        startBrokers();

        Destination[] dests = new Destination[destCount];

        for (int i = 0; i < destCount; i++) {
            Destination.SingleDestination bean = new Destination.SingleDestination();
            bean.setName(new AsciiBuffer("dest" + (i + 1)));
            bean.setDomain(ptp ? Router.QUEUE_DOMAIN : Router.TOPIC_DOMAIN);
            dests[i] = bean;
            if (ptp) {
                sendBroker.getDefaultVirtualHost().createQueue(dests[i]);
                if (multibroker) {
                    rcvBroker.getDefaultVirtualHost().createQueue(dests[i]);
                }
            }
        }

        for (int i = 0; i < producerCount; i++) {
            Destination destination = dests[i % destCount];
            RemoteProducer producer = createProducer(i, destination);
            producer.setPersistentDelivery(PERSISTENT);
            producers.add(producer);
        }

        for (int i = 0; i < consumerCount; i++) {
            Destination destination = dests[i % destCount];
            RemoteConsumer consumer = createConsumer(i, destination);
            consumer.setDurable(DURABLE);
            consumers.add(consumer);
        }

        // Create MultiBroker connections:
        // if (multibroker) {
        // Pipe<Message> pipe = new Pipe<Message>();
        // sendBroker.createBrokerConnection(rcvBroker, pipe);
        // rcvBroker.createBrokerConnection(sendBroker, pipe.connect());
        // }
    }

    private RemoteConsumer createConsumer(int i, Destination destination) throws URISyntaxException {
        RemoteConsumer consumer = createConsumer();
        consumer.setExceptionListener(new ExceptionListener() {
            public void exceptionThrown(Exception error) {
                if (!stopping.get()) {
                    System.err.println("Consumer Async Error:");
                    error.printStackTrace();
                }
            }
        });
        consumer.setUri(new URI(rcvBroker.getConnectUri()));
        consumer.setDestination(destination);
        consumer.setName("consumer" + (i + 1));
        consumer.setTotalConsumerRate(totalConsumerRate);
        consumer.setDispatcher(dispatcher);
        return consumer;
    }

    abstract protected RemoteConsumer createConsumer();

    private RemoteProducer createProducer(int id, Destination destination) throws URISyntaxException {
        RemoteProducer producer = cerateProducer();
        producer.setExceptionListener(new ExceptionListener() {
            public void exceptionThrown(Exception error) {
                if (!stopping.get()) {
                    System.err.println("Producer Async Error:");
                    error.printStackTrace();
                }
            }
        });
        producer.setUri(new URI(sendBroker.getConnectUri()));
        producer.setProducerId(id + 1);
        producer.setName("producer" + (id + 1));
        producer.setDestination(destination);
        producer.setMessageIdGenerator(msgIdGenerator);
        producer.setTotalProducerRate(totalProducerRate);
        producer.setDispatcher(dispatcher);
        return producer;
    }

    abstract protected RemoteProducer cerateProducer();

    private MessageBroker createBroker(String name, String bindURI, String connectUri) throws Exception {
        MessageBroker broker = new MessageBroker();
        broker.setName(name);
        broker.setBindUri(bindURI);
        broker.setConnectUri(connectUri);
        broker.setDispatcher(dispatcher);
        broker.setStore(createStore(broker));
        return broker;
    }

    protected Store createStore(MessageBroker broker) throws Exception {
        Store store = null;
        if (USE_KAHA_DB) {
            store = StoreFactory.createStore("kaha-db");
        } else {
            store = StoreFactory.createStore("memory");
        }

        store.setStoreDirectory(new File("test-data/broker-test/" + broker.getName()));
        store.setDeleteAllMessages(PURGE_STORE);
        return store;
    }

    private void stopServices() throws Exception {
        stopping.set(true);
        for (MessageBroker broker : brokers) {
            broker.stop();
        }
        for (RemoteProducer connection : producers) {
            connection.stop();
        }
        for (RemoteConsumer connection : consumers) {
            connection.stop();
        }
        if (dispatcher != null) {
            dispatcher.shutdown();
        }
    }

    private void startBrokers() throws Exception {
        for (MessageBroker broker : brokers) {
            broker.start();
        }
    }

    private void startClients() throws Exception {

        for (RemoteConsumer connection : consumers) {
            connection.start();
        }

        for (RemoteProducer connection : producers) {
            connection.start();
        }
    }

}
