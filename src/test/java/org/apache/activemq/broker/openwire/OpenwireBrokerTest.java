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
package org.apache.activemq.broker.openwire;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.Queue;
import org.apache.activemq.broker.Router;
import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.dispatch.PriorityDispatcher;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.Period;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.queue.Mapper;

public class OpenwireBrokerTest extends TestCase {

    protected static final int PERFORMANCE_SAMPLES = 3000000;

    protected static final int IO_WORK_AMOUNT = 0;
    protected static final int FANIN_COUNT = 10;
    protected static final int FANOUT_COUNT = 10;

    protected static final int PRIORITY_LEVELS = 10;
    protected static final boolean USE_INPUT_QUEUES = true;

    // Set to put senders and consumers on separate brokers.
    protected boolean multibroker = false;

    // Set to mockup up ptp:
    protected boolean ptp = false;

    // Set to use tcp IO
    protected boolean tcp = true;
    // set to force marshalling even in the NON tcp case.
    protected boolean forceMarshalling = false;

    protected String sendBrokerURI;
    protected String receiveBrokerURI;

    // Set's the number of threads to use:
    protected final int asyncThreadPoolSize = Runtime.getRuntime().availableProcessors();
    protected boolean usePartitionedQueue = false;

    protected int producerCount;
    protected int consumerCount;
    protected int destCount;

    protected MetricAggregator totalProducerRate = new MetricAggregator().name("Aggregate Producer Rate").unit("items");
    protected MetricAggregator totalConsumerRate = new MetricAggregator().name("Aggregate Consumer Rate").unit("items");

    protected Broker sendBroker;
    protected Broker rcvBroker;
    protected ArrayList<Broker> brokers = new ArrayList<Broker>();
    protected IDispatcher dispatcher;
    protected final AtomicLong msgIdGenerator = new AtomicLong();

    final ArrayList<RemoteProducer> producers = new ArrayList<RemoteProducer>();
    final ArrayList<RemoteConsumer> consumers = new ArrayList<RemoteConsumer>();

    static public final Mapper<AsciiBuffer, MessageDelivery> KEY_MAPPER = new Mapper<AsciiBuffer, MessageDelivery>() {
        public AsciiBuffer map(MessageDelivery element) {
            return element.getMsgId();
        }
    };
    static public final Mapper<Integer, MessageDelivery> PARTITION_MAPPER = new Mapper<Integer, MessageDelivery>() {
        public Integer map(MessageDelivery element) {
            // we modulo 10 to have at most 10 partitions which the producers
            // gets split across.
            return (int) (element.getProducerId().hashCode() % 10);
        }
    };

    @Override
    protected void setUp() throws Exception {
        dispatcher = createDispatcher();
        dispatcher.start();
        if (tcp) {
            sendBrokerURI = "tcp://localhost:10000";
            receiveBrokerURI = "tcp://localhost:20000";
        } else {
            if (forceMarshalling) {
                sendBrokerURI = "pipe://SendBroker";
                receiveBrokerURI = "pipe://ReceiveBroker";
            } else {
                sendBrokerURI = "pipe://SendBroker";
                receiveBrokerURI = "pipe://ReceiveBroker";
            }
        }
    }

    protected IDispatcher createDispatcher() {
        return PriorityDispatcher.createPriorityDispatchPool("BrokerDispatcher", Broker.MAX_PRIORITY, asyncThreadPoolSize);
    }
    
    public void test_10_10_10() throws Exception {
        producerCount = 2;
        destCount = 2;
        consumerCount = 2;

        createConnections();

        // Start 'em up.
        startServices();
        try {
            reportRates();
        } finally {
            stopServices();
        }
    }

    public void test_1_1_0() throws Exception {
        producerCount = 1;
        destCount = 1;

        createConnections();

        // Start 'em up.
        startServices();
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
        startServices();
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
        startServices();
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
        startServices();
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
        startServices();
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
        startServices();
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
        startServices();
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
        startServices();
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
        startServices();
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
        startServices();
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

    private void createConnections() throws IOException, URISyntaxException {

        if (multibroker) {
            sendBroker = createBroker("SendBroker", sendBrokerURI);
            rcvBroker = createBroker("RcvBroker", receiveBrokerURI);
            brokers.add(sendBroker);
            brokers.add(rcvBroker);
        } else {
            sendBroker = rcvBroker = createBroker("Broker", sendBrokerURI);
            brokers.add(sendBroker);
        }

        Destination[] dests = new Destination[destCount];

        for (int i = 0; i < destCount; i++) {
            Destination.SingleDestination bean = new Destination.SingleDestination();
            bean.setName(new AsciiBuffer("dest" + (i + 1)));
            bean.setDomain(ptp ? Router.QUEUE_DOMAIN : Router.TOPIC_DOMAIN);
            dests[i] = bean;
            if (ptp) {
                Queue queue = createQueue(sendBroker, dests[i]);
                sendBroker.addQueue(queue);
                if (multibroker) {
                    queue = createQueue(rcvBroker, dests[i]);
                    rcvBroker.addQueue(queue);
                }
            }
        }

        for (int i = 0; i < producerCount; i++) {
            Destination destination = dests[i % destCount];
            RemoteProducer producer = createProducer(i, destination);
            producers.add(producer);
        }

        for (int i = 0; i < consumerCount; i++) {
            Destination destination = dests[i % destCount];
            RemoteConsumer consumer = createConsumer(i, destination);
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
        RemoteConsumer consumer = new RemoteConsumer();
        consumer.setUri(new URI(rcvBroker.getUri()));
        consumer.setDestination(destination);
        consumer.setName("consumer" + (i + 1));
        consumer.setTotalConsumerRate(totalConsumerRate);
        consumer.setDispatcher(dispatcher);
        return consumer;
    }

    private RemoteProducer createProducer(int id, Destination destination) throws URISyntaxException {
        RemoteProducer producer = new RemoteProducer();
        producer.setUri(new URI(sendBroker.getUri()));
        producer.setProducerId(id + 1);
        producer.setName("producer" + (id + 1));
        producer.setDestination(destination);
        producer.setMessageIdGenerator(msgIdGenerator);
        producer.setTotalProducerRate(totalProducerRate);
        producer.setDispatcher(dispatcher);
        return producer;
    }

    private Queue createQueue(Broker broker, Destination destination) {
        Queue queue = new Queue();
        queue.setBroker(broker);
        queue.setDestination(destination);
        queue.setKeyExtractor(KEY_MAPPER);
        if (usePartitionedQueue) {
            queue.setPartitionMapper(PARTITION_MAPPER);
        }
        return queue;
    }

    private Broker createBroker(String name, String uri) {
        Broker broker = new Broker();
        broker.setName(name);
        broker.setUri(uri);
        broker.setDispatcher(dispatcher);
        return broker;
    }

    private void stopServices() throws Exception {
        for (RemoteProducer connection : producers) {
            connection.stop();
        }
        for (RemoteConsumer connection : consumers) {
            connection.stop();
        }
        for (Broker broker : brokers) {
            broker.stop();
        }
        if (dispatcher != null) {
            dispatcher.shutdown();
        }
    }

    private void startServices() throws Exception {
        for (Broker broker : brokers) {
            broker.start();
        }
        for (RemoteConsumer connection : consumers) {
            connection.start();
        }

        for (RemoteProducer connection : producers) {
            connection.start();
        }
    }

}
