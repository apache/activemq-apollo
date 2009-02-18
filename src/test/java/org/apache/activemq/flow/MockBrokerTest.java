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
package org.apache.activemq.flow;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.dispatch.PriorityPooledDispatcher;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.flow.Commands.Destination.DestinationBean;
import org.apache.activemq.flow.Commands.Destination.DestinationBuffer;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.Period;
import org.apache.activemq.queue.Mapper;
import org.apache.activemq.transport.nio.SelectorManager;

public class MockBrokerTest extends TestCase {

    protected static final int PERFORMANCE_SAMPLES = 30000;

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

    protected MockBroker sendBroker;
    protected MockBroker rcvBroker;
    protected ArrayList<MockBroker> brokers = new ArrayList<MockBroker>();
    protected IDispatcher dispatcher;
    protected final AtomicLong msgIdGenerator = new AtomicLong();

    static public final Mapper<Long, Message> KEY_MAPPER = new Mapper<Long, Message>() {
        public Long map(Message element) {
            return element.getMsgId();
        }
    };
    static public final Mapper<Integer, Message> PARTITION_MAPPER = new Mapper<Integer, Message>() {
        public Integer map(Message element) {
            // we modulo 10 to have at most 10 partitions which the producers
            // gets split across.
            return (int) (element.getProducerId() % 10);
        }
    };

    @Override
    protected void setUp() throws Exception {
        if( tcp ) {
            sendBrokerURI = "tcp://localhost:10000?wireFormat=proto";
            receiveBrokerURI = "tcp://localhost:20000?wireFormat=proto";
        } else {
            if( forceMarshalling ) {
                sendBrokerURI = "pipe://SendBroker?wireFormat=proto";
                receiveBrokerURI = "pipe://ReceiveBroker?wireFormat=proto";
            } else {
                sendBrokerURI = "pipe://SendBroker";
                receiveBrokerURI = "pipe://ReceiveBroker";
            }
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

    public void test_10_10_10() throws Exception {
        producerCount = FANIN_COUNT;
        destCount = FANIN_COUNT;
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
        rcvBroker.consumers.get(0).setThinkTime(5);

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
            rcvBroker.consumers.get(i).setSelector(property);
            sendBroker.producers.get(i).setProperty(property);
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
        RemoteProducer producer = sendBroker.producers.get(0);
        producer.setPriority(1);
        producer.getRate().setName("High Priority Producer Rate");

        rcvBroker.consumers.get(0).setThinkTime(1);

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
        RemoteProducer producer = sendBroker.producers.get(0);
        producer.setPriority(1);
        producer.setPriorityMod(3);
        producer.getRate().setName("High Priority Producer Rate");

        rcvBroker.consumers.get(0).setThinkTime(1);

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
        System.out.println("Checking rates for test: " + getName()+", "+(ptp?"ptp":"topic"));
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

        dispatcher = new PriorityPooledDispatcher("BrokerDispatcher", asyncThreadPoolSize, Message.MAX_PRIORITY);
        FlowController.setFlowExecutor(dispatcher.createPriorityExecutor(Message.MAX_PRIORITY));
                
        if (multibroker) {
            sendBroker = createBroker("SendBroker", sendBrokerURI);
            rcvBroker = createBroker("RcvBroker", receiveBrokerURI);
            brokers.add(sendBroker);
            brokers.add(rcvBroker);
        } else {
            sendBroker = rcvBroker = createBroker("Broker", sendBrokerURI);
            brokers.add(sendBroker);
        }

        DestinationBuffer[] dests = new DestinationBuffer[destCount];

        for (int i = 0; i < destCount; i++) {
            DestinationBean bean = new DestinationBean();
            bean.setName("dest" + (i + 1));
            bean.setPtp(ptp);
            dests[i] = bean.freeze();
            if (ptp) {
                MockQueue queue = createQueue(sendBroker, dests[i]);
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
            sendBroker.producers.add(producer);
        }
        
        for (int i = 0; i < consumerCount; i++) {
            Destination destination = dests[i % destCount];
            RemoteConsumer consumer = createConsumer(i, destination);
            sendBroker.consumers.add(consumer);
        }

        // Create MultiBroker connections:
//        if (multibroker) {
//            Pipe<Message> pipe = new Pipe<Message>();
//            sendBroker.createBrokerConnection(rcvBroker, pipe);
//            rcvBroker.createBrokerConnection(sendBroker, pipe.connect());
//        }
    }

    private RemoteConsumer createConsumer(int i, Destination destination) {
        RemoteConsumer consumer = new RemoteConsumer();
        consumer.setBroker(rcvBroker);
        consumer.setDestination(destination);
        consumer.setName("consumer"+(i+1));
        consumer.setTotalConsumerRate(totalConsumerRate);
        consumer.setDispatcher(dispatcher);
        return consumer;
    }

    private RemoteProducer createProducer(int id, Destination destination) {
        RemoteProducer producer = new RemoteProducer();
        producer.setBroker(sendBroker);
        producer.setProducerId(id+1);
        producer.setName("producer" +(id+1));
        producer.setDestination(destination);
        producer.setMessageIdGenerator(msgIdGenerator);
        producer.setTotalProducerRate(totalProducerRate);
        producer.setDispatcher(dispatcher);
        return producer;
    }

    private MockQueue createQueue(MockBroker broker, Destination destination) {
        MockQueue queue = new MockQueue();
        queue.setBroker(broker);
        queue.setDestination(destination);
        queue.setKeyExtractor(KEY_MAPPER);
        if( usePartitionedQueue ) {
            queue.setPartitionMapper(PARTITION_MAPPER);
        }
        return queue;
    }

    private MockBroker createBroker(String name, String uri) {
        MockBroker broker = new MockBroker();
        broker.setName(name);
        broker.setUri(uri);
        broker.setDispatcher(dispatcher);
        return broker;
    }

    private void stopServices() throws Exception {
        if (dispatcher != null) {
            dispatcher.shutdown();
        }
        for (MockBroker broker : brokers) {
            broker.stopServices();
        }
    }

    private void startServices() throws Exception {
        for (MockBroker broker : brokers) {
            broker.startServices();
        }
        SelectorManager.SINGLETON.setChannelExecutor(dispatcher.createPriorityExecutor(PRIORITY_LEVELS));
    }

}
