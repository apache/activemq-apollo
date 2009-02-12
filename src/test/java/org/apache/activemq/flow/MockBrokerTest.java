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
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.Period;
import org.apache.activemq.queue.Mapper;

public class MockBrokerTest extends TestCase {

    private static final int PERFORMANCE_SAMPLES = 3;

    static final int IO_WORK_AMOUNT = 0;
    private static final int FANIN_COUNT = 10;
    private static final int FANOUT_COUNT = 10;

    static final int PRIORITY_LEVELS = 10;
    static final boolean USE_INPUT_QUEUES = true;

    // Set to put senders and consumers on separate brokers.
    private boolean multibroker = false;

    // Set to mockup up ptp:
    boolean ptp = false;

    // Set to use tcp IO
    boolean tcp = false;

    // Can be set to BLOCKING, POLLING or ASYNC
    public final static int DISPATCH_MODE = AbstractTestConnection.ASYNC;
    // Set's the number of threads to use:
    private final int asyncThreadPoolSize = Runtime.getRuntime().availableProcessors();
    boolean usePartitionedQueue = false;

    private int producerCount;
    private int consumerCount;
    private int destCount;

    MetricAggregator totalProducerRate = new MetricAggregator().name("Aggregate Producer Rate").unit("items");
    MetricAggregator totalConsumerRate = new MetricAggregator().name("Aggregate Consumer Rate").unit("items");

    MockBroker sendBroker;
    MockBroker rcvBroker;
    private ArrayList<MockBroker> brokers = new ArrayList<MockBroker>();
    IDispatcher dispatcher;

    public interface DeliveryTarget {
        public IFlowSink<Message> getSink();

        public boolean match(Message message);
    }

    final AtomicLong msgIdGenerator = new AtomicLong();

    class BrokerConnection extends AbstractTestConnection implements DeliveryTarget {
        private final Pipe<Message> pipe;
        private final MockBroker local;

        BrokerConnection(MockBroker local, MockBroker remote, Pipe<Message> pipe) {
            super(local, remote.getName(), null, pipe);
            this.pipe = pipe;
            this.local = local;
        }

        @Override
        protected Message getNextMessage() throws InterruptedException {
            return pipe.read();
        }

        @Override
        protected void addReadReadyListener(final ReadReadyListener listener) {
            pipe.setReadReadyListener(new Pipe.ReadReadyListener<Message>() {
                public void onReadReady(Pipe<Message> pipe) {
                    listener.onReadReady();
                }
            });
        }

        public Message pollNextMessage() {
            return pipe.poll();
        }

        @Override
        protected void messageReceived(Message m, ISourceController<Message> controller) {

            m = new Message(m);
            m.hopCount++;

            local.router.route(controller, m);
        }

        @Override
        protected void write(Message m, ISourceController<Message> controller) throws InterruptedException {
            pipe.write(m);
        }

        public IFlowSink<Message> getSink() {
            return output;
        }

        public boolean match(Message message) {
            // Avoid loops:
            if (message.hopCount > 0) {
                return false;
            }

            return true;
        }
    }

    final Mapper<Long, Message> keyExtractor = new Mapper<Long, Message>() {
        public Long map(Message element) {
            return element.getMsgId();
        }
    };
    final Mapper<Integer, Message> partitionMapper = new Mapper<Integer, Message>() {
        public Integer map(Message element) {
            // we modulo 10 to have at most 10 partitions which the producers
            // gets split across.
            return (int) (element.getProducerId() % 10);
        }
    };

    private void reportRates() throws InterruptedException {
        System.out.println("Checking rates for test: " + getName());
        for (int i = 0; i < PERFORMANCE_SAMPLES; i++) {
            Period p = new Period();
            Thread.sleep(1000 * 5);
            System.out.println(totalProducerRate.getRateSummary(p));
            System.out.println(totalConsumerRate.getRateSummary(p));
            totalProducerRate.reset();
            totalConsumerRate.reset();
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
    
    private void createConnections() throws IOException, URISyntaxException {

        if (DISPATCH_MODE == AbstractTestConnection.ASYNC || DISPATCH_MODE == AbstractTestConnection.POLLING) {
            dispatcher = new PriorityPooledDispatcher("BrokerDispatcher", asyncThreadPoolSize, Message.MAX_PRIORITY);
            FlowController.setFlowExecutor(dispatcher.createPriorityExecutor(Message.MAX_PRIORITY));
        }

        if (multibroker) {
            if( tcp ) {
                sendBroker = createBroker("SendBroker", "tcp://localhost:10000?wireFormat=test");
                rcvBroker = createBroker("RcvBroker", "tcp://localhost:20000?wireFormat=test");
            } else {
                sendBroker = createBroker("SendBroker", "pipe://SendBroker");
                rcvBroker = createBroker("RcvBroker", "pipe://RcvBroker");
            }
            brokers.add(sendBroker);
            brokers.add(rcvBroker);
        } else {
            if( tcp ) {
                sendBroker = rcvBroker = createBroker("Broker", "tcp://localhost:10000?wireFormat=test");
            } else {
                sendBroker = rcvBroker = createBroker("Broker", "pipe://Broker");
            }
            
            brokers.add(sendBroker);
        }

        Destination[] dests = new Destination[destCount];

        for (int i = 0; i < destCount; i++) {
            dests[i] = new Destination("dest" + (i + 1), ptp);
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
        return producer;
    }

    private MockQueue createQueue(MockBroker broker, Destination destination) {
        MockQueue queue = new MockQueue();
        queue.setBroker(broker);
        queue.setDestination(destination);
        queue.setKeyExtractor(keyExtractor);
        if( usePartitionedQueue ) {
            queue.setPartitionMapper(partitionMapper);
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
        for (MockBroker broker : brokers) {
            broker.stopServices();
        }
        if (dispatcher != null) {
            dispatcher.shutdown();
        }

    }

    private void startServices() throws Exception {
        for (MockBroker broker : brokers) {
            broker.startServices();
        }
    }

}
