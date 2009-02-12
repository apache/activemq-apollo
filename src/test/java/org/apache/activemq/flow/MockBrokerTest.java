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
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.dispatch.PriorityPooledDispatcher;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.ISourceController;
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
    private boolean ptp = true;

    // Can be set to BLOCKING, POLLING or ASYNC
    final int dispatchMode = AbstractTestConnection.ASYNC;
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
    final AtomicInteger prodcuerIdGenerator = new AtomicInteger();

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

    class Router {
        final HashMap<Destination, Collection<DeliveryTarget>> lookupTable = new HashMap<Destination, Collection<DeliveryTarget>>();

        final synchronized void bind(DeliveryTarget dt, Destination destination) {
            Collection<DeliveryTarget> targets = lookupTable.get(destination);
            if (targets == null) {
                targets = new ArrayList<DeliveryTarget>();
                lookupTable.put(destination, targets);
            }
            targets.add(dt);
        }

        final void route(ISourceController<Message> source, Message msg) {
            Collection<DeliveryTarget> targets = lookupTable.get(msg.getDestination());
            for (DeliveryTarget dt : targets) {
                if (dt.match(msg)) {
                    dt.getSink().add(msg, source);
                }
            }
        }
    }

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
        MockProducerConnection producer = sendBroker.producers.get(0);
        producer.msgPriority = 1;
        producer.producerRate.setName("High Priority Producer Rate");

        rcvBroker.consumers.get(0).setThinkTime(1);

        // Start 'em up.
        startServices();
        try {

            System.out.println("Checking rates for test: " + getName());
            for (int i = 0; i < PERFORMANCE_SAMPLES; i++) {
                Period p = new Period();
                Thread.sleep(1000 * 5);
                System.out.println(producer.producerRate.getRateSummary(p));
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
        MockProducerConnection producer = sendBroker.producers.get(0);
        producer.msgPriority = 1;
        producer.priorityMod = 3;
        producer.producerRate.setName("High Priority Producer Rate");

        rcvBroker.consumers.get(0).setThinkTime(1);

        // Start 'em up.
        startServices();
        try {

            System.out.println("Checking rates for test: " + getName());
            for (int i = 0; i < PERFORMANCE_SAMPLES; i++) {
                Period p = new Period();
                Thread.sleep(1000 * 5);
                System.out.println(producer.producerRate.getRateSummary(p));
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
            rcvBroker.consumers.get(i).selector = sendBroker.producers.get(i).property = "match" + i;
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

        if (dispatchMode == AbstractTestConnection.ASYNC || dispatchMode == AbstractTestConnection.POLLING) {
            dispatcher = new PriorityPooledDispatcher("BrokerDispatcher", asyncThreadPoolSize, Message.MAX_PRIORITY);
            FlowController.setFlowExecutor(dispatcher.createPriorityExecutor(Message.MAX_PRIORITY));
        }

        if (multibroker) {
            sendBroker = new MockBroker(this, "SendBroker");
            rcvBroker = new MockBroker(this, "RcvBroker");
            brokers.add(sendBroker);
            brokers.add(rcvBroker);
        } else {
            sendBroker = rcvBroker = new MockBroker(this, "Broker");
            brokers.add(sendBroker);
        }

        Destination[] dests = new Destination[destCount];

        for (int i = 0; i < destCount; i++) {
            dests[i] = new Destination("dest" + (i + 1));
            if (ptp) {
                sendBroker.createQueue(dests[i]);
                if (multibroker) {
                    rcvBroker.createQueue(dests[i]);
                }
            }
        }

        for (int i = 0; i < producerCount; i++) {
            sendBroker.createProducerConnection(dests[i % destCount]);
        }
        for (int i = 0; i < consumerCount; i++) {
            rcvBroker.createConsumerConnection(dests[i % destCount], ptp);
        }

        // Create MultiBroker connections:
        if (multibroker) {
            Pipe<Message> pipe = new Pipe<Message>();
            sendBroker.createBrokerConnection(rcvBroker, pipe);
            rcvBroker.createBrokerConnection(sendBroker, pipe.connect());
        }
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
