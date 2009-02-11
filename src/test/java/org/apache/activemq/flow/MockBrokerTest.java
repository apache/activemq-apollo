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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.dispatch.PriorityPooledDispatcher;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.ISinkController;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.PrioritySizeLimiter;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.MetricCounter;
import org.apache.activemq.metric.Period;
import org.apache.activemq.queue.IQueue;
import org.apache.activemq.queue.Mapper;
import org.apache.activemq.queue.PartitionedQueue;
import org.apache.activemq.queue.SharedPriorityQueue;
import org.apache.activemq.queue.SharedQueue;
import org.apache.activemq.queue.Subscription;

public class MockBrokerTest extends TestCase {

    private static final int PERFORMANCE_SAMPLES = 3;

    private static final int IO_WORK_AMOUNT = 0;
    private static final int FANIN_COUNT = 10;
    private static final int FANOUT_COUNT = 10;

    private static final int PRIORITY_LEVELS = 10;
    private static final boolean USE_INPUT_QUEUES = true;

    // Set to put senders and consumers on separate brokers.
    private boolean multibroker = false;

    // Set to mockup up ptp:
    private boolean ptp = true;

    // Can be set to BLOCKING, POLLING or ASYNC
    private final int dispatchMode = AbstractTestConnection.ASYNC;
    // Set's the number of threads to use:
    private final int asyncThreadPoolSize = Runtime.getRuntime().availableProcessors();
    private boolean usePartitionedQueue = false;

    private int producerCount;
    private int consumerCount;
    private int destCount;

    MetricAggregator totalProducerRate = new MetricAggregator().name("Aggregate Producer Rate").unit("items");
    MetricAggregator totalConsumerRate = new MetricAggregator().name("Aggregate Consumer Rate").unit("items");

    MockBroker sendBroker;
    MockBroker rcvBroker;
    private ArrayList<MockBroker> brokers = new ArrayList<MockBroker>();
    private IDispatcher dispatcher;

    public interface DeliveryTarget {
        public IFlowSink<Message> getSink();

        public boolean match(Message message);
    }

    class MockBroker {
        private final TestFlowManager flowMgr;
        private final ArrayList<MockProducerConnection> producers = new ArrayList<MockProducerConnection>();
        private final ArrayList<MockConsumerConnection> consumers = new ArrayList<MockConsumerConnection>();
        private final ArrayList<BrokerConnection> brokerConns = new ArrayList<BrokerConnection>();

        private final HashMap<Destination, MockQueue> queues = new HashMap<Destination, MockQueue>();
        private final Router router;
        private int pCount;
        private int cCount;
        private final String name;
        public final int dispatchMode;

        public final IDispatcher dispatcher;
        public final int priorityLevels = PRIORITY_LEVELS;
        public final int ioWorkAmount = IO_WORK_AMOUNT;
        public final boolean useInputQueues = USE_INPUT_QUEUES;

        MockBroker(String name) {
            this.flowMgr = new TestFlowManager();
            this.router = new Router();
            this.name = name;
            this.dispatchMode = MockBrokerTest.this.dispatchMode;
            this.dispatcher = MockBrokerTest.this.dispatcher;
        }

        TestFlowManager getFlowManager() {
            return flowMgr;
        }

        public String getName() {
            return name;
        }

        public void createProducerConnection(Destination destination) {
            MockProducerConnection c = new MockProducerConnection("producer" + ++pCount, this, destination);
            producers.add(c);
        }

        public void createConsumerConnection(Destination destination, boolean ptp) {
            MockConsumerConnection c = new MockConsumerConnection("consumer" + ++cCount, this, destination);
            consumers.add(c);
            if (ptp) {
                queues.get(destination).addConsumer(c);
            } else {
                router.bind(c, destination);
            }

        }

        public void createClusterConnection(Destination destination) {
            MockConsumerConnection c = new MockConsumerConnection("consumer" + ++cCount, this, destination);
            consumers.add(c);
            router.bind(c, destination);
        }

        public void createQueue(Destination destination) {
            MockQueue queue = new MockQueue(this, destination);
            queues.put(destination, queue);
        }

        public void createBrokerConnection(MockBroker target, Pipe<Message> pipe) {
            BrokerConnection bc = new BrokerConnection(this, target, pipe);
            // Set up the pipe for polled access
            if (dispatchMode != AbstractTestConnection.BLOCKING) {
                pipe.setMode(Pipe.POLLING);
            }
            // Add subscriptions for the target's destinations:
            for (Destination d : target.router.lookupTable.keySet()) {
                router.bind(bc, d);
            }
            brokerConns.add(bc);
        }

        final void stopServices() throws Exception {
            for (MockProducerConnection connection : producers) {
                connection.stop();
            }
            for (MockConsumerConnection connection : consumers) {
                connection.stop();
            }
            for (BrokerConnection connection : brokerConns) {
                connection.stop();
            }
            for (MockQueue queue : queues.values()) {
                queue.stop();
            }
            dispatcher.shutdown();

        }

        final void startServices() throws Exception {
            dispatcher.start();
            for (MockConsumerConnection connection : consumers) {
                connection.start();
            }

            for (MockQueue queue : queues.values()) {
                queue.start();
            }

            for (MockProducerConnection connection : producers) {
                connection.start();
            }

            for (BrokerConnection connection : brokerConns) {
                connection.start();
            }
        }
    }

    private final AtomicLong msgIdGenerator = new AtomicLong();
    private final AtomicInteger prodcuerIdGenerator = new AtomicInteger();

    class MockProducerConnection extends AbstractTestConnection {

        MetricCounter producerRate = new MetricCounter();

        private final Destination destination;
        private int msgCounter;
        private String name;
        private String property;
        private Message next;
        private int msgPriority = 0;
        private int priorityMod = 0;
        int producerId = prodcuerIdGenerator.getAndIncrement();

        public MockProducerConnection(String name, MockBroker broker, Destination destination) {

            super(broker, name, broker.getFlowManager().createFlow(name), null);
            this.destination = destination;

            producerRate.name("Producer " + name + " Rate");
            totalProducerRate.add(producerRate);

        }

        /*
         * Gets the next message blocking until space is available for it.
         * (non-Javadoc)
         * 
         * @see com.progress.flow.AbstractTestConnection#getNextMessage()
         */
        public Message getNextMessage() throws InterruptedException {

            Message m = new Message(msgIdGenerator.getAndIncrement(), producerId, name + ++msgCounter, flow, destination, msgPriority);
            if (property != null) {
                m.setProperty(property);
            }
            simulateEncodingWork();
            input.getFlowController(m.getFlow()).waitForFlowUnblock();
            return m;
        }

        @Override
        protected void addReadReadyListener(final ReadReadyListener listener) {
            if (next == null) {
                next = new Message(msgIdGenerator.getAndIncrement(), producerId, name + ++msgCounter, flow, destination, msgPriority);
                if (property != null) {
                    next.setProperty(property);
                }
                simulateEncodingWork();
            }

            if (!input.getFlowController(next.getFlow()).addUnblockListener(new ISinkController.FlowUnblockListener<Message>() {
                public void onFlowUnblocked(ISinkController<Message> controller) {
                    listener.onReadReady();
                }
            })) {
                // Return value of false means that the controller didn't
                // register the listener because it was not blocked.
                listener.onReadReady();
            }
        }

        @Override
        public final Message pollNextMessage() {
            if (next == null) {
                int priority = msgPriority;
                if (priorityMod > 0) {
                    priority = msgCounter % priorityMod == 0 ? 0 : msgPriority;
                }

                next = new Message(msgIdGenerator.getAndIncrement(), producerId, name + ++msgCounter, flow, destination, priority);
                if (property != null) {
                    next.setProperty(property);
                }
                simulateEncodingWork();
            }

            if (input.getFlowController(next.getFlow()).isSinkBlocked()) {
                return null;
            }

            Message m = next;
            next = null;
            return m;
        }

        @Override
        public void messageReceived(Message m, ISourceController<Message> controller) {

            broker.router.route(controller, m);
            producerRate.increment();
        }

        @Override
        public void write(Message m, ISourceController<Message> controller) {
            // Noop
        }

    }

    class MockConsumerConnection extends AbstractTestConnection implements DeliveryTarget {

        MetricCounter consumerRate = new MetricCounter();
        private final Destination destination;
        private String selector;
        private boolean autoRelease = false;

        private long thinkTime = 0;

        public MockConsumerConnection(String name, MockBroker broker, Destination destination) {
            super(broker, name, broker.getFlowManager().createFlow(destination.getName()), null);
            this.destination = destination;
            output.setAutoRelease(autoRelease);
            consumerRate.name("Consumer " + name + " Rate");
            totalConsumerRate.add(consumerRate);

        }

        public Destination getDestination() {
            return destination;
        }

        public String getSelector() {
            return selector;
        }

        public void setThinkTime(long time) {
            thinkTime = time;
        }

        @Override
        protected synchronized Message getNextMessage() throws InterruptedException {
            wait();
            return null;
        }

        @Override
        protected void addReadReadyListener(final ReadReadyListener listener) {
            return;
        }

        public Message pollNextMessage() {
            return null;
        }

        @Override
        protected void messageReceived(Message m, ISourceController<Message> controller) {
        }

        @Override
        protected void write(final Message m, final ISourceController<Message> controller) throws InterruptedException {
            if (!m.isSystem()) {
                // /IF we are async don't tie up the calling thread
                // schedule dispatch complete for later.
                if (dispatchMode == ASYNC && thinkTime > 0) {
                    Runnable acker = new Runnable() {
                        public void run() {
                            if (thinkTime > 0) {
                                try {
                                    Thread.sleep(thinkTime);
                                } catch (InterruptedException e) {
                                }
                            }
                            simulateEncodingWork();
                            if (!autoRelease) {
                                controller.elementDispatched(m);
                            }
                            consumerRate.increment();
                        }
                    };

                    broker.dispatcher.schedule(acker, thinkTime, TimeUnit.MILLISECONDS);
                } else {
                    simulateEncodingWork();
                    if (!autoRelease) {
                        controller.elementDispatched(m);
                    }
                    consumerRate.increment();
                }
            }
        }

        public IFlowSink<Message> getSink() {
            return output;
        }

        public boolean match(Message message) {
            if (selector == null)
                return true;
            return message.match(selector);
        }

    }

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

    final private Mapper<Long, Message> keyExtractor = new Mapper<Long, Message>() {
        public Long map(Message element) {
            return element.getMsgId();
        }
    };
    final private Mapper<Integer, Message> partitionMapper = new Mapper<Integer, Message>() {
        public Integer map(Message element) {
            // we modulo 10 to have at most 10 partitions which the producers
            // gets split across.
            return (int) (element.getProducerId() % 10);
        }
    };

    private class MockQueue implements DeliveryTarget {
        HashMap<MockConsumerConnection, Subscription<Message>> subs = new HashMap<MockConsumerConnection, Subscription<Message>>();
        private final Destination destination;
        private final IQueue<Long, Message> queue;
        private final MockBroker broker;

        MockQueue(MockBroker broker, Destination destination) {
            this.broker = broker;
            this.destination = destination;
            this.queue = createQueue();
            broker.router.bind(this, destination);
        }

        private IQueue<Long, Message> createQueue() {

            if (usePartitionedQueue) {
                PartitionedQueue<Integer, Long, Message> queue = new PartitionedQueue<Integer, Long, Message>() {
                    @Override
                    protected IQueue<Long, Message> cratePartition(Integer partitionKey) {
                        return createSharedFlowQueue();
                    }
                };
                queue.setPartitionMapper(partitionMapper);
                queue.setResourceName(destination.getName());
                return queue;
            } else {
                return createSharedFlowQueue();
            }
        }

        private IQueue<Long, Message> createSharedFlowQueue() {
            if (broker.priorityLevels > 1) {
                PrioritySizeLimiter<Message> limiter = new PrioritySizeLimiter<Message>(100, 1, broker.priorityLevels);
                limiter.setPriorityMapper(Message.PRIORITY_MAPPER);
                SharedPriorityQueue<Long, Message> queue = new SharedPriorityQueue<Long, Message>(destination.getName(), limiter);
                queue.setKeyMapper(keyExtractor);
                queue.setAutoRelease(true);
                queue.setDispatcher(broker.dispatcher);
                return queue;
            } else {
                SizeLimiter<Message> limiter = new SizeLimiter<Message>(100, 1);
                SharedQueue<Long, Message> queue = new SharedQueue<Long, Message>(destination.getName(), limiter);
                queue.setKeyMapper(keyExtractor);
                queue.setAutoRelease(true);
                queue.setDispatcher(broker.dispatcher);
                return queue;
            }
        }

        public final void deliver(ISourceController<Message> source, Message msg) {

            queue.add(msg, source);
        }

        public String getSelector() {
            return null;
        }

        public final Destination getDestination() {
            return destination;
        }

        public final void addConsumer(final MockConsumerConnection dt) {
            Subscription<Message> sub = new Subscription<Message>() {
                public boolean isPreAcquired() {
                    return true;
                }

                public boolean matches(Message message) {
                    return dt.match(message);
                }

                public boolean isRemoveOnDispatch() {
                    return true;
                }

                public IFlowSink<Message> getSink() {
                    return dt.getSink();
                }

                @Override
                public String toString() {
                    return getSink().toString();
                }
            };
            subs.put(dt, sub);
            queue.addSubscription(sub);
        }

        public boolean removeSubscirption(final DeliveryTarget dt) {
            Subscription<Message> sub = subs.remove(dt);
            if (sub != null) {
                return queue.removeSubscription(sub);
            }
            return false;
        }

        public void start() throws Exception {
        }

        public void stop() throws Exception {
        }

        public IFlowSink<Message> getSink() {
            return queue;
        }

        public boolean match(Message message) {
            return true;
        }

    }

    private class Router {
        private final HashMap<Destination, Collection<DeliveryTarget>> lookupTable = new HashMap<Destination, Collection<DeliveryTarget>>();

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

    private void createConnections() {

        if (dispatchMode == AbstractTestConnection.ASYNC || dispatchMode == AbstractTestConnection.POLLING) {
            dispatcher = new PriorityPooledDispatcher("BrokerDispatcher", asyncThreadPoolSize, Message.MAX_PRIORITY);
            FlowController.setFlowExecutor(dispatcher.createPriorityExecutor(Message.MAX_PRIORITY));
        }

        if (multibroker) {
            sendBroker = new MockBroker("SendBroker");
            rcvBroker = new MockBroker("RcvBroker");
            brokers.add(sendBroker);
            brokers.add(rcvBroker);
        } else {
            sendBroker = rcvBroker = new MockBroker("Broker");
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
