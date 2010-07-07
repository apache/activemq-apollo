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
package org.apache.activemq.perf.broker;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.JMSException;

import junit.framework.TestCase;

import org.apache.activemq.apollo.broker.BrokerDatabase;
import org.apache.activemq.apollo.broker.BrokerQueueStore;
import org.apache.activemq.apollo.broker.MessageDelivery;
import org.apache.activemq.broker.openwire.OpenWireMessageDelivery;
import org.apache.activemq.broker.openwire.OpenWireMessageDelivery.PersistListener;
import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.StoreFactory;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.flow.AbstractLimitedFlowResource;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowController;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.ISinkController;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.flow.ISinkController.FlowUnblockListener;
import org.apache.activemq.apollo.util.metric.MetricAggregator;
import org.apache.activemq.apollo.util.metric.MetricCounter;
import org.apache.activemq.metric.Period;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.queue.AbstractFlowRelay;
import org.apache.activemq.queue.IQueue;
import org.apache.activemq.queue.QueueDispatchTarget;
import org.apache.activemq.queue.SingleFlowRelay;
import org.apache.activemq.queue.Subscription;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.hawtdispatch.DispatchQueue;

import static org.apache.activemq.dispatch.DispatchOption.*;

public class SharedQueuePerfTest extends TestCase {

    private static int PERFORMANCE_SAMPLES = 5;

    BrokerDatabase database;
    BrokerQueueStore queueStore;
    private static final boolean USE_KAHA_DB = true;
    private static final boolean PERSISTENT = true;
    private static final boolean PURGE_STORE = true;
    // Producers send sync and operations are never canceled. 
    private static final boolean TEST_MAX_STORE_LATENCY = false;
    private static final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();

    protected MetricAggregator totalProducerRate = new MetricAggregator().name("Aggregate Producer Rate").unit("items");
    protected MetricAggregator totalConsumerRate = new MetricAggregator().name("Aggregate Consumer Rate").unit("items");

    protected ArrayList<Consumer> consumers = new ArrayList<Consumer>();
    protected ArrayList<Producer> producers = new ArrayList<Producer>();
    protected ArrayList<IQueue<Long, MessageDelivery>> queues = new ArrayList<IQueue<Long, MessageDelivery>>();

    protected int consumerStartDelay = 0;

    protected void startServices() throws Exception {
        database = new BrokerDatabase(createStore());
        database.setDispatchQueue(Dispatch.createQueue());
        if( TEST_MAX_STORE_LATENCY ) {
        	database.setFlushDelay(0);
        	database.setStoreBypass(false);
        }
        database.start();
        queueStore = new BrokerQueueStore();
        queueStore.setDatabase(database);
        queueStore.loadQueues();
    }

    protected void stopServices() throws Exception {
        database.stop();
        consumers.clear();
        producers.clear();
        queues.clear();
    }

    protected Store createStore() throws Exception {
        Store store = null;
        if (USE_KAHA_DB) {
            store = StoreFactory.createStore("kaha-db");
        } else {
            store = StoreFactory.createStore("memory");
        }

        store.setStoreDirectory(new File("test-data/shared-queue-perf-test/"));
        store.setDeleteAllMessages(PURGE_STORE);
        return store;
    }

    protected void cleanup() throws Exception {
        consumers.clear();
        producers.clear();
        queues.clear();
        stopServices();
        consumerStartDelay = 0;
    }

    public void test1_1_1() throws Exception {
        startServices();
        try {
            createQueues(1);
            createProducers(1);
            createConsumers(1);
            doTest();

        } finally {
            cleanup();
        }
    }

    public void test10_10_10() throws Exception {
        startServices();
        try {
            createQueues(10);
            createProducers(10);
            createConsumers(10);
            doTest();

        } finally {
            cleanup();
        }
    }

    public void test10_1_10() throws Exception {
        startServices();
        try {
            createQueues(1);
            createProducers(10);
            createConsumers(10);
            doTest();

        } finally {
            cleanup();
        }
    }

    public void test10_1_1() throws Exception {
        startServices();
        try {
            createQueues(10);
            createProducers(10);
            createConsumers(10);
            doTest();

        } finally {
            cleanup();
        }
    }

    public void test1_1_10() throws Exception {
        startServices();
        try {
            createQueues(10);
            createProducers(10);
            createConsumers(10);
            doTest();

        } finally {
            cleanup();
        }
    }

    private void doTest() throws Exception {

        try {
            // Start queues:
            for (IQueue<Long, MessageDelivery> queue : queues) {
                queue.start();
            }

            Runnable startConsumers = new Runnable() {
                public void run() {
                    // Start consumers:
                    for (Consumer consumer : consumers) {
                        consumer.start();
                    }
                }
            };

            if (consumerStartDelay > 0) {
                Dispatch.getGlobalQueue().dispatchAfter(consumerStartDelay, TimeUnit.SECONDS, startConsumers);
            } else {
                startConsumers.run();
            }

            // Start producers:
            for (Producer producer : producers) {
                producer.start();
            }
            reportRates();
        } finally {
            // Stop producers:
            for (Producer producer : producers) {
                producer.stop();
            }

            // Stop consumers:
            for (Consumer consumer : consumers) {
                consumer.stop();
            }

            // Stop queues:
            for (IQueue<Long, MessageDelivery> queue : queues) {
                queue.stop();
            }
        }
    }

    private final void createQueues(int count) {
        for (int i = 0; i < count; i++) {
            IQueue<Long, MessageDelivery> queue = queueStore.createSharedQueue("queue-" + (i + 1));
            queue.setDispatchPriority(1);
            queues.add(queue);
        }
    }

    private final void createProducers(int count) {
        for (int i = 0; i < count; i++) {
            Producer producer = new Producer("producer" + (i + 1), queues.get(i % queues.size()));
            producers.add(producer);
        }
    }

    private final void createConsumers(int count) {
        for (int i = 0; i < count; i++) {
            Consumer consumer = new Consumer("consumer" + (i + 1), queues.get(i % queues.size()));
            consumers.add(consumer);
        }
    }

    private void reportRates() throws InterruptedException {
        System.out.println("Checking rates for test: " + super.getName());
        for (int i = 0; i < PERFORMANCE_SAMPLES; i++) {
            Period p = new Period();
            Thread.sleep(5000);
            System.out.println(totalProducerRate.getRateSummary(p));
            System.out.println(totalConsumerRate.getRateSummary(p));
            /*
             * if (includeDetailedRates) {
             * System.out.println(totalProducerRate.getChildRateSummary(p));
             * System.out.println(totalConsumerRate.getChildRateSummary(p)); }
             */
            totalProducerRate.reset();
            totalConsumerRate.reset();
        }
    }

    class Producer implements FlowUnblockListener<OpenWireMessageDelivery> {
        private AtomicBoolean stopped = new AtomicBoolean(false);
        private String name;
        protected final MetricCounter sendRate = new MetricCounter();
		AtomicBoolean waitingForAck = new AtomicBoolean();
        private final DispatchQueue dispatchQueue;
        private final Runnable dispatchTask;

        protected IFlowController<OpenWireMessageDelivery> outboundController;
        protected final AbstractFlowRelay<OpenWireMessageDelivery> outboundQueue;
        protected OpenWireMessageDelivery next;
        private int priority;
        private final String payload;
        private int sequenceNumber;
        private final ActiveMQDestination destination;
        private final IQueue<Long, MessageDelivery> targetQueue;

        private final ProducerId producerId;
        private final OpenWireFormat wireFormat;

        public Producer(String name, IQueue<Long, MessageDelivery> targetQueue) {
            this.name = name;
            sendRate.name("Producer " + name + " Rate");
            totalProducerRate.add(sendRate);
            
            dispatchQueue = Dispatch.createQueue(name);
            dispatchTask = new Runnable(){
                public void run() {
                    dispatch();
                }
            };
 
            // create a 1024 byte payload (2 bytes per char):
            payload = new String(new byte[512]);
            producerId = new ProducerId(name);
            wireFormat = new OpenWireFormat();
            wireFormat.setCacheEnabled(false);
            wireFormat.setSizePrefixDisabled(false);
            wireFormat.setVersion(OpenWireFormat.DEFAULT_VERSION);

            SizeLimiter<OpenWireMessageDelivery> limiter = new SizeLimiter<OpenWireMessageDelivery>(1000 * 1024, 500 * 1024) {
                @Override
                public int getElementSize(OpenWireMessageDelivery elem) {
                    return elem.getFlowLimiterSize();
                }
            };

            Flow flow = new Flow(name, true);
            outboundQueue = new SingleFlowRelay<OpenWireMessageDelivery>(flow, name, limiter);
            outboundQueue.setFlowExecutor(Dispatch.getGlobalQueue());
            outboundQueue.setDrain(new QueueDispatchTarget<OpenWireMessageDelivery>() {

                public void drain(OpenWireMessageDelivery elem, ISourceController<OpenWireMessageDelivery> controller) {

                    next.setStoreWireFormat(wireFormat);
                    next.beginDispatch(database);
                    Producer.this.targetQueue.add(elem, controller);
                    // Saves the message to the database:
                    try {
                        elem.finishDispatch(controller);
                    } catch (IOException e) {
                        e.printStackTrace();
                        stop();
                    } finally {
                        controller.elementDispatched(elem);
                    }
                }
            });
            outboundController = outboundQueue.getFlowController(flow);
            this.targetQueue = targetQueue;
            this.destination = new ActiveMQQueue(targetQueue.getResourceName());
        }

        public void start() {
            dispatchQueue.dispatchAsync(dispatchTask);
        }

        public void stop() {
            stopped.set(true);
        }

        public void dispatch() {
            // If flow controlled stop until flow control is lifted.
            if (outboundController.isSinkBlocked()) {
                if (outboundController.addUnblockListener(this)) {
                    return;
                }
            }

            if( TEST_MAX_STORE_LATENCY ) {
            	// We can't send again until we get persist ack.
            	if( waitingForAck.get() ) {
                    return;
            	}
            }
            
            if (next == null) {
                try {
                	next = createNextMessage();
					if (TEST_MAX_STORE_LATENCY) {
						waitingForAck.set(true);
						next.setPersistListener(new PersistListener() {
							public void onMessagePersisted(OpenWireMessageDelivery delivery) {
								waitingForAck.set(false);
					            dispatchQueue.dispatchAsync(dispatchTask);
							}
						});
					}
                } catch (JMSException e) {
                    e.printStackTrace();
                    stopped.set(true);
                    return;
                }
            }

            sendRate.increment();
            outboundQueue.add(next, null);
            next = null;
            if ( !stopped.get() ) {
                dispatchQueue.dispatchAsync(dispatchTask);
            }
        }

        private OpenWireMessageDelivery createNextMessage() throws JMSException {
            ActiveMQTextMessage message = new ActiveMQTextMessage();
            message.setJMSPriority(priority);
            message.setProducerId(producerId);
            message.setMessageId(new MessageId(name, ++sequenceNumber));
            message.setDestination(destination);
            message.setPersistent(PERSISTENT);
            if (payload != null) {
                message.setText(payload);
            }
            return new OpenWireMessageDelivery(message);
        }

        public void onFlowUnblocked(ISinkController<OpenWireMessageDelivery> controller) {
            dispatchQueue.dispatchAsync(dispatchTask);
        }

        public String toString() {
            return name + " on " + targetQueue.getResourceName();
        }
    }

    class Consumer extends AbstractLimitedFlowResource<MessageDelivery> implements Subscription<MessageDelivery>, IFlowSink<MessageDelivery> {
        private AtomicBoolean stopped = new AtomicBoolean(true);
        protected final MetricCounter rate = new MetricCounter();
        private final String name;
        private final SizeLimiter<MessageDelivery> limiter;
        private final FlowController<MessageDelivery> controller;
        private final IQueue<Long, MessageDelivery> sourceQueue;

        public Consumer(String name, IQueue<Long, MessageDelivery> sourceQueue) {
            this.sourceQueue = sourceQueue;
            this.name = name;
            Flow flow = new Flow(name + "-outbound", false);
            limiter = new SizeLimiter<MessageDelivery>(1024 * 1024, 512 * 1024) {
                @Override
                public int getElementSize(MessageDelivery m) {
                    return m.getFlowLimiterSize();
                }
            };

            controller = new FlowController<MessageDelivery>(null, flow, limiter, this);
            controller.useOverFlowQueue(false);
            controller.setExecutor(Dispatch.getGlobalQueue());

            rate.name("Consumer " + name + " Rate");
            totalConsumerRate.add(rate);
        }

        public void start() {
            stopped.set(false);
            subscribe(sourceQueue);
        }

        private void subscribe(IQueue<Long, MessageDelivery> source) {
            source.addSubscription(this);
        }

        public void stop() throws InterruptedException {
            sourceQueue.removeSubscription(this);
            stopped.set(true);
        }

        public String toString() {
            return name + " on " + sourceQueue.getResourceName();
        }

        public boolean isExclusive() {
            return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.queue.Subscription#add(java.lang.Object,
         * org.apache.activemq.flow.ISourceController,
         * org.apache.activemq.queue.Subscription.SubscriptionDeliveryCallback)
         */
        public void add(MessageDelivery element, ISourceController<?> source, SubscriptionDelivery<MessageDelivery> callback) {
            controller.add(element, source);
            addInternal(element, source, callback);
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.queue.Subscription#offer(java.lang.Object,
         * org.apache.activemq.flow.ISourceController,
         * org.apache.activemq.queue.Subscription.SubscriptionDeliveryCallback)
         */
        public boolean offer(MessageDelivery element, ISourceController<?> source, SubscriptionDelivery<MessageDelivery> callback) {
            if (controller.offer(element, source)) {
                addInternal(element, source, callback);
                return true;
            }
            return false;
        }

        /**
         * @param element
         * @param source
         * @param callback
         */
        private void addInternal(MessageDelivery element, ISourceController<?> source, SubscriptionDelivery<MessageDelivery> callback) {
            rate.increment();
            synchronized (this) {
                controller.elementDispatched(element);
            }
            callback.acknowledge();
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.queue.Subscription#hasSelector()
         */
        public boolean hasSelector() {
            return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.queue.Subscription#isBrowser()
         */
        public boolean isBrowser() {
            return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.queue.Subscription#isRemoveOnDispatch(java.lang
         * .Object)
         */
        public boolean isRemoveOnDispatch(MessageDelivery elem) {
            return false;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.queue.Subscription#matches(java.lang.Object)
         */
        public boolean matches(MessageDelivery elem) {
            return true;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.flow.IFlowSink#add(java.lang.Object,
         * org.apache.activemq.flow.ISourceController)
         */
        public void add(MessageDelivery elem, ISourceController<?> source) {
            add(elem, source, null);
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.flow.IFlowSink#offer(java.lang.Object,
         * org.apache.activemq.flow.ISourceController)
         */
        public boolean offer(MessageDelivery elem, ISourceController<?> source) {
            return offer(elem, source, null);
        }
    }
}
