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
package org.apache.activemq.broker.store;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.broker.store.Store.QueueRecord;
import org.apache.activemq.broker.store.Store.Session;
import org.apache.activemq.broker.store.Store.VoidCallback;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.MetricCounter;
import org.apache.activemq.metric.Period;
import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;

public abstract class StorePerformanceBase extends TestCase {
    
    private static int PERFORMANCE_SAMPLES = 50;
    private static boolean SYNC_TO_DISK = true;
    private static final boolean USE_SHARED_WRITER = true;

    private Store store;
    private QueueDescriptor queueId;
    private AtomicLong queueKey = new AtomicLong(0);

    protected MetricAggregator totalProducerRate = new MetricAggregator().name("Aggregate Producer Rate").unit("items");
    protected MetricAggregator totalConsumerRate = new MetricAggregator().name("Aggregate Consumer Rate").unit("items");

    protected ArrayList<Consumer> consumers = new ArrayList<Consumer>();
    protected ArrayList<Producer> producers = new ArrayList<Producer>();

    abstract protected Store createStore();

    private SharedWriter writer = null;
    
    private Semaphore enqueuePermits;
    private Semaphore dequeuePermits;

    @Override
    protected void setUp() throws Exception {
        store = createStore();
        //store.setDeleteAllMessages(false);
        store.start();

        if (USE_SHARED_WRITER) {
            writer = new SharedWriter();
            writer.start();
        }
        
        enqueuePermits = new Semaphore(20000000);
        dequeuePermits = new Semaphore(0);
        
        queueId = new QueueDescriptor();
        queueId.setQueueName(new AsciiBuffer("test"));
        store.execute(new VoidCallback<Exception>() {
            @Override
            public void run(Session session) throws Exception {
                session.queueAdd(queueId);
            }
        }, null);
        
        store.execute(new VoidCallback<Exception>() {
            @Override
            public void run(Session session) throws Exception {
                Iterator<Store.QueueQueryResult> qqrs = session.queueList(queueId, 1);
                assertTrue(qqrs.hasNext());
                Store.QueueQueryResult qqr = qqrs.next();
                if(qqr.getSize() > 0)
                {
                    queueKey.set(qqr.getLastSequence() + 1);
                    System.out.println("Recovered queue: " + qqr.getDescriptor().getQueueName() + " with " + qqr.getCount() + " messages");
                }                   
            }
        }, null);
    }

    @Override
    protected void tearDown() throws Exception {
        for (Consumer c : consumers) {
            c.stop();
        }
        consumers.clear();
        for (Producer p : producers) {
            p.stop();
        }
        producers.clear();

        if (writer != null) {
            writer.stop();
        }

        if (store != null) {
            store.stop();
        }
    }

    class SharedWriter implements Runnable {
        LinkedBlockingQueue<SharedQueueOp> queue = new LinkedBlockingQueue<SharedQueueOp>(1000);
        private Thread thread;
        private AtomicBoolean stopped = new AtomicBoolean();

        public void start() {
            thread = new Thread(this, "Writer");
            thread.start();
        }

        public void stop() throws InterruptedException {
            stopped.set(true);

            //Add an op to trigger shutdown:
            SharedQueueOp op = new SharedQueueOp() {
                public void run() {
                }
            };
            op.op = new Store.VoidCallback<Exception>() {

                @Override
                public void run(Session session) throws Exception {
                    // TODO Auto-generated method stub
                }
            };

            queue.put(op);
            thread.join();
        }

        public void run() {
            Session session = store.getSession();
            try {
                LinkedList<Runnable> processed = new LinkedList<Runnable>();
                while (!stopped.get()) {
                    SharedQueueOp op = queue.take();
                    session.acquireLock();
                    int ops = 0;
                    while (op != null && ops < 1000) {
                        op.op.execute(session);
                        processed.add(op);
                        op = queue.poll();
                        ops++;
                    }

                    session.commit();
                    session.releaseLock();

                    if (SYNC_TO_DISK) {
                        store.flush();
                    }

                    for (Runnable r : processed) {
                        r.run();
                    }
                    processed.clear();
                }

            } catch (InterruptedException e) {
                if (!stopped.get()) {
                    e.printStackTrace();
                }
                return;
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
        }

        public void addOp(SharedQueueOp op) throws InterruptedException {
            queue.put(op);
        }
    }

    abstract class SharedQueueOp implements Runnable {
        VoidCallback<Exception> op;
    }

    class Producer implements Runnable {
        private Thread thread;
        private AtomicBoolean stopped = new AtomicBoolean();
        private String name;
        protected final MetricCounter rate = new MetricCounter();
        private long sleep;

        public Producer(String name) {
            this.name = name;
        }

        public void start() {
            rate.name("Producer " + name + " Rate");
            totalProducerRate.add(rate);
            thread = new Thread(this, "Producer" + name);
            thread.start();
        }

        public void stop() throws InterruptedException {
            stopped.set(true);
            while (enqueuePermits.hasQueuedThreads()) {
                enqueuePermits.release();
            }
            thread.join();
        }

        public void run() {
            try {
                Buffer buffer = new Buffer(new byte[1024]);
                for (long i = 0; !stopped.get(); i++) {

                    enqueuePermits.acquire();

                    final MessageRecord messageRecord = new MessageRecord();
                    messageRecord.setKey(store.allocateStoreTracking());
                    messageRecord.setMessageId(new AsciiBuffer("" + i));
                    messageRecord.setEncoding(new AsciiBuffer("encoding"));
                    messageRecord.setBuffer(buffer);
                    messageRecord.setSize(buffer.getLength());

                    SharedQueueOp op = new SharedQueueOp() {
                        public void run() {
                            rate.increment();
                        }
                    };

                    op.op = new VoidCallback<Exception>() {
                        @Override
                        public void run(Session session) throws Exception {
                            session.messageAdd(messageRecord);
                            QueueRecord queueRecord = new Store.QueueRecord();
                            queueRecord.setMessageKey(messageRecord.getKey());
                            queueRecord.setQueueKey(queueKey.incrementAndGet());
                            queueRecord.setSize(messageRecord.getSize());
                            session.queueAddMessage(queueId, queueRecord);
                            dequeuePermits.release();
                        }
                    };

                    if (!USE_SHARED_WRITER) {
                        store.execute(op.op, op);

                        if (SYNC_TO_DISK) {
                            store.flush();
                        }

                    } else {
                        writer.addOp(op);
                    }

                    if (sleep > 0) {
                        Thread.sleep(sleep);
                    }
                }
            } catch (InterruptedException e) {
                if (!stopped.get()) {
                    e.printStackTrace();
                }
                return;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    class Consumer implements Runnable {
        private Thread thread;
        private AtomicBoolean stopped = new AtomicBoolean();
        protected final MetricCounter rate = new MetricCounter();
        private String name;
        private final Semaphore queryWait = new Semaphore(0);

        public Consumer(String name) {
            this.name = name;
        }

        public void start() {
            rate.name("Consumer " + name + " Rate");
            totalConsumerRate.add(rate);
            thread = new Thread(this, "Consumer " + name);
            thread.start();
        }

        public void stop() throws InterruptedException {
            stopped.set(true);
            queryWait.release();
            thread.join();
        }

        public void run() {
            try {
                while (!stopped.get()) {
                    final ArrayList<MessageRecord> records = new ArrayList<MessageRecord>(1000);
                    SharedQueueOp op = new SharedQueueOp() {
                        public void run() {
                            rate.increment(records.size());
                            enqueuePermits.release(records.size());
                            queryWait.release();
                        }
                    };

                    op.op = new VoidCallback<Exception>() {
                        @Override
                        public void run(Session session) throws Exception {
                            Iterator<QueueRecord> queueRecords = session.queueListMessagesQueue(queueId, 0L, -1L, 1000);
                            for (Iterator<QueueRecord> iterator = queueRecords; iterator.hasNext();) {
                                QueueRecord r = iterator.next();
                                records.add(session.messageGetRecord(r.getMessageKey()));
                                session.queueRemoveMessage(queueId, r.queueKey);
                            }
                        }
                    };

                    if (!USE_SHARED_WRITER) {
                        store.execute(op.op, op);
                        if (SYNC_TO_DISK) {
                            store.flush();
                        }
                    } else {
                        writer.addOp(op);
                    }

                    dequeuePermits.acquire();
                    records.clear();
                }
            } catch (InterruptedException e) {
                if (!stopped.get()) {
                    e.printStackTrace();
                }
                return;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void test1_1_0() throws Exception {
        startProducers(1);
        reportRates();
    }
    
    
    public void test1_1_1() throws Exception {
        startProducers(1);
        startConsumers(1);
        reportRates();
    }

    public void test10_1_1() throws Exception {
        startProducers(10);
        startConsumers(1);
        reportRates();
    }

    private void startProducers(int count) {
        for (int i = 0; i < count; i++) {
            Producer p = new Producer("" + (i + 1));
            producers.add(p);
            p.start();
        }
    }

    private void startConsumers(int count) {
        for (int i = 0; i < count; i++) {
            Consumer c = new Consumer("" + (i + 1));
            consumers.add(c);
            c.start();
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

}
