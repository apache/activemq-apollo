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
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.broker.store.Store.QueueRecord;
import org.apache.activemq.broker.store.Store.Session;
import org.apache.activemq.broker.store.Store.VoidCallback;
import org.apache.activemq.metric.MetricAggregator;
import org.apache.activemq.metric.MetricCounter;
import org.apache.activemq.metric.Period;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;

public abstract class StorePerformanceBase extends TestCase {

    private static int PERFORMANCE_SAMPLES = 3;
    private static boolean SYNC_TO_DISK = true;
    
    
    private Store store;
    private AsciiBuffer queueName;

    protected MetricAggregator totalProducerRate = new MetricAggregator().name("Aggregate Producer Rate").unit("items");
    protected MetricAggregator totalConsumerRate = new MetricAggregator().name("Aggregate Consumer Rate").unit("items");

    protected ArrayList<Consumer> consumers = new ArrayList<Consumer>();
    protected ArrayList<Producer> producers = new ArrayList<Producer>();

    abstract protected Store createStore();

    @Override
    protected void setUp() throws Exception {
        store = createStore();
        store.start();
        
        queueName = new AsciiBuffer("test");
        store.execute(new VoidCallback<Exception>() {
            @Override
            public void run(Session session) throws Exception {
                session.queueAdd(queueName);
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
        
        if (store != null) {
            store.stop();
        }
    }

    private final Object wakeupMutex = new Object(); 
    
    class Producer implements Runnable {
        private Thread thread;
        private AtomicBoolean stopped = new AtomicBoolean();
        private String name;
        protected final MetricCounter rate = new MetricCounter();
        private long sleep;

        public Producer(String name) {
            this.name=name;
        }
        public void start() {
            rate.name("Producer " + name + " Rate");
            totalProducerRate.add(rate);
            thread = new Thread(this, "Producer"+ name);
            thread.start();
        }
        public void stop() throws InterruptedException {
            stopped.set(true);
            thread.join();
        }
        public void run() {
            try {
                Buffer buffer = new Buffer(new byte[1024]);
                for( long i=0; !stopped.get(); i++ ) {
                    
                    final MessageRecord messageRecord = new MessageRecord();
                    messageRecord.setKey(store.allocateStoreTracking());
                    messageRecord.setMessageId(new AsciiBuffer(""+i));
                    messageRecord.setEncoding(new AsciiBuffer("encoding"));
                    messageRecord.setBuffer(buffer);

                    Runnable onFlush = new Runnable(){
                        public void run() {
                            rate.increment();
                            synchronized(wakeupMutex){
                                wakeupMutex.notify();
                            }
                        }
                    };
                    store.execute(new VoidCallback<Exception>() {
                        @Override
                        public void run(Session session) throws Exception {
                            Long messageKey = session.messageAdd(messageRecord);
                            QueueRecord queueRecord = new Store.QueueRecord();
                            queueRecord.setMessageKey(messageKey);
                            session.queueAddMessage(queueName, queueRecord);
                        }
                    }, onFlush);
                    
                    if( SYNC_TO_DISK ) {
                        store.flush();
                    }

                    
                    if( sleep>0 ) {
                        Thread.sleep(sleep);
                    }
                }
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

        public Consumer(String name) {
            this.name=name;
        }
        public void start() {
            rate.name("Consumer " + name + " Rate");
            totalConsumerRate.add(rate);
            thread = new Thread(this, "Consumer " + name );
            thread.start();
        }
        public void stop() throws InterruptedException {
            stopped.set(true);
            thread.join();
        }
        
        public void run() {
            try {
                while( !stopped.get() ) {
                    final ArrayList<MessageRecord> records = new ArrayList<MessageRecord>(1000);;
                    Runnable onFlush = new Runnable(){
                        public void run() {
                            rate.increment(records.size());
                            if( records.isEmpty() ) {
                                synchronized(wakeupMutex){
                                    try {
                                        wakeupMutex.wait(500);
                                    } catch (InterruptedException e) {
                                    }
                                }
                            }
                        }
                    };
                    store.execute(new VoidCallback<Exception>() {
                        @Override
                        public void run(Session session) throws Exception {
                            Iterator<QueueRecord> queueRecords = session.queueListMessagesQueue(queueName, null, 1000);
                            for (Iterator<QueueRecord> iterator = queueRecords; iterator.hasNext();) {
                                QueueRecord r = iterator.next();
                                records.add(session.messageGetRecord(r.getMessageKey()));
                                session.queueRemoveMessage(queueName, r.messageKey);
                            }
                        }
                    }, onFlush);
                    if( SYNC_TO_DISK ) {
                        store.flush();
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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
            Producer p = new  Producer(""+(i+1));
            producers.add(p);
            p.start();
        }
    }
    
    private void startConsumers(int count) {
        for (int i = 0; i < count; i++) {
            Consumer c = new  Consumer(""+(i+1));
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
