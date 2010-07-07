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
package org.apache.activemq.apollo.broker;

import junit.framework.TestCase;

/**
 * @author cmacnaug
 * 
 */
public class SharedQueueTest extends TestCase {

// TODO:
//    DispatchQueue dispatchQueue;
//    BrokerDatabase database;
//    BrokerQueueStore queueStore;
//    private static final boolean USE_KAHA_DB = true;
//    private static final boolean PERSISTENT = true;
//    private static final boolean PURGE_STORE = false;
//
//    protected ArrayList<IQueue<Long, MessageDelivery>> queues = new ArrayList<IQueue<Long, MessageDelivery>>();
//
//    protected DispatchQueue createDispatcher() {
//        return Dispatch.createQueue();
//    }
//
//    protected int consumerStartDelay = 0;
//
//    @Override
//    protected void setUp() throws Exception {
//    	startServices();
//    }
//
//    @Override
//    protected void tearDown() throws Exception {
//    	stopServices();
//    }
//
//    protected void startServices() throws Exception {
//        dispatchQueue = createDispatcher();
//        dispatchQueue.resume();
//        database = new BrokerDatabase(createStore());
//        database.setDispatchQueue(dispatchQueue);
//        database.start();
//        queueStore = new BrokerQueueStore();
//        queueStore.setDatabase(database);
//        queueStore.loadQueues();
//    }
//
//    protected void stopServices() throws Exception {
//        database.stop();
//        dispatchQueue.release();
//        queues.clear();
//    }
//
//    protected Store createStore() throws Exception {
//        Store store = null;
//        if (USE_KAHA_DB) {
//            store = StoreFactory.createStore("hawtdb");
//        } else {
//            store = StoreFactory.createStore("memory");
//        }
//
//        store.setStoreDirectory(new File("target/test-data/shared-queue-test/"));
//        store.setDeleteAllMessages(PURGE_STORE);
//        return store;
//    }
//
//    private final void createQueues(int count) {
//        for (int i = 0; i < count; i++) {
//            IQueue<Long, MessageDelivery> queue = queueStore.createSharedQueue("queue-" + (i + 1));
//            queues.add(queue);
//        }
//    }
//
//    protected void cleanup() throws Exception {
//        queues.clear();
//        stopServices();
//    }
//
//    public void testExpiration() {
//        createQueues(1);
//        IQueue<Long, MessageDelivery> queue = queues.get(0);
//    }

}
