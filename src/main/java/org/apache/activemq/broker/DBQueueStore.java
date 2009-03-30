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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.store.BrokerDatabase;
import org.apache.activemq.broker.store.BrokerDatabase.MessageRestoreListener;
import org.apache.activemq.broker.store.BrokerDatabase.RestoredMessage;
import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.dispatch.IDispatcher.DispatchContext;
import org.apache.activemq.dispatch.IDispatcher.Dispatchable;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.queue.Mapper;
import org.apache.activemq.queue.Store;
import org.apache.activemq.queue.Subscription;

public class DBQueueStore<K> implements Store<K, MessageDelivery> {

    private final BrokerDatabase database;
    private final AsciiBuffer queue;
    private final MessageRetriever retriever;

    private long firstKey = -1;
    private long lastKey = -1;

    private int count = 0;
    private boolean loading = true;

    protected HashMap<K, DBStoreNode> map = new HashMap<K, DBStoreNode>();
    protected TreeMap<Long, DBStoreNode> order = new TreeMap<Long, DBStoreNode>();
    private Mapper<K, MessageDelivery> keyExtractor;

    DBQueueStore(BrokerDatabase database, AsciiBuffer queue, IDispatcher dispatcher) {
        this.database = database;
        this.queue = queue;
        retriever = new MessageRetriever(dispatcher);
        retriever.start();
    }

    public StoreNode<K, MessageDelivery> add(K key, MessageDelivery delivery) {

        // New to this queue?
        if (delivery.getStoreTracking() > lastKey) {
            return addInternal(key, delivery);
        } else {
            throw new IllegalArgumentException(this + " Duplicate key: " + delivery);
        }
    }

    public void setKeyMapper(Mapper<K, MessageDelivery> keyExtractor) {
        this.keyExtractor = keyExtractor;
    }
    
    private DBStoreNode addInternal(K key, MessageDelivery delivery) {
        DBStoreNode node = new DBStoreNode(delivery);
        map.put(keyExtractor.map(delivery), node);
        order.put(delivery.getStoreTracking(), node);
        return node;
    }

    public boolean isEmpty() {
        return count == 0;
    }

    public StoreCursor<K, MessageDelivery> openCursor() {
        return new DBStoreCursor();
    }

    public StoreCursor<K, MessageDelivery> openCursorAt(StoreNode<K, MessageDelivery> next) {
        DBStoreCursor cursor = new DBStoreCursor();
        cursor.next = (DBStoreNode) next;
        return cursor;
    }

    public StoreNode<K, MessageDelivery> remove(K key) {
        // TODO Auto-generated method stub
        return null;
    }

    public int size() {
        return count;
    }

    private class DBStoreCursor implements StoreCursor<K, MessageDelivery> {
        private long pos;
        private long last = -1;
        
        private DBStoreNode node;
        private DBStoreNode next;

        public StoreNode<K, MessageDelivery> peekNext() {
            // TODO Auto-generated method stub
            return null;
        }

        public void setNext(StoreNode<K, MessageDelivery> node) {
            this.next = (DBStoreNode) next;

        }

        public boolean hasNext() {
            if (next != null)
                return true;

            SortedMap<Long, DBStoreNode> m = order.tailMap(last + 1);
            if (m.isEmpty()) {
                next = null;
            } else {
                next = m.get(m.firstKey());
            }
            return next != null;
        }

        public StoreNode<K, MessageDelivery> next() {
            try {
                hasNext();
                return next;
            } finally {
                last = next.tracking;
                next = null;
            }
        }

        public boolean isReady() {
            return !loading;
        }
        
        public void remove() {
            database.deleteMessage(node.delivery, queue);
        }
    }

    private class DBStoreNode implements StoreNode<K, MessageDelivery> {
        private MessageDelivery delivery;
        private K key;
        private long ownerId = -1;
        private final long tracking;

        DBStoreNode(MessageDelivery delivery) {
            this.delivery = delivery;
            tracking = delivery.getStoreTracking();
            key = keyExtractor.map(delivery);
            retriever.save(this);
        }

        public boolean acquire(Subscription<MessageDelivery> owner) {
            long id = owner.getSink().getResourceId();
            // TODO Auto-generated method stub
            if (ownerId == -1 || id == ownerId) {
                ownerId = owner.getSink().getResourceId();
                return true;
            }
            return false;
        }

        public K getKey() {
            return key;
        }

        public MessageDelivery getValue() {
            return delivery;
        }

        public void unacquire() {
            ownerId = -1;
        }
    }

    private class MessageRetriever implements Dispatchable, MessageRestoreListener {

        private final DispatchContext dispatchContext;
        private AtomicBoolean loaded = new AtomicBoolean(false);

        private long loadCursor = 0;
        private long max = -1;
        private long loadedCount;
        
        private final ConcurrentLinkedQueue<RestoredMessage> restoredMsgs = new ConcurrentLinkedQueue<RestoredMessage>();

        MessageRetriever(IDispatcher dispatcher) {
            dispatchContext = dispatcher.register(this, "MessageRetriever-" + queue);
        }

        public void save(DBStoreNode node) {
            try {
                node.delivery.persist(queue, false);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        public void start() {
            if (!loaded.get()) {
                database.restoreMessages(queue, loadCursor, 50, this);
            }
        }

        public boolean dispatch() {
            while (true) {
                RestoredMessage restored = restoredMsgs.poll();

                if (restored == null) {
                    break;
                }

                try {
                    MessageDelivery delivery = restored.getMessageDelivery();
                    addInternal(keyExtractor.map(delivery), delivery);
                    if (firstKey == -1) {
                        firstKey = delivery.getStoreTracking();
                    }
                    if (lastKey < delivery.getStoreTracking()) {
                        lastKey = delivery.getStoreTracking();
                    }
                    loadedCount++;

                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            if (!loaded.get()) {
                database.restoreMessages(queue, loadCursor, 50, this);
            }
            return false;
        }

        public void messagesRestored(Collection<RestoredMessage> msgs) {
            if (!msgs.isEmpty()) {
                restoredMsgs.addAll(msgs);
            } else {
                loaded.set(true);
            }
            dispatchContext.requestDispatch();
        }
    }    
}
