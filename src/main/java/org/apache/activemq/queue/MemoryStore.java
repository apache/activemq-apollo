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
package org.apache.activemq.queue;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kahadb.util.LinkedNode;
import org.apache.kahadb.util.LinkedNodeList;

public class MemoryStore<K, V> implements Store<K, V> {

    AtomicLong counter = new AtomicLong();

    class MemoryStoreNode extends LinkedNode<MemoryStoreNode> implements StoreNode<K, V> {
        private Subscription<V> owner;
        private final K key;
        private final V value;
        private long id = counter.getAndIncrement();

        public MemoryStoreNode(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public boolean acquire(Subscription<V> owner) {
            if (this.owner == null) {
                this.owner = owner;
            }
            return true;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "node:" + id;
        }

        public void unacquire() {
            this.owner = null;
        }

    }

    class MemoryStoreCursor implements StoreCursor<K, V> {
        private MemoryStoreNode last;
        private MemoryStoreNode next;

        public MemoryStoreCursor() {
        }

        public MemoryStoreCursor(MemoryStoreNode next) {
            this.next = next;
        }

        public void setNext(StoreNode<K, V> next) {
            this.next = (MemoryStoreNode) next;
        }

        public boolean hasNext() {
            if (next != null)
                return true;

            if (last == null || last.getNextCircular() == last) {
                next = (MemoryStoreNode) elements.getHead();
                return next != null;
            }

            while (true) {
                MemoryStoreNode t = last.getNextCircular();
                if (t.id > last.id) {
                    next = t;
                    return true;
                } else {
                    return false;
                }
            }
        }

        public StoreNode<K, V> peekNext() {
            hasNext();
            return next;
        }

        public StoreNode<K, V> next() {
            try {
                hasNext();
                return next;
            } finally {
                last = next;
                next = null;
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    protected HashMap<K, MemoryStoreNode> map = new HashMap<K, MemoryStoreNode>();
    protected LinkedNodeList<MemoryStoreNode> elements = new LinkedNodeList<MemoryStoreNode>();

    public StoreNode<K, V> add(K key, V value) {
        MemoryStoreNode rc = new MemoryStoreNode(key, value);
        map.put(key, rc);
        elements.addLast(rc);
        return rc;
    }

    public StoreNode<K, V> remove(K key) {
        MemoryStoreNode node = (MemoryStoreNode) map.remove(key);
        if (node != null) {
            node.unlink();
        }
        return node;
    }

    public boolean isEmpty() {
        return elements.isEmpty();
    }

    public org.apache.activemq.queue.Store.StoreCursor<K, V> openCursor() {
        MemoryStoreCursor cursor = new MemoryStoreCursor();
        return cursor;
    }

    public org.apache.activemq.queue.Store.StoreCursor<K, V> openCursorAt(org.apache.activemq.queue.Store.StoreNode<K, V> next) {
        MemoryStoreCursor cursor = new MemoryStoreCursor((MemoryStoreNode) next);
        return cursor;
    }

    public int size() {
        return map.size();
    }

}
