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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.IFlowSizeLimiter;
import org.apache.activemq.flow.ISinkController;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.util.Mapper;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.apache.activemq.util.list.LinkedNode;
import org.apache.activemq.util.list.LinkedNodeList;

/**
 * IQueue which does direct dispatch whenever it can.
 */
public class SharedQueueOld<K, V> extends AbstractFlowQueue<V> implements IQueue<K, V> {

    protected TreeMemoryStore store = new TreeMemoryStore();

    private final LinkedNodeList<SubscriptionNode> unreadyDirectSubs = new LinkedNodeList<SubscriptionNode>();
    private final LinkedNodeList<SubscriptionNode> readyDirectSubs = new LinkedNodeList<SubscriptionNode>();

    private final LinkedNodeList<SubscriptionNode> unreadyPollingSubs = new LinkedNodeList<SubscriptionNode>();
    private final LinkedNodeList<SubscriptionNode> readyPollingSubs = new LinkedNodeList<SubscriptionNode>();

    private final HashMap<Subscription<V>, SubscriptionNode> subscriptions = new HashMap<Subscription<V>, SubscriptionNode>();
    //private final HashMap<IFlowResource, SubscriptionNode> sinks = new HashMap<IFlowResource, SubscriptionNode>();

    private final FlowController<V> sinkController;
    private final IFlowSizeLimiter<V> limiter;
    private final Object mutex;

    protected Mapper<K, V> keyMapper;
    private long directs;

    private QueueDescriptor queueDescriptor;

    public SharedQueueOld(String name, IFlowSizeLimiter<V> limiter) {
        this(name, limiter, new Object());
        autoRelease = true;
    }

    /**
     * Creates a flow queue that can handle multiple flows.
     * 
     * @param flow
     *            The {@link Flow}
     * @param controller
     *            The FlowController if this queue is flow controlled:
     */
    public SharedQueueOld(String name, IFlowSizeLimiter<V> limiter, Object mutex) {
        super(name);
        queueDescriptor = new QueueDescriptor();
        queueDescriptor.setQueueName(new AsciiBuffer(super.getResourceName()));
        this.mutex = mutex;
        Flow flow = new Flow(name, false);
        this.limiter = limiter;
        this.sinkController = new FlowController<V>(getFlowControllableHook(), flow, limiter, mutex);
        super.onFlowOpened(sinkController);
    }

    public int getEnqueuedCount() {
        synchronized (mutex) {
            return store.size();
        }
    }

    public long getEnqueuedSize() {
        synchronized (mutex) {
            return limiter.getSize();
        }
    }

    public void initialize(long sequenceMin, long sequenceMax, int count, long size) {
        // this queue is not persistent, so we can ignore this.
    }

    public void setStore(QueueStore<K, V> store) {
        // No-op
    }

    public void setPersistencePolicy(PersistencePolicy<V> persistencePolicy) {
        // this queue is not persistent, so we can ignore this.
    }

    
    /* (non-Javadoc)
     * @see org.apache.activemq.queue.IQueue#setExpirationMapper(org.apache.activemq.util.Mapper)
     */
    public void setExpirationMapper(Mapper<Long, V> expirationMapper) {
        //not implemented.
    }

    public void add(V elem, ISourceController<?> source) {
        sinkController.add(elem, source);
    }

    public boolean offer(V elem, ISourceController<?> source) {
        return sinkController.offer(elem, source);
    }
    

    public void remove(long key) {
        throw new UnsupportedOperationException();
    }
    
    /**
     * Called when the controller accepts a message for this queue.
     */
    public void flowElemAccepted(ISourceController<V> controller, V value) {
        synchronized (mutex) {

            // Try to directly dispatch to one of the attached subscriptions
            // sourceDispatch returns null on successful dispatch
            ArrayList<SubscriptionNode> matches = directDispatch(value);
            if (matches != null) {

                if (directs != 0) {
                    // System.out.println("could not directly dispatch.. had directly dispatched: "+directs);
                    directs = 0;
                }

                K key = keyMapper.map(value);
                StoreNode<K, V> node = store.add(key, value);

                int matchCount = 0;
                // Go through the un-ready direct subs and find out if any those
                // would
                // have matched the message, and if so then set it up to cursor
                // from
                // it.
                SubscriptionNode sub = unreadyDirectSubs.getHead();
                while (sub != null) {
                    SubscriptionNode next = sub.getNext();
                    if (sub.subscription.matches(value)) {
                        sub.unlink();
                        sub.resumeAt(node);
                        unreadyPollingSubs.addLast(sub);
                        matchCount++;
                        // System.out.println("Subscription state change: un-ready direct -> un-ready polling: "+sub);
                    }
                    sub = next;
                }

                // Also do it for all the ready nodes that matched... but which
                // we
                // could not enqueue to.
                for (SubscriptionNode subNode : matches) {
                    subNode.unlink();
                    subNode.resumeAt(node);
                    unreadyPollingSubs.addLast(subNode);
                    // System.out.println("Subscription state change: ready direct -> un-ready polling: "+subNode);
                }
                matchCount += matches.size();

                if (matchCount > 0) {
                    // We have interested subscriptions for the message.. but
                    // they are not ready to receive.
                    // Would be cool if we could flow control the source.
                }

                if (!readyPollingSubs.isEmpty()) {
                    notifyReady();
                }
            } else {
                directs++;
            }
        }
    }

    public FlowController<V> getFlowController(Flow flow) {
        return sinkController;
    }

    public boolean isDispatchReady() {
        return started && !store.isEmpty() && !readyPollingSubs.isEmpty();
    }

    private ArrayList<SubscriptionNode> directDispatch(V elem) {
        ArrayList<SubscriptionNode> matches = new ArrayList<SubscriptionNode>(readyDirectSubs.size());
        boolean accepted = false;
        SubscriptionNode next = null;
        SubscriptionNode node = readyDirectSubs.getHead();
        while (node != null) {
            next = node.getNext();
            if (node.subscription.matches(elem)) {
                accepted = node.subscription.offer(elem, node, null);
                if (accepted) {
                    if (autoRelease) {
                        sinkController.elementDispatched(elem);
                    }
                    break;
                } else {
                    matches.add(node);
                }
            }
            node = next;
        }
        if (next != null) {
            readyDirectSubs.rotateTo(next);
        }
        return accepted ? null : matches;
    }

    public QueueDescriptor getDescriptor() {
        return queueDescriptor;
    }

    public boolean pollingDispatch() {

        // System.out.println("polling dispatch");

        // Keep looping until we can find one subscription that we can
        // dispatch a message to.
        while (true) {

            // Find a subscription that has a message available for dispatch.
            SubscriptionNode subNode = null;
            StoreNode<K, V> storeNode = null;
            synchronized (mutex) {

                if (readyPollingSubs.isEmpty()) {
                    return false;
                }

                SubscriptionNode next = null;
                subNode = readyPollingSubs.getHead();
                while (subNode != null) {
                    next = subNode.getNext();

                    storeNode = subNode.cursorPeek();
                    if (storeNode != null) {
                        // Found a message..
                        break;
                    } else {
                        // Cursor dried up... this subscriber can now be direct
                        // dispatched.
                        // System.out.println("Subscription state change: ready polling -> ready direct: "+subNode);
                        subNode.unlink();
                        readyDirectSubs.addLast(subNode);
                    }
                    subNode = next;
                }

                if (storeNode == null) {
                    return false;
                }

                if (next != null) {
                    readyPollingSubs.rotateTo(next);
                }
            }

            // The subscription's sink may be full..
            boolean accepted = subNode.subscription.offer(storeNode.getValue(), subNode, null);

            synchronized (mutex) {
                if (accepted) {
                    subNode.cursorNext();
                    if (/*subNode.subscription.isPreAcquired() &&*/ subNode.subscription.isRemoveOnDispatch(storeNode.getValue())) {
                        StoreNode<K, V> removed = store.remove(storeNode.getKey());
                        assert removed != null : "Since the node was aquired.. it should not have been removed by anyone else.";
                        sinkController.elementDispatched(storeNode.getValue());
                    }
                    return true;
                } else {
                    // System.out.println("Subscription state change: ready polling -> un-ready polling: "+subNode);
                    // Subscription is no longer ready..
                    subNode.cursorUnPeek(storeNode);
                    subNode.unlink();
                    unreadyPollingSubs.addLast(subNode);
                }
            }
        }
    }

    public final V poll() {
        throw new UnsupportedOperationException("Not supported");
    }

    public void addSubscription(Subscription<V> subscription) {
        synchronized (mutex) {
            SubscriptionNode node = subscriptions.get(subscription);
            if (node == null) {
                node = new SubscriptionNode(subscription);
                subscriptions.put(subscription, node);
                //sinks.put(subscription.getSink(), node);
                if (!store.isEmpty()) {
                    readyPollingSubs.addLast(node);
                    notifyReady();
                } else {
                    readyDirectSubs.addLast(node);
                }
            }
        }
    }

    public boolean removeSubscription(Subscription<V> subscription) {
        synchronized (mutex) {
            SubscriptionNode node = subscriptions.remove(subscription);
            if (node != null) {
                //sinks.remove(subscription.getSink());
                node.unlink();
                return true;
            }
            return false;
        }
    }

    private class SubscriptionNode extends LinkedNode<SubscriptionNode> implements ISourceController<V> {
        public final Subscription<V> subscription;
        public StoreCursor<K, V> cursor;

        public SubscriptionNode(Subscription<V> subscription) {
            this.subscription = subscription;
            this.cursor = store.openCursor();
        }

        public void resumeAt(StoreNode<K, V> node) {
            this.cursor = store.openCursorAt(node);
        }

        public void cursorNext() {
            cursor.next();
        }

        public StoreNode<K, V> cursorPeek() {
            if (cursor == null) {
                return null;
            }
            while (cursor.hasNext()) {
                StoreNode<K, V> elemNode = cursor.peekNext();

                // Skip over messages that are not a match.
                if (!subscription.matches(elemNode.getValue())) {
                    cursor.next();
                    continue;
                }

                //if (subscription.isPreAcquired()) {
                    if (elemNode.acquire(subscription)) {
                        return elemNode;
                    } else {
                        cursor.next();
                        continue;
                    }
                //}
            }
            cursor = null;
            return null;
        }

        public void cursorUnPeek(StoreNode<K, V> node) {
            //if (subscription.isPreAcquired()) {
                node.unacquire();
            //}
        }

        @Override
        public String toString() {
            return "subscription from " + getResourceName() + " to " + subscription;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.flow.ISourceController#elementDispatched(java.lang.Object)
         */
        public void elementDispatched(V elem) {
            
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.flow.ISourceController#getFlow()
         */
        public Flow getFlow() {
            return sinkController.getFlow();
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.flow.ISourceController#getFlowResource()
         */
        public IFlowResource getFlowResource() {
            return SharedQueueOld.this;
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.flow.ISourceController#onFlowBlock(org.apache.activemq.flow.ISinkController)
         */
        public void onFlowBlock(ISinkController<?> sinkController) {
            
            
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.flow.ISourceController#onFlowResume(org.apache.activemq.flow.ISinkController)
         */
        public void onFlowResume(ISinkController<?> sinkController) {
            synchronized (mutex) {
                    unlink();
                boolean notify = false;
                if (cursor == null) {
                    readyDirectSubs.addLast(this);
                    // System.out.println("Subscription state change: un-ready direct -> ready direct: "+node);
                } else {
                    if (readyPollingSubs.isEmpty()) {
                        notify = !store.isEmpty();
                    }
                    readyPollingSubs.addLast(this);
                    // System.out.println("Subscription state change: un-ready polling -> ready polling: "+node);
                }

                if (notify) {
                    notifyReady();
                }
            }
        }
    }

    public Mapper<K, V> getKeyMapper() {
        return keyMapper;
    }

    public void setKeyMapper(Mapper<K, V> keyMapper) {
        this.keyMapper = keyMapper;
    }

    @Override
    public String toString() {
        return getResourceName();
    }

    public FlowController<V> getFlowControler() {
        return this.sinkController;
    }

    public interface StoreNode<K, V> {

        public boolean acquire(Subscription<V> ownerId);

        public void unacquire();

        public V getValue();

        public K getKey();

    }

    public interface StoreCursor<K, V> extends Iterator<StoreNode<K, V>> {
        public StoreNode<K, V> peekNext();

        public void setNext(StoreNode<K, V> node);

    }

    private class TreeMemoryStore {
        AtomicLong counter = new AtomicLong();

        class MemoryStoreNode implements StoreNode<K, V> {
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
                    return true;
                }
                return false;
            }

            public K getKey() {
                return key;
            }

            public V getValue() {
                return value;
            }

            @Override
            public String toString() {
                return "node:" + id + ", owner=" + owner;
            }

            public void unacquire() {
                this.owner = null;
            }

        }

        class MemoryStoreCursor implements StoreCursor<K, V> {
            private long last = -1;
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

                SortedMap<Long, MemoryStoreNode> m = order.tailMap(last + 1);
                if (m.isEmpty()) {
                    next = null;
                } else {
                    next = m.get(m.firstKey());
                }
                return next != null;
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
                    last = next.id;
                    next = null;
                }
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }

        }

        protected HashMap<K, MemoryStoreNode> map = new HashMap<K, MemoryStoreNode>();
        protected TreeMap<Long, MemoryStoreNode> order = new TreeMap<Long, MemoryStoreNode>();

        public StoreNode<K, V> add(K key, V value) {
            MemoryStoreNode rc = new MemoryStoreNode(key, value);
            MemoryStoreNode oldNode = map.put(key, rc);
            if (oldNode != null) {
                map.put(key, oldNode);
                throw new IllegalArgumentException("Duplicate key violation");
            }
            order.put(rc.id, rc);
            return rc;
        }

        public StoreNode<K, V> remove(K key) {
            MemoryStoreNode node = (MemoryStoreNode) map.remove(key);
            if (node != null) {
                order.remove(node.id);
            }
            return node;
        }

        public boolean isEmpty() {
            return map.isEmpty();
        }

        public StoreCursor<K, V> openCursor() {
            MemoryStoreCursor cursor = new MemoryStoreCursor();
            return cursor;
        }

        public StoreCursor<K, V> openCursorAt(StoreNode<K, V> next) {
            MemoryStoreCursor cursor = new MemoryStoreCursor((MemoryStoreNode) next);
            return cursor;
        }

        public int size() {
            return map.size();
        }

    }

}
