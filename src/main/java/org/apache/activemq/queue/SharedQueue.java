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

import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowLimiter;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.IFlowSource;
import org.apache.activemq.flow.ISinkController;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.queue.Store.StoreCursor;
import org.apache.activemq.queue.Store.StoreNode;
import org.apache.kahadb.util.LinkedNode;
import org.apache.kahadb.util.LinkedNodeList;

/**
 * IQueue which does direct dispatch whenever it can.
 */
public class SharedQueue<K, V> extends AbstractFlowQueue<V> implements IQueue<K, V> {

    protected Store<K, V> store = new TreeMemoryStore<K, V>();

    private final LinkedNodeList<SubscriptionNode> unreadyDirectSubs = new LinkedNodeList<SubscriptionNode>();
    private final LinkedNodeList<SubscriptionNode> readyDirectSubs = new LinkedNodeList<SubscriptionNode>();

    private final LinkedNodeList<SubscriptionNode> unreadyPollingSubs = new LinkedNodeList<SubscriptionNode>();
    private final LinkedNodeList<SubscriptionNode> readyPollingSubs = new LinkedNodeList<SubscriptionNode>();

    private final HashMap<Subscription<V>, SubscriptionNode> subscriptions = new HashMap<Subscription<V>, SubscriptionNode>();
    private final HashMap<IFlowSink<V>, SubscriptionNode> sinks = new HashMap<IFlowSink<V>, SubscriptionNode>();

    private final FlowController<V> sinkController;
    private final Object mutex;
    private final AbstractFlowQueue whoToWakeup = this;

    protected Mapper<K, V> keyMapper;
    private long directs;

    private final ISourceController<V> sourceControler = new ISourceController<V>() {

        public Flow getFlow() {
            return sinkController.getFlow();
        }

        public void elementDispatched(V elem) {
        }

        public void onFlowBlock(ISinkController<V> sink) {
        }

        public void onFlowResume(ISinkController<V> sinkController) {
            IFlowSink<V> sink = sinkController.getFlowSink();
            synchronized (mutex) {
                SubscriptionNode node = sinks.get(sink);
                if (node != null) {
                    node.unlink();
                    boolean notify = false;
                    if (node.cursor == null) {
                        readyDirectSubs.addLast(node);
                        // System.out.println("Subscription state change: un-ready direct -> ready direct: "+node);
                    } else {
                        if (readyPollingSubs.isEmpty()) {
                            notify = !store.isEmpty();
                        }
                        readyPollingSubs.addLast(node);
                        // System.out.println("Subscription state change: un-ready polling -> ready polling: "+node);
                    }
                    if (notify) {
                        notifyReady();
                    }
                }
            }
        }

        @Override
        public String toString() {
            return getResourceName();
        }

        public boolean isSourceBlocked() {
            throw new UnsupportedOperationException();
        }

        public IFlowSource<V> getFlowSource() {
            return SharedQueue.this;
        }

    };

    public SharedQueue(String name, IFlowLimiter<V> limiter) {
        this(name, limiter, new Object());
    }

    /**
     * Creates a flow queue that can handle multiple flows.
     * 
     * @param flow
     *            The {@link Flow}
     * @param controller
     *            The FlowController if this queue is flow controlled:
     */
    public SharedQueue(String name, IFlowLimiter<V> limiter, Object mutex) {
        super(name);
        this.mutex = mutex;
        Flow flow = new Flow(name, false);
        this.sinkController = new FlowController<V>(getFlowControllableHook(), flow, limiter, mutex);
        super.onFlowOpened(sinkController);
    }

    public boolean offer(V elem, ISourceController<V> source) {
        return sinkController.offer(elem, source);
    }

    /**
     * Performs a limited add to the queue.
     */
    public final void add(V value, ISourceController<V> source) {
        sinkController.add(value, source);
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
        return !store.isEmpty() && !readyPollingSubs.isEmpty();
    }

    private ArrayList<SubscriptionNode> directDispatch(V elem) {
        ArrayList<SubscriptionNode> matches = new ArrayList<SubscriptionNode>(readyDirectSubs.size());
        boolean accepted = false;
        SubscriptionNode next = null;
        SubscriptionNode node = readyDirectSubs.getHead();
        while (node != null) {
            next = node.getNext();
            if (node.subscription.matches(elem)) {
                accepted = node.subscription.getSink().offer(elem, sourceControler);
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

    public boolean pollingDispatch() {

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
            IFlowSink<V> sink = subNode.subscription.getSink();
            boolean accepted = sink.offer(storeNode.getValue(), sourceControler);

            synchronized (mutex) {
                if (accepted) {
                    subNode.cursorNext();
                    if (subNode.subscription.isPreAcquired() && subNode.subscription.isRemoveOnDispatch()) {
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

    public void addSubscription(Subscription<V> subscription) {
        synchronized (mutex) {
            SubscriptionNode node = subscriptions.get(subscription);
            if (node == null) {
                node = new SubscriptionNode(subscription);
                subscriptions.put(subscription, node);
                sinks.put(subscription.getSink(), node);
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
                sinks.remove(subscription.getSink());
                node.unlink();
                return true;
            }
            return false;
        }
    }

    private class SubscriptionNode extends LinkedNode<SubscriptionNode> {
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

                if (subscription.isPreAcquired()) {
                    if (elemNode.acquire(subscription)) {
                        return elemNode;
                    } else {
                        cursor.next();
                        continue;
                    }
                }
            }
            cursor = null;
            return null;
        }

        public void cursorUnPeek(StoreNode<K, V> node) {
            if (subscription.isPreAcquired()) {
                node.unacquire();
            }
        }

        @Override
        public String toString() {
            return "subscription from " + getResourceName() + " to " + subscription;
        }
    }

    public Mapper<K, V> getKeyMapper() {
        return keyMapper;
    }

    public void setKeyMapper(Mapper<K, V> keyMapper) {
        this.keyMapper = keyMapper;
    }

    public boolean removeByKey(K key) {
        return false;
    }

    public boolean removeByValue(V value) {
        return false;
    }

    @Override
    public String toString() {
        return getResourceName();
    }

    public FlowController<V> getFlowControler() {
        return this.sinkController;
    }
}
