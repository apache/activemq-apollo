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

import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.dispatch.DispatchPriority;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowController;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.IFlowSizeLimiter;
import org.apache.activemq.flow.ISinkController;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.queue.CursoredQueue.Cursor;
import org.apache.activemq.queue.CursoredQueue.CursorReadyListener;
import org.apache.activemq.queue.CursoredQueue.QueueElement;
import org.apache.activemq.queue.Subscription.SubscriptionDelivery;
import org.apache.activemq.util.Mapper;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.apache.activemq.util.list.LinkedNode;
import org.apache.activemq.util.list.LinkedNodeList;

/**
 * A SharedMessageQueue.
 * 
 * @author cmacnaug
 * 
 * @param <E>
 */
public class SharedQueue<K, V> extends AbstractFlowQueue<V> implements IQueue<K, V> {

    private static final boolean DEBUG = false;

    private final Object mutex;

    //The queue:
    private CursoredQueue<V> queue;

    private final Flow flow;
    // Limiter/Controller for the size of the queue:
    private FlowController<V> inputController;
    private final IFlowSizeLimiter<V> sizeLimiter;

    private final QueueDescriptor queueDescriptor;

    private static final int ACCEPTED = 0;
    private static final int NO_MATCH = 1;
    private static final int DECLINED = 2;

    private Mapper<K, V> keyMapper;

    private QueueStore<K, V> store;
    private PersistencePolicy<V> persistencePolicy;

    private SubscriptionContext exclusiveConsumer = null;
    private int exclusiveConsumerCount = 0;

    // Open consumers:
    private final HashMap<Subscription<V>, SubscriptionContext> consumers = new HashMap<Subscription<V>, SubscriptionContext>();
    private int startedConsumers = 0;
    // Tracks count of active subscriptions with a selector:
    private int activeSelectorSubs = 0;

    // Consumers that are operating against the shared cursor:
    private final LinkedNodeList<SubscriptionContext> sharedConsumers = new LinkedNodeList<SubscriptionContext>();

    // Browsing subscriptions that are ready for dispatch:
    private final LinkedNodeList<SubscriptionContext> readyBrowsers = new LinkedNodeList<SubscriptionContext>();

    // Consumers that are behind the shared cursor
    private final LinkedNodeList<SubscriptionContext> trailingConsumers = new LinkedNodeList<SubscriptionContext>();

    private boolean initialized = false;

    private Mapper<Long, V> expirationMapper;

    private Cursor<V> sharedCursor;

    public SharedQueue(String name, IFlowSizeLimiter<V> limiter) {
        this(name, limiter, null);
    }

    SharedQueue(String name, IFlowSizeLimiter<V> sizeLimiter, Object mutex) {
        super(name);
        this.mutex = mutex == null ? new Object() : mutex;

        flow = new Flow(getResourceName(), false);
        queueDescriptor = new QueueDescriptor();
        queueDescriptor.setQueueName(new AsciiBuffer(super.getResourceName()));
        queueDescriptor.setQueueType(QueueDescriptor.SHARED);
        this.sizeLimiter = sizeLimiter;
    }

    /**
     * Called to initialize the queue with values from the message store.
     * 
     * @param sequenceMin
     *            The lowest sequence number in the store.
     * @param sequenceMax
     *            The max sequence number in the store.
     * @param count
     *            The number of messages in the queue
     * @param size
     *            The size of the messages in the queue
     */
    public void initialize(long sequenceMin, long sequenceMax, int count, long size) {
        synchronized (mutex) {
            if (initialized) {
                throw new IllegalStateException("Already initialized");
            } else {
                initialized = true;

                // Default persistence policy when not set.
                if (persistencePolicy == null) {
                    persistencePolicy = new PersistencePolicy.NON_PERSISTENT_POLICY<V>();
                }

                inputController = new FlowController<V>(null, flow, sizeLimiter, mutex);
                inputController.useOverFlowQueue(false);
                super.onFlowOpened(inputController);

                //Initialize the limiter:
                if (count > 0) {
                    sizeLimiter.add(count, size);
                }

                queue = new CursoredQueue<V>(persistencePolicy, expirationMapper, flow, queueDescriptor, store, mutex) {

                    @Override
                    protected void onElementRemoved(QueueElement<V> elem) {
                        synchronized (mutex) {
                            //If the element wasn't acqired release space:
                            sizeLimiter.remove(1, elem.getLimiterSize());
                        }
                    }

                    @Override
                    protected Object getMutex() {
                        return mutex;
                    }

                    @Override
                    protected int getElementSize(V elem) {
                        return sizeLimiter.getElementSize(elem);
                    }

                    @Override
                    protected void requestDispatch() {
                        notifyReady();
                    }

                    @Override
                    protected void onElementReenqueued(QueueElement<V> qe, ISourceController<?> controller) {
                        synchronized (SharedQueue.this) {
                            if (isDispatchReady()) {
                                notifyReady();
                            }
                        }
                    }
                };

                queue.initialize(sequenceMin, sequenceMax, count, size);

                sharedCursor = openCursor(getResourceName(), true, true);
                sharedCursor.reset(sequenceMin);
                sharedCursor.setThrottleToMemoryWhenActive(persistencePolicy.isThrottleSourcesToMemoryLimit());

                if (DEBUG)
                    System.out.println(this + "Initialized, first seq: " + sequenceMin + " last sequence: " + sequenceMax + " count: " + count);
            }
        }
    }

    public void addSubscription(Subscription<V> subscription) {
        synchronized (mutex) {
            SubscriptionContext context = new SubscriptionContext(subscription);
            SubscriptionContext old = consumers.put(subscription, context);
            if (old != null) {
                context.close();
                consumers.put(subscription, old);
            } else {
                if (exclusiveConsumer == null) {
                    if (context.isExclusive()) {
                        exclusiveConsumer = context;
                    }
                }

                context.start();
            }
        }
    }

    public boolean removeSubscription(Subscription<V> subscription) {
        synchronized (mutex) {
            SubscriptionContext old = consumers.remove(subscription);
            if (old != null) {
                old.close();

                //Was this the exclusive consumer?
                if (old == exclusiveConsumer) {
                    if (exclusiveConsumerCount > 0) {
                        for (SubscriptionContext context : consumers.values()) {
                            if (context.isExclusive()) {
                                exclusiveConsumer = context;
                                //Update the dispatch list:
                                context.updateDispatchList();
                                break;
                            }
                        }
                    } else {
                        //Otherwise add the remaining subs to appropriate dispatch
                        //lists:
                        exclusiveConsumer = null;
                        for (SubscriptionContext context : consumers.values()) {
                            if (!context.sub.isBrowser()) {
                                context.updateDispatchList();
                            }
                        }
                    }
                }
                return true;
            }
            return false;
        }
    }

    public QueueDescriptor getDescriptor() {
        return queueDescriptor;
    }

    public int getEnqueuedCount() {
        synchronized (mutex) {
            return queue.getEnqueuedCount();
        }
    }

    public long getEnqueuedSize() {
        synchronized (mutex) {
            return sizeLimiter.getSize();
        }
    }

    private final Cursor<V> openCursor(String name, boolean pageInElements, boolean skipAcquired) {

        FlowController<QueueElement<V>> controller = null;
        if (pageInElements && persistencePolicy.isPagingEnabled() && sizeLimiter.getCapacity() > persistencePolicy.getPagingInMemorySize()) {
            IFlowSizeLimiter<QueueElement<V>> limiter = new SizeLimiter<QueueElement<V>>(persistencePolicy.getPagingInMemorySize(), persistencePolicy.getPagingInMemorySize() / 2) {
                @Override
                public int getElementSize(QueueElement<V> qe) {
                    return qe.getLimiterSize();
                };
            };

            controller = new FlowController<QueueElement<V>>(null, flow, limiter, mutex) {
                @Override
                public IFlowResource getFlowResource() {
                    return SharedQueue.this;
                }
            };
            controller.useOverFlowQueue(false);
            controller.setExecutor(dispatcher.getGlobalQueue(DispatchPriority.HIGH));
        }

        return queue.openCursor(name, controller, pageInElements, skipAcquired);
    }

    public void setKeyMapper(Mapper<K, V> keyMapper) {
        this.keyMapper = keyMapper;
    }

    public void setStore(QueueStore<K, V> store) {
        this.store = store;
    }

    public void setPersistencePolicy(PersistencePolicy<V> persistencePolicy) {
        this.persistencePolicy = persistencePolicy;
    }

    public void setExpirationMapper(Mapper<Long, V> expirationMapper) {
        this.expirationMapper = expirationMapper;
    }

    public V poll() {
        throw new UnsupportedOperationException("poll not supported for shared queue");
    }

    public IFlowController<V> getFlowControler() {
        return inputController;
    }

    /**
     * Starts this queue.
     */
    public void start() {
        synchronized (mutex) {
            if (!initialized) {
                throw new IllegalStateException("Not able to start uninitialized queue: " + getResourceName());
            }

            if (!started) {
                started = true;
                queue.start();
                sharedCursor.activate();
                if (startedConsumers == 0) {
                    sharedCursor.pause();
                }
                if (isDispatchReady()) {
                    notifyReady();
                }
            }
        }
    }

    /**
     * Stops this queue.
     */
    public void stop() {
        synchronized (mutex) {
            started = false;
            sharedCursor.pause();
            queue.stop();
        }
    }

    public void shutdown(final Runnable onShutdown) {
        super.shutdown(new Runnable() {
            public void run() {
                synchronized (mutex) {
                    queue.shutdown();
                }
                if( onShutdown!=null ) {
                    onShutdown.run();
                }
            }
        });
    }

    public void add(V elem, ISourceController<?> source) {
        synchronized (mutex) {
            inputController.add(elem, source);
            accepted(source, elem);
        }
    }

    public void remove(long key) {
        synchronized (mutex) {
            queue.remove(key);
        }
    }

    public boolean offer(V elem, ISourceController<?> source) {
        synchronized (mutex) {

            if (inputController.offer(elem, source)) {
                accepted(source, elem);
                return true;
            }
            return false;
        }
    }

    public void flowElemAccepted(ISourceController<V> source, V elem) {
        throw new UnsupportedOperationException("Flow Controller pass-through not supported");
    }

    private final void accepted(ISourceController<?> source, V elem) {

        if (!initialized) {
            throw new IllegalStateException("Uninitialized queue: " + getResourceName());
        }

        // Add it to the queue:
        queue.add(source, elem);

        // Request dispatch for the newly enqueued element.
        if (isDispatchReady()) {
            notifyReady();
            //TODO consider doing direct dispatch. This can boost 
            //performance significantly. However, this should likely
            //be done through the dispatcher itself to give the 
            //execution load balancer an opportunity to build up
            //dispatch relationships.

            //while(pollingDispatch());
        }
    }

    public boolean pollingDispatch() {

        synchronized (mutex) {
            //Do an queue house keeping:
            queue.dispatch();

            // Dispatch ready consumers:
            SubscriptionContext consumer = trailingConsumers.getHead();
            while (consumer != null) {
                SubscriptionContext next = consumer.getNext();
                consumer.trailingDispatch();
                if (next != null) {
                    consumer = next;
                } else {
                    consumer = trailingConsumers.getHead();
                }
            }

            // Service any browsers:
            SubscriptionContext browser = readyBrowsers.getHead();
            while (browser != null) {
                SubscriptionContext nextBrowser = browser.getNext();
                browser.trailingDispatch();
                if (nextBrowser != null) {
                    browser = nextBrowser;
                } else {
                    break;
                }
            }

            // Process shared consumers:
            if (!sharedConsumers.isEmpty()) {
                QueueElement<V> next = sharedCursor.getNext();

                if (next != null) {

                    // See if there are any interested consumers:
                    consumer = sharedConsumers.getHead();
                    boolean interested = false;

                    find_consumer: while (consumer != null) {

                        // Get the nextConsumer now since the consumer may
                        // remove itself
                        // from the list when we offer to it:
                        SubscriptionContext nextConsumer = consumer.getNext();
                        switch (consumer.offer(next)) {
                        case ACCEPTED:
                            // Rotate list so this one is last next time:
                            sharedConsumers.rotate();
                            interested = true;
                            break find_consumer;
                        case DECLINED:
                            interested = true;
                            break;
                        case NO_MATCH:
                            // Move on to the next consumer if this one didn't
                            // match
                            consumer = consumer.getNext();
                        }

                        consumer = nextConsumer;
                    }

                    // Advance the shared cursor if no one was interested:
                    if (!interested) {
                        sharedCursor.skip(next);
                    }
                }
            }
            return isDispatchReady();
        }

    }

    public boolean isDispatchReady() {
        if (!initialized) {
            return false;
        }

        if (started) {
            // If we have shared consumers, and an element ready for dispatch
            if (!sharedConsumers.isEmpty() && sharedCursor.isReady()) {
                return true;
            }

            // If there are ready trailing consumers:
            if (!trailingConsumers.isEmpty()) {
                return true;
            }

            // Might consider allowing browsers to browse
            // while stopped:
            if (!readyBrowsers.isEmpty()) {
                return true;
            }
        }

        //Check if the queue needs dispatch:
        if (queue.needsDispatch()) {
            return true;
        }

        return false;
    }

    /**
     * This class holds state associated with a subscription in this queue.
     */
    class SubscriptionContext extends LinkedNode<SubscriptionContext> implements ISourceController<V> {

        final Subscription<V> sub;
        boolean isStarted;

        // The consumer's cursor:
        final Cursor<V> cursor;

        SubscriptionContext(Subscription<V> target) {
            this.sub = target;
            this.cursor = openCursor(target.toString(), true, !sub.isBrowser());
            if (isExclusive()) {
                exclusiveConsumerCount++;
            }
            cursor.setCursorReadyListener(new CursorReadyListener() {
                public void onElementReady() {
                    if (!isLinked()) {
                        updateDispatchList();
                    }
                }
            });
        }

        public boolean isExclusive() {
            return sub.isExclusive() && !sub.isBrowser();
        }

        public void start() {
            if (!isStarted) {
                isStarted = true;
                if (!sub.isBrowser()) {

                    if (sub.hasSelector()) {
                        activeSelectorSubs++;
                    }
                    if (++startedConsumers == 1) {
                        sharedCursor.resume();
                    }
                }

                if (queue.isEmpty()) {
                    cursor.reset(sharedCursor.getCurrentSequeunce());
                } else {
                    cursor.reset(queue.getFirstSequence());
                }

                if (DEBUG)
                    System.out.println("Starting " + this + " at " + cursor);

                updateDispatchList();
            }
        }

        public void stop() {
            // If started remove this from any dispatch list
            if (isStarted) {
                if (!sub.isBrowser()) {

                    if (sub.hasSelector()) {
                        activeSelectorSubs--;
                    }
                    if (--startedConsumers == 1) {
                        sharedCursor.pause();
                    }
                }
                unlink();
                isStarted = false;
            }
        }

        public void close() {
            if (isExclusive()) {
                exclusiveConsumerCount--;
            }

            stop();
        }

        /**
         * When the consumer is trailing the dispatch calls this method until
         * the consumer is caught up.
         */
        public final void trailingDispatch() {

            if (checkJoinShared()) {
                return;
            }

            QueueElement<V> next = cursor.getNext();
            // If the next element isn't yet available
            // then unlink this subscription
            if (next == null) {
                unlink();
            } else {
                offer(next);
            }
        }

        private final boolean checkJoinShared() {
            if (list == sharedConsumers) {
                return true;
            }

            // Browsers always operate independently:
            if (sub.isBrowser()) {
                return false;
            }

            boolean join = false;
            //If we are the exlusive consumer then we join the shared
            //cursor:
            if (exclusiveConsumer == this) {
                join = true;
            }
            //Otherwise if we aren't we won't be joining anything!
            else if (exclusiveConsumer != null) {
                return false;
            } else if (activeSelectorSubs == 0) {
                join = true;
            } else {

                // TODO Even if there are subscriptions with selectors present
                // we can still join the shared cursor as long as there is at
                // least one ready selector-less sub.
                cursor.getNext();
                if (queue.isEmpty() || cursor.compareTo(sharedCursor) >= 0) {
                    join = true;
                }
            }

            // Have we joined the shared cursor? If so deactivate our cursor,
            // and link to the
            // sharedConsumers list:
            if (join) {
                cursor.deactivate();
                unlink();
                sharedConsumers.addLast(this);
                return true;
            }
            return false;
        }

        /**
         * Adds to subscription to the appropriate dispatch list:
         */
        final void updateDispatchList() {

            if (!checkJoinShared()) {
                //Otherwise if we're not the exclusive consumer
                if (!sub.isBrowser() && exclusiveConsumer != null) {
                    return;
                }

                // Make sure our cursor is activated:
                cursor.activate();
                // If our next element is paged out
                // Add to the restoring consumers list:
                if (cursor.isReady()) {
                    if (!sub.isBrowser()) {
                        trailingConsumers.addLast(this);
                    } else {
                        readyBrowsers.addLast(this);
                    }

                    if (isDispatchReady()) {
                        notifyReady();
                    }
                } else {
                    // Unlink ouselves if our cursor isn't ready:
                    unlink();
                }
            } else {
                // Notify ready if we were the first in the list and
                // we are ready for dispatch:
                if (sharedConsumers.size() == 1 && isDispatchReady()) {
                    notifyReady();
                }
            }
        }

        public final int offer(QueueElement<V> qe) {

            // If we are already passed this element return NO_MATCH:
            if (cursor.getCurrentSequeunce() > qe.sequence) {
                return NO_MATCH;
            }

            // If this element isn't matched, NO_MATCH:
            if (!sub.matches(qe.elem)) {
                cursor.skip(qe);
                return NO_MATCH;
            }

            // Check for expiration:
            if (qe.isExpired()) {
                qe.acknowledge();
                return ACCEPTED;
            }

            // If the sub doesn't remove on dispatch pass it the callback
            SubscriptionDelivery<V> callback = sub.isRemoveOnDispatch(qe.elem) ? null : qe;
            // If the sub is a browser don't pass it a callback since it does not need to 
            // delete messages
            if (sub.isBrowser()) {
                callback = null;
            }

            // See if the sink has room:
            qe.setAcquired(sub);
            if (sub.offer(qe.elem, this, callback)) {
                if (DEBUG)
                    System.out.println("Dispatched " + qe.getElement() + " to " + this);

                if (!sub.isBrowser()) {

                    // If remove on dispatch acknowledge now:
                    if (callback == null) {
                        qe.acknowledge();
                    }
                } else {
                    qe.setAcquired(null);
                }

                // Advance our cursor:
                cursor.skip(qe);

                return ACCEPTED;
            } else {
                qe.setAcquired(null);
                // Remove from dispatch list until we are resumed:
                if (DEBUG) {
                    System.out.println(this + " Declined: " + qe);
                }
                return DECLINED;
            }
        }

        // ///////////////////////////////////////////////////////////////////////////////
        // Source sizeController implementation
        // ///////////////////////////////////////////////////////////////////////////////
        public void elementDispatched(V elem) {
            // No-op we only offer to the consumer
        }

        public Flow getFlow() {
            return flow;
        }

        public IFlowResource getFlowResource() {
            return SharedQueue.this;
        }

        public void onFlowBlock(ISinkController<?> sinkController) {
            if (DEBUG)
                System.out.println(this + " blocked.");
            synchronized (mutex) {
                unlink();
            }
        }

        public void onFlowResume(ISinkController<?> sinkController) {
            if (DEBUG)
                System.out.println(this + " resumed.");
            synchronized (mutex) {
                updateDispatchList();
            }
        }

        public String toString() {
            return sub + ", " + cursor;
        }
    }

    public String toString() {
        return "SharedQueue: " + getResourceName();
    }
}
