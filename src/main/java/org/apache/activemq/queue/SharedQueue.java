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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowController;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.IFlowSizeLimiter;
import org.apache.activemq.flow.ISinkController;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.queue.QueueStore.QueueDescriptor;
import org.apache.activemq.queue.QueueStore.RestoreListener;
import org.apache.activemq.queue.QueueStore.RestoredElement;
import org.apache.activemq.queue.QueueStore.SaveableQueueElement;
import org.apache.activemq.queue.Subscription.SubscriptionDeliveryCallback;
import org.apache.activemq.util.Mapper;
import org.apache.activemq.util.SortedLinkedList;
import org.apache.activemq.util.SortedLinkedListNode;
import org.apache.kahadb.util.LinkedNode;
import org.apache.kahadb.util.LinkedNodeList;

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

    private final Flow flow;
    private final QueueStore.QueueDescriptor queueDescriptor;
    // For now each queue element is assigned a restoreBlock number
    // which is used for tracking page in requests. A trailing
    // consumer will request messages from at most one restoreBlock
    // at a time from the database.
    private static final int RESTORE_BLOCK_SIZE = 1000;

    private static final int ACCEPTED = 0;
    private static final int NO_MATCH = 1;
    private static final int DECLINED = 2;

    private final SortedLinkedList<QueueElement<V>> queue = new SortedLinkedList<QueueElement<V>>();
    private Mapper<K, V> keyMapper;

    private final ElementLoader loader;
    private Cursor<V> sharedCursor;
    private QueueStore<K, V> store;
    private PersistencePolicy<V> persistencePolicy;
    private long nextSequenceNumber = 0;

    // Open consumers:
    private final HashMap<Subscription<V>, SubscriptionContext> consumers = new HashMap<Subscription<V>, SubscriptionContext>();
    // Tracks count of active subscriptions with a selector:
    private int activeSelectorSubs = 0;

    // Consumers that are operating against the shared cursor:
    private final LinkedNodeList<SubscriptionContext> sharedConsumers = new LinkedNodeList<SubscriptionContext>();

    // Browsing subscriptions that are ready for dispatch:
    private final LinkedNodeList<SubscriptionContext> readyBrowsers = new LinkedNodeList<SubscriptionContext>();

    // Consumers that are behind the shared cursor
    private final LinkedNodeList<SubscriptionContext> trailingConsumers = new LinkedNodeList<SubscriptionContext>();

    // Limiter/Controller for the size of the queue:
    private FlowController<V> inputController;
    private final IFlowSizeLimiter<V> sizeLimiter;
    private final boolean RELEASE_ON_ACQUISITION = true;

    private int totalQueueCount;

    private boolean initialized = false;
    private boolean started = false;

    private Mapper<Long, V> expirationMapper;
    private Expirator expirator;

    public SharedQueue(String name, IFlowSizeLimiter<V> limiter) {
        this(name, limiter, null);
    }

    SharedQueue(String name, IFlowSizeLimiter<V> sizeLimiter, Object mutex) {
        super(name);
        this.mutex = mutex == null ? new Object() : mutex;

        flow = new Flow(getResourceName(), false);
        queueDescriptor = new QueueStore.QueueDescriptor();
        queueDescriptor.setQueueName(new AsciiBuffer(super.getResourceName()));
        queueDescriptor.setQueueType(QueueDescriptor.SHARED);
        this.sizeLimiter = sizeLimiter;
        loader = new ElementLoader();

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

                // Default persistence policy when not set.
                if (persistencePolicy == null) {
                    persistencePolicy = new PersistencePolicy.NON_PERSISTENT_POLICY<V>();
                }

                inputController = new FlowController<V>(null, flow, sizeLimiter, mutex);
                inputController.useOverFlowQueue(false);
                super.onFlowOpened(inputController);

                sharedCursor = openCursor(getResourceName(), true, true);

                // Initialize counts:
                nextSequenceNumber = sequenceMax + 1;
                if (count > 0) {
                    sizeLimiter.add(count, size);
                    totalQueueCount = count;
                    // Add a paged out placeholder:
                    QueueElement<V> qe = new QueueElement<V>(null, sequenceMin, this);
                    qe.loaded = false;
                    queue.add(qe);
                }

                initialized = true;
                sharedCursor.reset(sequenceMin);

                // Create an expiration mapper if one is not set.
                if (expirationMapper == null) {
                    expirationMapper = new Mapper<Long, V>() {
                        public Long map(V element) {
                            return -1L;
                        }
                    };
                }

                expirator = new Expirator();

                if (DEBUG)
                    System.out.println(this + "Initialized, first seq: " + sequenceMin + " next sequence: " + nextSequenceNumber);
            }
        }
    }

    public QueueStore.QueueDescriptor getDescriptor() {
        return queueDescriptor;
    }

    public int getEnqueuedCount() {
        synchronized (mutex) {
            return totalQueueCount;
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
                public int getElementSize(QueueElement<V> qe) {
                    return qe.size;
                };
            };

            controller = new FlowController<QueueElement<V>>(null, flow, limiter, mutex) {
                @Override
                public IFlowResource getFlowResource() {
                    return SharedQueue.this;
                }
            };
            controller.useOverFlowQueue(false);
            controller.setExecutor(dispatcher.createPriorityExecutor(dispatcher.getDispatchPriorities() - 1));
        }

        return new Cursor<V>(queue, loader, name, skipAcquired, pageInElements, controller);
    }

    final int getElementSize(V elem) {
        return sizeLimiter.getElementSize(elem);
    }

    final long getElementExpiration(V elem) {
        return expirationMapper.map(elem);
    }

    final Expirator getExpirator() {
        return expirator;
    }

    final QueueStore<K, V> getQueueStore() {
        return store;
    }

    final ElementLoader getLoader() {
        return loader;
    }

    final PersistencePolicy<V> getPersistencePolicy() {
        return persistencePolicy;
    }

    final void acknowledge(QueueElement<V> qe) {
        synchronized (mutex) {
            V elem = qe.getElement();
            if (qe.delete()) {
                if (!qe.acquired || !RELEASE_ON_ACQUISITION) {
                    inputController.elementDispatched(elem);
                }
                totalQueueCount--;
            }
        }
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
                sharedCursor.activate();
                loader.start();
                expirator.start();
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
        }
    }

    public void shutdown() {
        stop();
    }

    public void add(V elem, ISourceController<?> source) {
        synchronized (mutex) {
            inputController.add(elem, source);
            accepted(source, elem);
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
        synchronized (mutex) {
            // TODO should change flow controller to pass original source:
            accepted(null, elem);
        }
    }

    private final void accepted(ISourceController<?> source, V elem) {

        if (!initialized) {
            throw new IllegalStateException("Uninitialized queue: " + getResourceName());
        }

        // Create a new queue element with the next sequence number:
        QueueElement<V> qe = new QueueElement<V>(elem, nextSequenceNumber++, this);

        // Save the element (note that it is important this be done after
        // we've set the sequence number above)
        if (persistencePolicy.isPersistent(elem)) {
            // For now base decision on whether to delay flush on
            // whether or not there are
            // consumers ready.
            boolean delayable = !sharedConsumers.isEmpty();
            qe.save(source, delayable);
        }

        // Add it to our queue:
        queue.add(qe);
        totalQueueCount++;
        if (!persistencePolicy.isPagingEnabled()) {
            qe.addHardRef();
        }
        // Check with the shared cursor to see if it is willing to
        // absorb the element. If so that's good enough.
        if (persistencePolicy.isPagingEnabled() && !sharedCursor.offer(qe, source)) {

            // Otherwise check with any other open cursor to see if
            // it can take the element:
            Collection<Cursor<V>> active = loader.getActiveCursors(qe);

            // If there are none, unload the element:
            if (active == null) {
                qe.unload(source);
                return;
            }

            // See if a cursor is willing to hang on to the
            // element:
            boolean accepted = false;
            for (Cursor<V> cursor : active) {
                // Already checked the shared cursor above:
                if (cursor == sharedCursor) {
                    continue;
                }

                if (cursor.offer(qe, source)) {
                    accepted = true;
                    break;
                }
            }

            // If no cursor accepted it, then page out the element:
            // keeping the element loaded.
            if (!accepted) {
                qe.unload(source);
            }
        }

        expirator.elementAdded(qe);

        // Request dispatch for the newly enqueued element.
        // TODO consider optimizing to do direct dispatch?
        // It might be better if the dispatcher itself provided
        // this for cases where the caller is on the same dispatcher
        if (isDispatchReady()) {
            notifyReady();
            // while(pollingDispatch());
        }
    }

    public boolean pollingDispatch() {

        synchronized (mutex) {
            loader.processLoadRequests();

            expirator.dispatch();

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

        // If there are restored messages ready for enqueue:
        if (loader.hasRestoredMessages()) {
            return true;
        }

        if (expirator.needsDispatch()) {
            return true;
        }

        return false;
    }

    public void addSubscription(Subscription<V> subscription) {
        synchronized (mutex) {
            SubscriptionContext context = new SubscriptionContext(subscription);
            SubscriptionContext old = consumers.put(subscription, context);
            if (old != null) {
                consumers.put(subscription, old);
            } else {
                context.start();
            }
        }
    }

    public boolean removeSubscription(Subscription<V> subscription) {
        synchronized (mutex) {
            SubscriptionContext old = consumers.remove(subscription);
            if (old != null) {
                old.close();
                return true;
            }
            return false;
        }
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
            cursor.setCursorReadyListener(new CursorReadyListener() {
                public void onElementReady() {
                    if (!isLinked()) {
                        updateDispatchList();
                    }
                }
            });
        }

        public void start() {
            if (!isStarted) {
                isStarted = true;
                if (sub.hasSelector() && !sub.isBrowser()) {
                    activeSelectorSubs++;
                }
                if (queue.isEmpty()) {
                    cursor.reset(sharedCursor.getCurrentSequeunce());
                } else {
                    cursor.reset(queue.getHead().sequence);
                }

                updateDispatchList();
            }
        }

        public void stop() {
            // If started remove this from any dispatch list
            if (isStarted) {
                if (sub.hasSelector() && !sub.isBrowser()) {
                    activeSelectorSubs--;
                }
                cursor.deactivate();
                unlink();
                isStarted = false;
            }
        }

        public void close() {
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

            // TODO Even if there are subscriptions with selectors present
            // we can still join the shared cursor as long as there is at
            // least one ready selector-less sub.
            boolean join = false;
            if (activeSelectorSubs == 0) {
                join = true;
            } else {
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
        private final void updateDispatchList() {

            if (!checkJoinShared()) {
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
                acknowledge(qe);
                return ACCEPTED;
            }

            // If the sub doesn't remove on dispatch set an ack listener:
            SubscriptionDeliveryCallback callback = sub.isRemoveOnDispatch() ? null : qe;

            // See if the sink has room:
            if (sub.offer(qe.elem, this, callback)) {
                if (!sub.isBrowser()) {
                    qe.setAcquired(true);
                    if (RELEASE_ON_ACQUISITION) {
                        inputController.elementDispatched(qe.getElement());
                    }

                    // If remove on dispatch acknowledge now:
                    if (callback == null) {
                        qe.acknowledge();
                    }
                }

                // Advance our cursor:
                cursor.skip(qe);

                return ACCEPTED;
            } else {
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

    public interface CursorReadyListener {
        public void onElementReady();
    }

    static class Cursor<V> implements Comparable<Cursor<V>> {

        private CursorReadyListener readyListener;

        private final String name;
        private final SharedQueue<?, V>.ElementLoader loader;
        private final SortedLinkedList<QueueElement<V>> queue;

        private boolean activated = false;;

        // The next element for this cursor, always non null
        // if activated, unless no element available:
        QueueElement<V> current = null;
        // The current sequence number for this cursor,
        // used when inactive or pointing to an element
        // sequence number beyond the queue's limit.
        long sequence = -1;

        // The cursor is holding references for all
        // elements between first and last inclusive:
        QueueElement<V> firstRef = null;
        QueueElement<V> lastRef = null;
        // This is set to the last block that for which
        // we have requested a load:
        long lastBlockRequest = -1;

        // Each cursor can optionally be memory limited
        // When the limiter is set the cursor is able to
        // keep as many elements in memory as its limiter
        // allows.
        private final IFlowController<QueueElement<V>> memoryController;

        // Indicates whether this cursor skips acquired elements
        private final boolean skipAcquired;
        // Indicates whether this cursor will page in elements
        private final boolean pageInElements;

        private long limit = Long.MAX_VALUE;

        public Cursor(SortedLinkedList<QueueElement<V>> queue, SharedQueue<?, V>.ElementLoader loader, String name, boolean skipAcquired, boolean pageInElements,
                IFlowController<QueueElement<V>> memoryController) {
            this.name = name;
            this.queue = queue;
            this.loader = loader;

            this.skipAcquired = skipAcquired;
            this.pageInElements = pageInElements;

            // Set up a limiter if this cursor pages in elements, and memory
            // limit is less than the queue size:
            if (pageInElements) {
                this.memoryController = memoryController;
            } else {
                this.memoryController = null;
            }
        }

        /**
         * Offers a queue element to the cursor's memory limiter The cursor will
         * return true if it has room for it in memory.
         * 
         * @param qe
         *            The element for which to check.
         * @return
         */
        public final boolean offer(QueueElement<V> qe, ISourceController<?> controller) {
            if (activated && memoryController != null) {
                getNext();
                if (lastRef != null) {
                    // Return true if we absorbed it:
                    if (qe.sequence <= lastRef.sequence && qe.sequence >= firstRef.sequence) {
                        return true;
                    }
                    // If our last ref is close to this one reserve the element
                    else if (qe.getPrevious() == lastRef) {
                        if (addCursorRef(qe, controller)) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
                return false;
            }
            // Always accept an element if not memory
            // limited providing we're active:
            return activated;
        }

        public final void reset(long sequence) {
            updateSequence(sequence);
            updateCurrent(null);
        }

        public final void activate() {
            if (!activated) {
                activated = true;
                getNext();
            }
        }

        public final boolean isActivated() {
            return activated;
        }

        public final void deactivate() {
            if (activated) {
                // Release all of our references:
                while (firstRef != null) {
                    firstRef.releaseHardRef(memoryController);

                    // If we're passing into a new block release the old one:
                    if (firstRef.isLastInBlock()) {
                        if (DEBUG)
                            System.out.println(this + " releasing block:" + firstRef.restoreBlock);
                        loader.releaseBlock(this, firstRef.restoreBlock);
                    }

                    if (firstRef == lastRef) {
                        firstRef = lastRef = null;
                    } else {
                        firstRef = firstRef.getNext();
                    }
                }

                // Release the last requested block:
                if (loader.isPageOutPlaceHolders() && lastBlockRequest >= 0) {
                    loader.releaseBlock(this, lastBlockRequest);
                }

                lastBlockRequest = -1;

                updateCurrent(null);
                activated = false;
            }
        }

        /**
         * Updates the current ref. We keep a soft ref to the current to keep it
         * in the queue so that we can get at the next without a costly lookup.
         */
        private final void updateCurrent(QueueElement<V> qe) {
            if (qe == current) {
                return;
            }
            if (current != null) {
                current.releaseSoftRef();
            }
            current = qe;
            if (current != null) {
                current.addSoftRef();
            }
        }

        /**
         * Makes sure elements are paged in
         */
        private final void updatePagingRefs() {
            if (!activated)
                return;

            if (pageInElements && memoryController != null) {

                // Release memory references up to our sequence number
                while (firstRef != null && firstRef.getSequence() < sequence) {
                    boolean lastInBlock = firstRef.isLastInBlock();
                    QueueElement<V> next = firstRef.getNext();
                    firstRef.releaseHardRef(memoryController);

                    // If we're passing into a new block release the old one:
                    if (lastInBlock) {
                        if (DEBUG)
                            System.out.println(this + " releasing block:" + firstRef.restoreBlock);

                        loader.releaseBlock(this, firstRef.restoreBlock);
                    }

                    // If we've reach our last ref null out held refs:
                    if (firstRef == lastRef) {
                        firstRef = lastRef = null;
                    } else {
                        firstRef = next;
                    }
                }

                // Now add refs for as many elements as we can hold:
                QueueElement<V> next = null;
                if (lastRef == null) {
                    next = current;
                } else {
                    next = lastRef.getNext();
                }

                while (next != null && !memoryController.isSinkBlocked()) {
                    if (!addCursorRef(next, null)) {
                        break;
                    }
                    next = lastRef.getNext();
                }
            }
            // Otherwise we still need to ensure the block has been loaded:
            else if (current != null && !current.isLoaded()) {
                if (lastBlockRequest != current.restoreBlock) {
                    if (lastBlockRequest != -1) {
                        loader.releaseBlock(this, lastBlockRequest);
                    }
                    lastBlockRequest = current.restoreBlock;
                    loader.reserveBlock(this, lastBlockRequest);
                }
            }
        }

        /**
         * Keeps the element paged in for this cursor accounting for it in the
         * cursor's memory limiter. The provided controller is blocked if this
         * overflows this cursor's limiter.
         * 
         * @param qe
         *            The element to hold in memory.
         * @param controller
         *            The controller adding the element.
         * @return false if the element isn't in memory.
         */
        private final boolean addCursorRef(QueueElement<V> qe, ISourceController<?> controller) {
            // Make sure we have requested the block:
            if (qe.restoreBlock != lastBlockRequest) {
                lastBlockRequest = qe.restoreBlock;
                if (DEBUG)
                    System.out.println(this + " requesting block:" + lastBlockRequest + " for" + qe);
                loader.reserveBlock(this, lastBlockRequest);
            }

            // If the next element isn't loaded then we can't yet
            // reference it:
            if (!qe.isLoaded()) {
                return false;
            }

            qe.addHardRef();
            if (firstRef == null) {
                firstRef = qe;
            }
            memoryController.add(qe, controller);
            lastRef = qe;
            return true;
        }

        private final void updateSequence(final long newSequence) {
            this.sequence = newSequence;
        }

        /**
         * Sets the cursor to the next sequence number after the provided
         * element:
         */
        public final void skip(QueueElement<V> elem) {
            QueueElement<V> next = elem.isLinked() ? elem.getNext() : null;

            if (next != null) {
                updateSequence(next.sequence);
                if (activated) {
                    updateCurrent(next);
                }
            } else {
                updateCurrent(null);
                updateSequence(sequence + 1);
            }
            updatePagingRefs();
        }

        /**
         * @return the next available element or null if one is not currently
         *         available.
         */
        public final QueueElement<V> getNext() {

            try {
                if (queue.isEmpty() || queue.getTail().sequence < sequence) {
                    updateCurrent(null);
                    return null;
                }

                if (queue.getTail().sequence == sequence) {
                    updateCurrent(queue.getTail());
                }

                // If we don't have a current, then look it up based
                // on our sequence:
                if (current == null) {
                    updateCurrent(queue.upper(sequence, true));
                    if (current == null) {
                        return null;
                    }
                }

                // Skip removed elements (and acquired ones if requested)
                while ((skipAcquired && current.isAcquired()) || current.isDeleted()) {
                    QueueElement<V> last = current;
                    updateCurrent(current.getNext());

                    // If the next element is null, increment our sequence
                    // and return:
                    if (current == null) {
                        updateSequence(last.getSequence() + 1);
                        return null;
                    }

                    // Break if we're waiting to load an element
                    if (!current.isLoaded()) {
                        break;
                    }

                    // If we're paged out break, this isn't the
                    // next, but it means that we need to page
                    // in:
                    if (current.isPagedOut() && pageInElements) {
                        break;
                    }
                }

                if (current.sequence < sequence) {
                    return null;
                } else {
                    updateSequence(current.sequence);
                }
            } finally {
                // Don't hold on to a current ref if we aren't activated:
                if (!activated) {
                    updateCurrent(null);
                }
                updatePagingRefs();
            }
            if (current != null) {
                // Don't return elements that are loaded:
                if (!current.isLoaded()) {
                    return null;
                }

                // Return null if the element isn't yet paged in:
                if (pageInElements && current.isPagedOut()) {
                    return null;
                }
            }
            return current;
        }

        public long getCurrentSequeunce() {
            return sequence;
        }

        public int compareTo(Cursor<V> o) {
            if (o.sequence > sequence) {
                return -1;
            } else if (sequence > o.sequence) {
                return 1;
            } else {
                return 0;
            }
        }

        /**
         * @return true if their is a paged in, unacquired element that is ready
         *         for dispatch
         */
        public final boolean isReady() {
            if (!activated)
                return false;

            if (getNext() == null) {
                return false;
            }
            return true;
        }

        /**
         * 
         */
        public void onElementsLoaded() {
            if (readyListener != null && isReady()) {
                if (DEBUG) {
                    System.out.println(this + " notifying ready");
                }
                readyListener.onElementReady();
            }
        }

        /**
         * @param cursorReadyListener
         */
        public void setCursorReadyListener(CursorReadyListener cursorReadyListener) {
            readyListener = cursorReadyListener;
        }

        /**
         * @return true if the cursor has passed the end of the queue.
         */
        public boolean atEnd() {
            if (queue.isEmpty()) {
                return true;
            }

            if (sequence > limit) {
                return true;
            }

            QueueElement<V> tail = queue.getTail();
            // Can't be at the end if the tail isn't loaded:
            if (!tail.isLoaded()) {
                return false;
            }

            if (tail.getSequence() < this.sequence) {
                return true;
            }

            return false;
        }

        public String toString() {
            return "Cursor: " + sequence + " [" + name + "]";
        }

        /**
         * @param l
         */
        public void setLimit(long l) {
            limit = l;
        }
    }

    static class QueueElement<V> extends SortedLinkedListNode<QueueElement<V>> implements SubscriptionDeliveryCallback, SaveableQueueElement<V> {

        final long sequence;
        final long restoreBlock;
        final SharedQueue<?, V> queue;

        V elem;
        int size = -1;
        long expiration = -1;
        boolean redelivered = false;

        // When this drops to 0 we can page out the
        // element.
        int hardRefs = 0;

        // When this drops to 0 we can unload the element
        // providing it isn't in the load queue:
        int softRefs = 0;

        // Indicates whether this element is loaded or a placeholder:
        boolean loaded = true;

        // Indicates that we have requested a save for the element
        boolean savePending = false;
        // Indicates whether the element has been saved in the store.
        boolean saved = false;

        boolean deleted = false;
        boolean acquired = false;

        public QueueElement(V elem, long sequence, SharedQueue<?, V> queue) {
            this.elem = elem;
            this.queue = queue;
            if (elem != null) {
                size = queue.getElementSize(elem);
                expiration = queue.getElementExpiration(elem);
            }
            this.sequence = sequence;
            this.restoreBlock = sequence / RESTORE_BLOCK_SIZE;
        }

        /**
         * @return true if this element has been deleted:
         */
        public boolean isDeleted() {
            return deleted;
        }

        public QueueElement(RestoredElement<V> restored, SharedQueue<?, V> queue) throws Exception {
            this(restored.getElement(), restored.getSequenceNumber(), queue);
            this.size = restored.getElementSize();
            this.expiration = restored.getExpiration();
            saved = true;
            savePending = false;
        }

        @Override
        public final long getSequence() {
            return sequence;
        }

        public final void addHardRef() {
            hardRefs++;
            // Page in the element (providing it wasn't removed):
            if (elem == null && !deleted) {
                // If this is the first request for this
                // element request a load:
                if (hardRefs == 1) {
                    queue.getLoader().pageIn(this);
                }
            }
        }

        public final void releaseHardRef(IFlowController<QueueElement<V>> controller) {
            hardRefs--;
            if (hardRefs == 0) {
                unload(controller);
            }
            if (controller != null) {
                controller.elementDispatched(this);
            }
            assert hardRefs >= 0;
        }

        public final void addSoftRef() {
            softRefs++;
        }

        public final void releaseSoftRef() {
            softRefs--;
            if (softRefs == 0) {
                unload(null);
            }
            assert softRefs >= 0;
        }

        public final void setAcquired(boolean val) {
            this.acquired = val;
        }

        public final void acknowledge() {
            queue.acknowledge(this);
        }

        public final boolean delete() {
            if (!deleted) {
                deleted = true;
                if (isExpirable()) {
                    queue.getExpirator().elementRemoved(this);
                }

                if (saved) {
                    queue.getQueueStore().deleteQueueElement(queue.getDescriptor(), elem);
                }
                elem = null;
                unload(null);
                return true;
            }
            return false;
        }

        public final void unacquire(ISourceController<?> source) {
            acquired = false;
            if (isExpired()) {
                acknowledge();
            } else {
                // TODO reset all cursors beyond this sequence number
                // back to this element
                throw new UnsupportedOperationException("Not yet implemented");
            }
        }

        /**
         * Attempts to unlink this element from the queue
         */
        public final void unload(ISourceController<?> controller) {

            // Don't page out of there is a hard ref to the element
            // or if it is acquired (since we need the element
            // during delete:
            if (!deleted && (hardRefs > 0 || acquired)) {
                return;
            }

            // If the element didn't require persistence on enqueue, then
            // we'll need to save it now before paging it out.
            if (elem != null) {
                if (!deleted) {
                    if (!queue.getPersistencePolicy().isPersistent(elem)) {
                        save(controller, true);
                        if (DEBUG)
                            System.out.println("Paged out element: " + this);
                    }

                    // If save is pending don't unload until the save has
                    // completed
                    if (savePending) {
                        return;
                    }
                }

                elem = null;
            }

            QueueElement<V> next = getNext();
            QueueElement<V> prev = getPrevious();

            // See if we can unload this element, don't unload if we have a soft
            if (softRefs == 0) {
                // If deleted unlink this element from the queue, and link
                // together adjacent paged out entries:
                if (deleted) {
                    unlink();
                    // If both next and previous entries are unloaded,
                    // then collapse them:
                    if (next != null && prev != null && !next.isLoaded() && !prev.isLoaded()) {
                        next.unlink();
                    }
                }
                // Otherwise as long as the element isn't acquired we can unload
                // it. If it is acquired we keep the soft ref around
                else if (!acquired && queue.getPersistencePolicy().isPageOutPlaceHolders()) {

                    loaded = false;

                    // If the next element is unloaded
                    // replace it with this
                    if (next != null && !next.isLoaded()) {
                        next.unlink();
                    }

                    // If the previous elem is unloaded unlink this
                    // entry:
                    if (prev != null && !prev.isLoaded()) {
                        unlink();
                    }
                } else {
                    return;
                }
            }

            if (DEBUG)
                System.out.println("Unloaded element: " + this);

        }

        /**
         * Called to relink a loaded element after this element.
         * 
         * @param qe
         *            The paged in element to relink.
         * @throws Exception
         *             If there was an error creating the loaded element:
         */
        public final QueueElement<V> loadAfter(RestoredElement<V> re) throws Exception {

            QueueElement<V> ret = null;

            // See if this element represents the one being loaded:
            if (sequence == re.getSequenceNumber()) {
                ret = this;
                // If this isn't yet loaded
                if (!isLoaded()) {

                    loaded = true;
                    // Add a place holder to the next element if it's not
                    // already
                    // loaded:
                    if (re.getNextSequenceNumber() != -1) {
                        // Otherwise if our next pointer doesn't match the
                        // next restored number:
                        QueueElement<V> next = getNext();
                        if (next == null || next.sequence != re.getNextSequenceNumber()) {
                            next = new QueueElement<V>(null, re.getNextSequenceNumber(), queue);
                            next.loaded = false;
                            this.linkAfter(next);
                        }
                    }
                    this.size = re.getElementSize();
                    this.expiration = re.getExpiration();
                }

                // If we're paged out set our elem to the restored one:
                if (isPagedOut() && !deleted) {
                    this.elem = re.getElement();
                }
                saved = true;
                savePending = false;

            } else {
                ret = new QueueElement<V>(re, queue);
                // Otherwise simply link this element into the list:
                queue.queue.add(ret);
            }

            if (DEBUG)
                System.out.println("Loaded element: " + ret);
            return ret;
        }

        public final boolean isFirstInBlock() {
            if (isHeadNode()) {
                return true;
            } else {
                return prev.restoreBlock != restoreBlock;
            }
        }

        public final boolean isLastInBlock() {
            if (isTailNode()) {
                return queue.nextSequenceNumber / RESTORE_BLOCK_SIZE != restoreBlock;
            } else {
                return next.restoreBlock != restoreBlock;
            }
        }

        public final boolean isPagedOut() {
            return elem == null || !isLoaded();
        }

        public final boolean isLoaded() {
            return loaded;
        }

        public final boolean isAcquired() {
            return acquired || deleted;
        }

        public final long getExpiration() {
            return expiration;
        }

        public boolean isExpirable() {
            return expiration > 0;
        }

        public final boolean isExpired() {
            return expiration > 0 && System.currentTimeMillis() > expiration;
        }

        public final void save(ISourceController<?> controller, boolean delayable) {
            if (!saved) {
                queue.getQueueStore().persistQueueElement(this, controller, delayable);
                saved = true;

                // If paging is enabled we can't unload the element until it
                // is saved, otherwise there is no guarantee that it will be
                // in the store on a subsequent load requests because the
                // save is done asynchronously.
                if (queue.getPersistencePolicy().isPagingEnabled()) {
                    savePending = true;
                }
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.queue.QueueStore.SaveableQueueElement#getElement
         * ()
         */
        public final V getElement() {
            return elem;
        }

        /*
         * (non-Javadoc)
         * 
         * @seeorg.apache.activemq.queue.QueueStore.SaveableQueueElement#
         * getSequenceNumber()
         */
        public final long getSequenceNumber() {
            return sequence;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.queue.QueueStore.SaveableQueueElement#notifySave
         * ()
         */
        public void notifySave() {
            // TODO Refactor this:
            synchronized (queue.mutex) {
                // Unload if we haven't already:
                if (isLinked()) {
                    savePending = false;
                    unload(null);
                }
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.queue.QueueStore.SaveableQueueElement#requestNotify
         * ()
         */
        public boolean requestSaveNotify() {
            return savePending;
        }

        /*
         * (non-Javadoc)
         * 
         * @seeorg.apache.activemq.queue.QueueStore.SaveableQueueElement#
         * getQueueDescriptor()
         */
        public QueueDescriptor getQueueDescriptor() {
            return queue.getDescriptor();
        }

        public String toString() {
            return "QueueElement " + sequence + " loaded: " + loaded + " elem loaded: " + !isPagedOut() + " aquired: " + acquired;
        }

    }

    private class Expirator {

        private final Cursor<V> cursor = openCursor("Expirator-" + getResourceName(), false, false);
        // Number of expirable elements in the queue:
        private int count = 0;

        private boolean loaded = false;
        private long recoverySequence;
        private long lastRecoverdSequence;

        private static final int MAX_CACHE_SIZE = 500;
        private long uncachedMin = Long.MAX_VALUE;
        TreeMap<Long, HashSet<QueueElement<V>>> expirationCache = new TreeMap<Long, HashSet<QueueElement<V>>>();
        private int cacheSize = 0;

        public final boolean needsDispatch() {
            // If we have expiration candidates or are scanning the
            // queue request dispatch:
            return hasExpirables() || cursor.isReady();
        }

        public void start() {
            if (getEnqueuedCount() == 0) {
                loaded = true;
            } else {
                // Otherwise open a cursor and scan the queue up to
                // the current sequence number checking for expirable
                // elements:
                recoverySequence = nextSequenceNumber;
                cursor.reset(queue.getHead().sequence);
                cursor.activate();
                cursor.setCursorReadyListener(new CursorReadyListener() {
                    public void onElementReady() {
                        synchronized (mutex) {
                            notifyReady();
                        }
                    }
                });
            }
        }

        public void dispatch() {
            if (!needsDispatch()) {
                return;
            }
            long now = -1;
            // If their are uncached elements in the queue that are ready for
            // expiration
            // then scan the queue:
            if (!cursor.isActivated() && uncachedMin < (now = System.currentTimeMillis())) {
                uncachedMin = Long.MAX_VALUE;
                cursor.reset(0);
            }

            // Scan the queue looking for expirables:
            if (cursor.isReady()) {
                QueueElement<V> qe = cursor.getNext();
                while (qe != null) {
                    if (!loaded) {
                        if (qe.sequence < recoverySequence) {
                            lastRecoverdSequence = qe.sequence;
                            elementAdded(qe);
                        }
                        cursor.skip(qe);
                        qe = cursor.getNext();
                    } else {
                        if (qe.isExpired()) {
                            qe.acknowledge();
                        } else {
                            addToCache(qe);
                        }
                    }
                }

                // Finished loading:
                if (!loaded && cursor.getCurrentSequeunce() >= recoverySequence) {
                    if (DEBUG)
                        System.out.println(this + " Queue Load Complete");
                    loaded = true;
                    cursor.deactivate();
                } else if (cursor.atEnd()) {
                    cursor.deactivate();
                }
            }

            if (now == -1 && !expirationCache.isEmpty()) {
                now = System.currentTimeMillis();
            }

            // 
            while (!expirationCache.isEmpty()) {
                Entry<Long, HashSet<QueueElement<V>>> first = expirationCache.firstEntry();
                if (first.getKey() < now) {
                    for (QueueElement<V> qe : first.getValue()) {
                        qe.releaseSoftRef();
                        qe.acknowledge();
                    }
                }
            }
        }

        public void elementAdded(QueueElement<V> qe) {
            if (qe.isExpirable() && !qe.isDeleted()) {
                count++;
                if (qe.isExpired()) {
                    qe.acknowledge();
                } else {
                    addToCache(qe);
                }
            }
        }

        private void addToCache(QueueElement<V> qe) {
            // See if we should cache it, evicting entries if possible
            if (cacheSize >= MAX_CACHE_SIZE) {
                Entry<Long, HashSet<QueueElement<V>>> last = expirationCache.lastEntry();
                if (last.getKey() <= qe.expiration) {
                    // Keep track of the minimum uncached value:
                    if (qe.expiration < uncachedMin) {
                        uncachedMin = qe.expiration;
                    }
                    return;
                }

                // Evict the entry:
                Iterator<QueueElement<V>> i = last.getValue().iterator();
                removeFromCache(i.next());

                if (last.getKey() <= uncachedMin) {
                    // Keep track of the minimum uncached value:
                    uncachedMin = last.getKey();
                }
            }

            HashSet<QueueElement<V>> entry = new HashSet<QueueElement<V>>();
            entry.add(qe);
            qe.addSoftRef();
            cacheSize++;
            HashSet<QueueElement<V>> old = expirationCache.put(qe.expiration, entry);
            if (old != null) {
                old.add(qe);
                expirationCache.put(qe.expiration, old);
            }
        }

        private final void removeFromCache(QueueElement<V> qe) {
            HashSet<QueueElement<V>> last = expirationCache.get(qe.expiration);
            if (last != null && last.remove(qe.getSequenceNumber())) {
                cacheSize--;
                qe.releaseSoftRef();
                if (last.isEmpty()) {
                    expirationCache.remove(qe.sequence);
                }
            }
        }

        public void elementRemoved(QueueElement<V> qe) {
            // While loading, ignore elements that we haven't been seen yet.
            if (!loaded && qe.sequence < recoverySequence && qe.sequence > lastRecoverdSequence) {
                return;
            }

            if (qe.isExpirable()) {
                count--;
                removeFromCache(qe);
                assert count > 0;
            }
        }

        public final boolean hasExpirables() {
            if (count == 0) {
                return false;
            } else {
                long now = System.currentTimeMillis();
                if (now > uncachedMin) {
                    return true;
                } else if (!expirationCache.isEmpty()) {
                    return now > expirationCache.firstKey();
                }

                return false;
            }
        }

        public String toString() {
            return "Expirator for " + SharedQueue.this + " expirable " + count;
        }
    }

    /**
     * Handles paging in of elements from the store.
     * 
     * If the queue's memory limit is greater than it's size this class -Does
     * the initial load of messages recovered from the queue. -Handles updating
     * redelivered status of elements.
     * 
     * If the queue's memory limit is less than the queue size then this class
     * tracks cursor activity in the queue, loading/unloading elements into
     * memory as they are needed.
     * 
     * @author cmacnaug
     */
    private class ElementLoader implements RestoreListener<V> {

        private LinkedList<QueueStore.RestoredElement<V>> fromDatabase = new LinkedList<QueueStore.RestoredElement<V>>();
        private final HashMap<Long, HashSet<Cursor<V>>> requestedBlocks = new HashMap<Long, HashSet<Cursor<V>>>();
        private final HashSet<Cursor<V>> pagingCursors = new HashSet<Cursor<V>>();

        private boolean loadOnRequest = false;
        private Cursor<V> recoveryCursor = null;

        public final void start() {

            // If paging is enabled and we don't keep placeholders in memory
            // then we load on
            // request.
            if (persistencePolicy.isPagingEnabled() && persistencePolicy.isPageOutPlaceHolders()) {
                loadOnRequest = true;
            } else {
                loadOnRequest = false;
                if (getEnqueuedCount() > 0) {
                    recoveryCursor = openCursor("Loader", false, false);
                    recoveryCursor.setLimit(nextSequenceNumber - 1);
                    recoveryCursor.activate();
                }
            }
        }

        public boolean inLoadQueue(QueueElement<V> queueElement) {
            return requestedBlocks.containsKey(queueElement.restoreBlock);
        }

        public final boolean isPageOutPlaceHolders() {
            return persistencePolicy.isPageOutPlaceHolders();
        }

        /**
         * @param queueElement
         */
        public void pageIn(QueueElement<V> qe) {
            store.restoreQueueElements(queueDescriptor, false, qe.sequence, qe.sequence, 1, this);
        }

        public final Collection<Cursor<V>> getActiveCursors(QueueElement<V> qe) {
            return requestedBlocks.get(qe.getSequence());
        }

        public void reserveBlock(Cursor<V> cursor, long block) {
            HashSet<Cursor<V>> cursors = requestedBlocks.get(block);
            boolean load = recoveryCursor != null && cursor == recoveryCursor;

            if (cursors == null) {
                cursors = new HashSet<Cursor<V>>();
                requestedBlocks.put(block, cursors);
                load |= loadOnRequest;
            }
            cursors.add(cursor);

            if (load) {
                // Max sequence number is the end of this restoreBlock:
                long firstSequence = block * RESTORE_BLOCK_SIZE;
                long maxSequence = block * RESTORE_BLOCK_SIZE + RESTORE_BLOCK_SIZE - 1;
                // Don't pull in more than is paged out:
                // int maxCount = Math.min(element.pagedOutCount,
                // RESTORE_BLOCK_SIZE);
                int maxCount = RESTORE_BLOCK_SIZE;
                if (DEBUG)
                    System.out.println(cursor + " requesting restoreBlock:" + block + " from " + firstSequence + " to " + maxSequence + " max: " + maxCount + " queueMax: " + nextSequenceNumber);

                // If paging is enabled only pull in queue records, don't
                // bring
                // in the payload.
                // Each active cursor will have to pull in messages based on
                // available memory.
                store.restoreQueueElements(queueDescriptor, persistencePolicy.isPagingEnabled(), firstSequence, maxSequence, maxCount, this);
            }
        }

        public void releaseBlock(Cursor<V> cursor, long block) {
            HashSet<Cursor<V>> cursors = requestedBlocks.get(block);
            if (cursors == null) {
                if (true || DEBUG)
                    System.out.println(this + " removeBlockInterest " + block + ", no cursors" + cursor);
            } else {
                if (cursors.remove(cursor)) {
                    if (cursors.isEmpty()) {
                        requestedBlocks.remove(block);
                        // If this is the last cursor active in this block
                        // unload the block:
                        if (persistencePolicy.isPagingEnabled()) {
                            QueueElement<V> qe = queue.upper(RESTORE_BLOCK_SIZE * block, true);
                            while (qe != null && qe.restoreBlock == block) {
                                QueueElement<V> next = qe.getNext();
                                qe.unload(cursor.memoryController);
                                qe = next;
                            }
                        }
                    }
                } else {
                    if (DEBUG)
                        System.out.println(this + " removeBlockInterest, no cursor " + cursor);
                }
            }
        }

        /**
         * Adds elements loaded from the store to the queue.
         * 
         */
        final void processLoadRequests() {
            LinkedList<RestoredElement<V>> restoredElems = null;
            synchronized (fromDatabase) {
                if (fromDatabase.isEmpty()) {
                    return;
                }
                restoredElems = fromDatabase;
                fromDatabase = new LinkedList<RestoredElement<V>>();
            }

            // Process restored messages:
            if (restoredElems != null) {
                for (RestoredElement<V> restored : restoredElems) {
                    try {

                        QueueElement<V> qe = queue.lower(restored.getSequenceNumber(), true);

                        // If we don't have a paged out place holder for this
                        // element
                        // it must have been deleted:
                        if (qe == null) {
                            System.out.println("Loaded non-existent element: " + restored.getSequenceNumber());
                            continue;
                        }

                        qe = qe.loadAfter(restored);

                        // If paging isn't enabled then add a hard ref to the
                        // element,
                        // this will keep it around until it deleted
                        if (!persistencePolicy.isPagingEnabled()) {
                            qe.addHardRef();
                        }

                        // If we don't page out place holders we needn't track
                        // block
                        // interest once the block is loaded.
                        if (!persistencePolicy.isPageOutPlaceHolders()) {
                            requestedBlocks.remove(qe.restoreBlock);
                        }

                        if (DEBUG)
                            System.out.println(this + " Loaded loaded" + qe);

                    } catch (Exception ioe) {
                        ioe.printStackTrace();
                        shutdown();
                    }
                }

                // Add restoring consumers back to trailing consumers:
                for (Cursor<V> paging : pagingCursors)
                    paging.onElementsLoaded();

                pagingCursors.clear();
            }

            // Advance the recovery cursor:
            if (recoveryCursor != null) {
                while (recoveryCursor.isReady()) {
                    QueueElement<V> qe = recoveryCursor.getNext();
                    if (recoveryCursor.atEnd()) {
                        recoveryCursor.deactivate();
                        recoveryCursor = null;
                        break;
                    }
                    recoveryCursor.skip(qe);

                }
            }
        }

        public final boolean hasRestoredMessages() {
            synchronized (fromDatabase) {
                return !fromDatabase.isEmpty();
            }
        }

        public void elementsRestored(Collection<RestoredElement<V>> msgs) {
            synchronized (fromDatabase) {
                fromDatabase.addAll(msgs);
            }
            synchronized (mutex) {
                notifyReady();
            }
        }

        public String toString() {
            return "QueueLoader " + SharedQueue.this;
        }
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

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.queue.IQueue#setExpirationMapper(org.apache.activemq
     * .util.Mapper)
     */
    public void setExpirationMapper(Mapper<Long, V> expirationMapper) {
        this.expirationMapper = expirationMapper;
    }

    public String toString() {
        return "SharedQueue: " + getResourceName();
    }

    @Override
    protected ISinkController<V> getSinkController(V elem, ISourceController<?> source) {
        return inputController;
    }

    public V poll() {
        throw new UnsupportedOperationException("poll not supported for shared queue");
    }

    public IFlowController<V> getFlowControler() {
        return inputController;
    }
}
