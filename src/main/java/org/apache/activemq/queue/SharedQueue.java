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

    private final SortedLinkedList<QueueElement> queue = new SortedLinkedList<QueueElement>();
    private Mapper<K, V> keyMapper;

    private final ElementLoader loader;
    private Cursor sharedCursor;
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
    private final FlowController<V> sizeController;
    private final IFlowSizeLimiter<V> sizeLimiter;

    // Default cursor memory limit:
    private static final long DEFAULT_MEMORY_LIMIT = 10;

    private int totalQueueCount;

    private boolean initialized = false;
    private boolean started = false;

    private Mapper<Long, V> expirationMapper;
    private final Expirator expirator = new Expirator();

    public SharedQueue(String name, IFlowSizeLimiter<V> limiter) {
        this(name, limiter, null);
    }

    SharedQueue(String name, IFlowSizeLimiter<V> limiter, Object mutex) {
        super(name);
        this.mutex = mutex == null ? new Object() : mutex;

        flow = new Flow(getResourceName(), false);
        queueDescriptor = new QueueStore.QueueDescriptor();
        queueDescriptor.setQueueName(new AsciiBuffer(super.getResourceName()));
        queueDescriptor.setQueueType(QueueDescriptor.SHARED);
        this.sizeLimiter = limiter;

        this.sizeController = new FlowController<V>(getFlowControllableHook(), flow, limiter, this.mutex);
        sizeController.useOverFlowQueue(false);
        super.onFlowOpened(sizeController);

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

                sharedCursor = new Cursor(queueDescriptor.getQueueName().toString(), true, true);

                // Initialize counts:
                nextSequenceNumber = sequenceMax + 1;
                if (count > 0) {
                    sizeLimiter.add(count, size);
                    totalQueueCount = count;
                    // Add a paged out placeholder:
                    QueueElement qe = new QueueElement(null, sequenceMin);
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

    public void flowElemAccepted(ISourceController<V> source, V elem) {

        synchronized (mutex) {

            if (!initialized) {
                throw new IllegalStateException("Not able to use uninitialized queue: " + getResourceName());
            }

            // Create a new queue element with the next sequence number:
            QueueElement qe = new QueueElement(elem, nextSequenceNumber++);

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
            // Check with the loader to see if it needs to be paged out:
            loader.elementAdded(qe, source);
            expirator.elementAdded(qe);

            // Request dispatch for the newly enqueued element.
            // TODO consider optimizing to do direct dispatch?
            // It might be better if the dispatcher itself provided
            // this for cases where the caller is on the same dispatcher
            if (isDispatchReady()) {
                notifyReady();
            }
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
                QueueElement next = sharedCursor.getNext();
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
        final Cursor cursor;

        SubscriptionContext(Subscription<V> target) {
            this.sub = target;
            this.cursor = new Cursor(target.toString(), !sub.isBrowser(), true);
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

            QueueElement next = cursor.getNext();
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

        public final int offer(QueueElement qe) {

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

            // If the sub doesn't remove on dispatch set an ack listener:
            SubscriptionDeliveryCallback callback = sub.isRemoveOnDispatch() ? null : qe;

            // See if the sink has room:
            if (sub.offer(qe.elem, this, callback)) {
                if (!sub.isBrowser()) {
                    qe.setAcquired(this);

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

    class Cursor implements Comparable<Cursor> {

        private CursorReadyListener readyListener;

        private final String name;
        private boolean activated = false;;

        // The next element for this cursor, always non null
        // if activated, unless no element available:
        QueueElement current = null;
        // The current sequence number for this cursor,
        // used when inactive or pointing to an element
        // sequence number beyond the queue's limit.
        long sequence = -1;

        // The cursor is holding references for all
        // elements between first and last inclusive:
        QueueElement firstRef = null;
        QueueElement lastRef = null;
        // This is set to the last block that for which
        // we have requested a load:
        long lastBlockRequest = -1;

        // Each cursor can optionally be memory limited
        // When the limiter is set the cursor is able to
        // keep as many elements in memory as its limiter
        // allows.
        private final IFlowSizeLimiter<QueueElement> memoryLimiter;
        private final IFlowController<QueueElement> memoryController;

        // Indicates whether this cursor skips acquired elements
        private final boolean skipAcquired;
        // Indicates whether this cursor will page in elements
        // 
        private final boolean pageInElements;

        public Cursor(String name, boolean skipAcquired, boolean pageInElements) {
            this.name = name;
            this.skipAcquired = skipAcquired;
            this.pageInElements = pageInElements;

            // Set up a limiter if this cursor pages in elements, and memory
            // limit is less than the queue size:
            if (pageInElements && persistencePolicy.isPagingEnabled() && DEFAULT_MEMORY_LIMIT < sizeLimiter.getCapacity()) {
                memoryLimiter = new SizeLimiter<QueueElement>(DEFAULT_MEMORY_LIMIT, DEFAULT_MEMORY_LIMIT) {
                    public int getElementSize(QueueElement qe) {
                        return qe.size;
                    };
                };

                memoryController = new FlowController<QueueElement>(null, flow, memoryLimiter, mutex) {
                    @Override
                    public IFlowResource getFlowResource() {
                        return SharedQueue.this;
                    }
                };
            } else {
                memoryLimiter = null;
                memoryController = null;
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
        public final boolean offer(QueueElement qe, ISourceController<?> controller) {
            if (activated && memoryLimiter != null) {
                getNext();
                if (lastRef != null) {
                    // Return true if we absorbed it:
                    if (qe.sequence <= lastRef.sequence && qe.sequence >= firstRef.sequence) {
                        return true;
                    }
                    // If our last ref is close to this one reserve the element
                    else if (qe.getPrevious() == lastRef) {
                        return addCursorRef(qe, controller);
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
            current = null;
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
                if (persistencePolicy.isPageOutPlaceHolders()) {
                    loader.releaseBlock(this, lastBlockRequest);
                }

                lastBlockRequest = -1;

                // Let go of our current ref:
                current = null;
                activated = false;
            }
        }

        /**
         * Makes sure elements are paged in
         */
        private final void updatePagingRefs() {
            if (!activated)
                return;

            if (pageInElements && memoryLimiter != null) {

                // Release memory references up to our sequence number
                while (firstRef != null && firstRef.getSequence() < sequence) {
                    boolean lastInBlock = firstRef.isLastInBlock();
                    QueueElement next = firstRef.getNext();
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
                QueueElement next = null;
                if (lastRef == null) {
                    next = current;
                } else {
                    next = lastRef.getNext();
                }

                while (next != null && !memoryLimiter.getThrottled()) {
                    if (!addCursorRef(next, null)) {
                        break;
                    }
                    next = lastRef.getNext();
                }
            }
            // Otherwise we still need to ensure the block has been loaded:
            else if (current != null && !current.isLoaded()) {
                if (lastBlockRequest != current.restoreBlock) {
                    if (persistencePolicy.isPageOutPlaceHolders()) {
                        loader.releaseBlock(this, lastBlockRequest);
                    }
                    loader.loadBlock(this, current.restoreBlock);
                    lastBlockRequest = current.restoreBlock;
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
        private final boolean addCursorRef(QueueElement qe, ISourceController<?> controller) {
            // Make sure we have requested the block:
            if (qe.restoreBlock != lastBlockRequest) {
                lastBlockRequest = qe.restoreBlock;
                if (DEBUG)
                    System.out.println(this + " requesting block:" + lastBlockRequest);
                loader.loadBlock(this, lastBlockRequest);
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
            if (DEBUG && sequence > nextSequenceNumber) {
                new Exception(this + "cursor overflow").printStackTrace();
            }
        }

        /**
         * Sets the cursor to the next sequence number after the provided
         * element:
         */
        public final void skip(QueueElement elem) {
            QueueElement next = elem.isLinked() ? elem.getNext() : null;

            if (next != null) {
                updateSequence(next.sequence);
                if (activated) {
                    current = next;
                }
            } else {
                current = null;
                updateSequence(sequence + 1);
            }
            updatePagingRefs();
        }

        /**
         * @return the next available element or null if one is not currently
         *         available.
         */
        public final QueueElement getNext() {

            try {
                if (queue.isEmpty() || queue.getTail().sequence < sequence) {
                    current = null;
                    return null;
                }

                if (queue.getTail().sequence == sequence) {
                    current = queue.getTail();
                }

                // Get a pointer to the next element
                if (current == null || !current.isLinked()) {
                    current = queue.upper(sequence, true);
                    if (current == null) {
                        return null;
                    }
                }

                // Skip removed elements (and acquired ones if requested)
                while ((skipAcquired && current.isAcquired()) || current.isDeleted()) {
                    QueueElement last = current;
                    current = current.getNext();

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
                    current = null;
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

        public int compareTo(Cursor o) {
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
                System.out.println(this + " notifying ready");
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
            // TODO Auto-generated method stub
            if (queue.isEmpty()) {
                return true;
            }

            QueueElement tail = queue.getTail();
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
    }

    class QueueElement extends SortedLinkedListNode<QueueElement> implements SubscriptionDeliveryCallback, SaveableQueueElement<V> {

        final long sequence;
        final long restoreBlock;

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
        SubscriptionContext owner;

        public QueueElement(V elem, long sequence) {
            this.elem = elem;

            if (elem != null) {
                size = sizeLimiter.getElementSize(elem);
                expiration = expirationMapper.map(elem);
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

        public QueueElement(RestoredElement<V> restored) throws Exception {
            this(restored.getElement(), restored.getSequenceNumber());
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
                    loader.pageIn(this);
                }
            }
        }

        public final void releaseHardRef(IFlowController<QueueElement> controller) {
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

        public final void setAcquired(SubscriptionContext owner) {
            this.owner = owner;
        }

        public final void acknowledge() {
            synchronized (mutex) {
                delete();
            }
        }

        public final void delete() {
            if (!deleted) {
                deleted = true;
                owner = null;
                totalQueueCount--;
                if (isExpirable()) {
                    expirator.elementRemoved(this);
                }
                sizeController.elementDispatched(elem);
                if (saved) {
                    store.deleteQueueElement(queueDescriptor, elem);
                }
                elem = null;
                unload(null);
            }
        }

        public final void unacquire(ISourceController<?> source) {
            owner = null;
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

            // Can't unlink if there is a cursor ref, the cursor
            // needs this element to decrement it's limiter space
            if (hardRefs > 0) {
                return;
            }

            // If the element didn't require persistence on enqueue, then
            // we'll need to save it now before paging it out.
            // Note that we don't page out the element if it has an owner
            // because we need the element when we issue the delete.
            if (owner == null && elem != null && !persistencePolicy.isPersistent(elem)) {
                save(controller, true);
                if (DEBUG)
                    System.out.println("Paged out element: " + this);
                elem = null;
            }

            // If save is pending don't unload until the save has completed
            if (savePending) {
                return;
            }

            QueueElement next = getNext();
            QueueElement prev = getPrevious();

            // See if we can unload this element:
            // Don't unload the element if it is:
            // -Has an owner (we keep the element in memory so we don't
            // forget about the owner).
            // -If there are soft references to it
            // -Or it is in the load queue
            if (owner == null && softRefs == 0 && !loader.inLoadQueue(this)) {
                // If deleted unlink this element from the queue, and link
                // together adjacent paged out entries:
                if (deleted) {
                    unlink();
                    // If both next and previous entries are unloaded,
                    // then collapse them:
                    if (next != null && prev != null && !next.isLoaded() && !prev.isLoaded()) {
                        next.unlink();
                    }
                } else {

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
        public final QueueElement loadAfter(RestoredElement<V> re) throws Exception {

            QueueElement ret = null;

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
                        QueueElement next = getNext();
                        if (next == null || next.sequence != re.getNextSequenceNumber()) {
                            next = new QueueElement(null, re.getNextSequenceNumber());
                            next.loaded = false;
                            this.linkAfter(next);
                        }
                    }
                    this.size = re.getElementSize();
                }

                // If we're paged out set our elem to the restored one:
                if (isPagedOut() && !deleted) {
                    this.elem = re.getElement();
                }
                saved = true;
                savePending = false;

            } else {
                ret = new QueueElement(re);
                // Otherwise simply link this element into the list:
                queue.add(ret);
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
                return nextSequenceNumber / RESTORE_BLOCK_SIZE != restoreBlock;
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
            return owner != null || deleted;
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
                store.persistQueueElement(this, controller, delayable);
                saved = true;

                // If paging is enabled we can't unload the element until it
                // is saved, otherwise there is no guarantee that it will be
                // in the store on a subsequent load requests because the
                // save is done asynchronously.
                if (persistencePolicy.isPagingEnabled()) {
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
            synchronized (mutex) {
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
            return queueDescriptor;
        }

        public String toString() {
            return "QueueElement " + sequence + " loaded: " + loaded + " elem loaded: " + !isPagedOut() + " owner: " + owner;
        }

    }

    private class Expirator {

        private final Cursor cursor = new Cursor("Expirator", false, false);
        // Number of expirable elements in the queue:
        private int count = 0;

        private boolean loaded = false;
        private long recoverySequence;
        private long lastRecoverdSequence;

        private static final int MAX_CACHE_SIZE = 500;
        private long uncachedMin = Long.MAX_VALUE;
        TreeMap<Long, HashSet<QueueElement>> expirationCache = new TreeMap<Long, HashSet<QueueElement>>();
        private int cacheSize = 0;

        public final boolean needsDispatch() {
            // If we have expiration candidates or are scanning the
            // queue request dispatch:
            return hasExpirationCandidates() || cursor.isReady();
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
                QueueElement qe = cursor.getNext();
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
                    System.out.println(this + " Queue Load Complete");
                    loaded = true;
                    cursor.deactivate();
                } else if (cursor.atEnd()) {
                    cursor.deactivate();
                }
            }

            if (now == -1) {
                now = System.currentTimeMillis();
            }

            // 
            while (!expirationCache.isEmpty()) {
                Entry<Long, HashSet<QueueElement>> first = expirationCache.firstEntry();
                if (first.getKey() < now) {
                    for (QueueElement qe : first.getValue()) {
                        qe.releaseSoftRef();
                        qe.acknowledge();
                    }
                }
            }
        }

        public void elementAdded(QueueElement qe) {
            if (qe.isExpirable() && !qe.isDeleted()) {
                count++;
                if (qe.isExpired()) {
                    qe.acknowledge();
                } else {
                    addToCache(qe);
                }
            }
        }

        private void addToCache(QueueElement qe) {
            // See if we should cache it, evicting entries if possible
            if (cacheSize >= MAX_CACHE_SIZE) {
                Entry<Long, HashSet<QueueElement>> last = expirationCache.lastEntry();
                if (last.getKey() <= qe.expiration) {
                    // Keep track of the minimum uncached value:
                    if (qe.expiration < uncachedMin) {
                        uncachedMin = qe.expiration;
                    }
                    return;
                }

                // Evict the entry:
                Iterator<QueueElement> i = last.getValue().iterator();
                removeFromCache(i.next());

                if (last.getKey() <= uncachedMin) {
                    // Keep track of the minimum uncached value:
                    uncachedMin = last.getKey();
                }
            }

            HashSet<QueueElement> entry = new HashSet<QueueElement>();
            entry.add(qe);
            qe.addSoftRef();
            cacheSize++;
            HashSet<QueueElement> old = expirationCache.put(qe.expiration, entry);
            if (old != null) {
                old.add(qe);
                expirationCache.put(qe.expiration, old);
            }
        }

        private final void removeFromCache(QueueElement qe) {
            HashSet<QueueElement> last = expirationCache.get(qe.expiration);
            if (last != null && last.remove(qe.getSequenceNumber())) {
                cacheSize--;
                qe.releaseSoftRef();
                if (last.isEmpty()) {
                    expirationCache.remove(qe.sequence);
                }
            }
        }

        public void elementRemoved(QueueElement qe) {
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

        public boolean hasExpirationCandidates() {
            return !loaded || hasExpirables();
        }

        public boolean hasExpirables() {
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
        private final HashMap<Long, HashSet<Cursor>> requestedBlocks = new HashMap<Long, HashSet<Cursor>>();
        private final HashSet<Cursor> pagingCursors = new HashSet<Cursor>();

        public boolean inLoadQueue(QueueElement queueElement) {
            return requestedBlocks.containsKey(queueElement.restoreBlock);
        }

        /**
         * @param queueElement
         */
        public void pageIn(QueueElement qe) {
            store.restoreQueueElements(queueDescriptor, false, qe.sequence, qe.sequence, 1, this);
        }

        /**
         * Must be called after an element is added to the queue to enforce
         * memory limits
         * 
         * @param elem
         *            The added element:
         * @param source
         *            The source of the message
         */
        public final void elementAdded(QueueElement qe, ISourceController<V> source) {

            if (persistencePolicy.isPagingEnabled()) {

                // Check with the shared cursor to see if it is willing to
                // absorb the element. If so that's good enough.
                if (sharedCursor.offer(qe, source)) {
                    return;
                }

                // Otherwise check with any other open cursor to see if
                // it can take the element:
                HashSet<Cursor> active = requestedBlocks.get(qe.sequence);

                // If there are none, unload the element:
                if (active == null) {
                    qe.unload(source);
                    return;
                }

                // See if a cursor is willing to hang on to the
                // element:
                boolean accepted = false;
                for (Cursor cursor : active) {
                    // Already checked the shared cursor above:
                    if (cursor == sharedCursor) {
                        continue;
                    }

                    if (cursor.offer(qe, source)) {
                        accepted = true;
                    }
                }

                // If no cursor accepted it, then page out the element:
                // keeping the element loaded.
                if (!accepted) {
                    qe.unload(source);
                }
            }
        }

        // Updates memory when an element is loaded from the database:
        private final void elementLoaded(QueueElement qe) {
            // TODO track the rate of loaded elements vs those that
            // are added to the queue. We'll want to throttle back
            // enqueueing sources to a rate less than the restore
            // rate so we can stay out of the store.
        }

        public void loadBlock(Cursor cursor, long block) {
            HashSet<Cursor> cursors = requestedBlocks.get(block);
            if (cursors == null) {
                cursors = new HashSet<Cursor>();
                requestedBlocks.put(block, cursors);

                // Max sequence number is the end of this restoreBlock:
                long firstSequence = block * RESTORE_BLOCK_SIZE;
                long maxSequence = block * RESTORE_BLOCK_SIZE + RESTORE_BLOCK_SIZE - 1;
                // Don't pull in more than is paged out:
                // int maxCount = Math.min(element.pagedOutCount,
                // RESTORE_BLOCK_SIZE);
                int maxCount = RESTORE_BLOCK_SIZE;
                if (DEBUG)
                    System.out.println(cursor + " requesting restoreBlock:" + block + " from " + firstSequence + " to " + maxSequence + " max: " + maxCount + " queueMax: " + nextSequenceNumber);

                // If paging is enabled only pull in queue records, don't bring
                // in the payload.
                // Each active cursor will have to pull in messages based on
                // available memory.
                store.restoreQueueElements(queueDescriptor, persistencePolicy.isPagingEnabled(), firstSequence, maxSequence, maxCount, this);
            }
            cursors.add(cursor);
        }

        public void releaseBlock(Cursor cursor, long block) {
            // Don't do anything if we don't page out placeholders
            if (!persistencePolicy.isPageOutPlaceHolders()) {
                return;
            }
            HashSet<Cursor> cursors = requestedBlocks.get(block);
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
                            QueueElement qe = queue.upper(RESTORE_BLOCK_SIZE * block, true);
                            while (qe != null && qe.restoreBlock == block) {
                                QueueElement next = qe.getNext();
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

                        QueueElement qe = queue.lower(restored.getSequenceNumber(), true);

                        // If we don't have a paged out place holder for this
                        // element
                        // it must have been deleted:
                        if (qe == null) {
                            System.out.println("Loaded non-existent element: " + restored.getSequenceNumber());
                            continue;
                        }

                        qe = qe.loadAfter(restored);
                        elementLoaded(qe);

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
                for (Cursor paging : pagingCursors)
                    paging.onElementsLoaded();

                pagingCursors.clear();
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
        return sizeController;
    }

    public V poll() {
        throw new UnsupportedOperationException("poll not supported for shared queue");
    }

    public IFlowController<V> getFlowControler() {
        return sizeController;
    }
}
