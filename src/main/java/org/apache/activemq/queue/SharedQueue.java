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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

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
    private static final int RESTORE_BLOCK_SIZE = 50;

    private static final int ACCEPTED = 0;
    private static final int NO_MATCH = 1;
    private static final int DECLINED = 2;

    private final SortedLinkedList<QueueElement> queue = new SortedLinkedList<QueueElement>();
    private Mapper<K, V> keyMapper;

    private final ElementLoader loader;
    private final Cursor liveCursor;
    private QueueStore<K, V> store;
    private long nextSequenceNumber = 0;

    // Open consumers:
    private final HashMap<Subscription<V>, SubscriptionContext> consumers = new HashMap<Subscription<V>, SubscriptionContext>();

    // Consumers that are operating against the live cursor:
    private final LinkedNodeList<SubscriptionContext> liveConsumers = new LinkedNodeList<SubscriptionContext>();

    // Browsing subscriptions:
    private final LinkedNodeList<SubscriptionContext> liveBrowsers = new LinkedNodeList<SubscriptionContext>();

    // Consumers that are behind the live cursor
    private final LinkedNodeList<SubscriptionContext> trailingConsumers = new LinkedNodeList<SubscriptionContext>();

    // Consumers that are waiting for elements to be paged in:
    private final LinkedNodeList<SubscriptionContext> restoringConsumers = new LinkedNodeList<SubscriptionContext>();

    // Limiter/Controller for the size of the queue:
    private final FlowController<V> sizeController;
    private final IFlowSizeLimiter<V> sizeLimiter;

    // Memory Limiter and controller operate against the liveCursor.
    private static final long DEFAULT_MEMORY_LIMIT = 1536;
    private final IFlowSizeLimiter<QueueElement> memoryLimiter;
    private final FlowController<QueueElement> memoryController;
    private boolean useMemoryLimiter;

    private int totalQueueCount;

    private boolean initialized = false;
    private boolean started = false;

    public SharedQueue(String name, IFlowSizeLimiter<V> limiter) {
        this(name, limiter, null);
    }

    SharedQueue(String name, IFlowSizeLimiter<V> limiter, Object mutex) {
        super(name);
        liveCursor = new Cursor(name);
        this.mutex = mutex == null ? new Object() : mutex;

        flow = new Flow(getResourceName(), false);
        queueDescriptor = new QueueStore.QueueDescriptor();
        queueDescriptor.setQueueName(new AsciiBuffer(super.getResourceName()));
        queueDescriptor.setQueueType(QueueDescriptor.SHARED);
        this.sizeLimiter = limiter;

        this.sizeController = new FlowController<V>(getFlowControllableHook(), flow, limiter, this.mutex);
        sizeController.useOverFlowQueue(false);
        super.onFlowOpened(sizeController);

        if (DEFAULT_MEMORY_LIMIT < limiter.getCapacity()) {
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
            useMemoryLimiter = true;
        } else {
            useMemoryLimiter = false;
            memoryLimiter = null;
            memoryController = null;
        }

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
                // Initialize counts:
                nextSequenceNumber = sequenceMax + 1;
                if (count > 0) {
                    sizeLimiter.add(count, size);
                    totalQueueCount = count;
                    // Add a paged out placeholder:
                    QueueElement qe = new QueueElement(null, sequenceMin);
                    qe.pagedOutCount = count;
                    qe.pagedOutSize = size;
                    queue.add(qe);
                }

                initialized = true;
                liveCursor.reset(sequenceMin);

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
                liveCursor.getNext();
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
            if (!store.isFromStore(elem) && store.isElemPersistent(elem)) {
                try {
                    // TODO Revisit delayability criteria (basically,
                    // opened, unblocked receivers)
                    store.persistQueueElement(queueDescriptor, source, elem, qe.sequence, true);

                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            //Add it to our queue:
            queue.add(qe);
            totalQueueCount++;
            
            // Check with the loader to see if it needs to be paged out:
            loader.elementAdded(qe, source);

            // Request dispatch for the newly enqueued element.
            // TODO consider optimizing to do direct dispatch?
            // It might be better if the dispatcher itself provided
            // this for cases where the caller is on the same dispatcher
            if (isDispatchReady()) {
                notifyReady();
                // pollingDispatch();
            }
        }
    }

    public boolean pollingDispatch() {

        synchronized (mutex) {
            loader.processPageInRequests();

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
            /*
             * SubscriptionContext browser = liveBrowsers.getHead(); while
             * (browser != null) { SubscriptionContext nextBrowser =
             * browser.getNext(); browser.trailingDispatch(); if (nextBrowser !=
             * null) { browser = nextBrowser; } else { break; } }
             */

            // Process live consumers:
            QueueElement next = liveCursor.getNext();
            if (next != null && !next.isPagedOut()) {

                // See if there are any interested consumers:
                consumer = liveConsumers.getHead();
                boolean interested = false;

                find_consumer: while (consumer != null) {

                    SubscriptionContext nextConsumer = consumer.getNext();
                    switch (consumer.offer(next, liveCursor)) {
                    case ACCEPTED:
                        // Rotate list so this one is last next time:
                        liveConsumers.rotate();
                        interested = true;
                        break find_consumer;
                    case DECLINED:
                        interested = true;
                        break;
                    case NO_MATCH:
                        // Move on to the next consumer if this one didn't match
                        consumer = consumer.getNext();
                    }

                    consumer = nextConsumer;
                }

                // Advance the live cursor if this element was acquired
                // or there was no interest:
                if (!interested) {
                    liveCursor.skip(next);
                }

                // Request page in if the next element is paged out:
                next = liveCursor.getNext();
            }
            return isDispatchReady();
        }

    }

    public boolean isDispatchReady() {
        if (!initialized) {
            return false;
        }

        if (started) {
            // If we have live consumers, and an element ready for dispatch
            if (!liveConsumers.isEmpty() && liveCursor.isReady()) {
                return true;
            }

            // If there are ready trailing consumers:
            if (!trailingConsumers.isEmpty()) {
                return true;
            }

            // Might consider allowing browsers to browse
            // while stopped:
            if (!liveBrowsers.isEmpty()) {
                return true;
            }
        }

        // If there are restored messages ready for enqueue:
        if (loader.hasRestoredMessages()) {
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
            this.cursor = new Cursor(target.toString());
            this.sub = target;
            if (queue.isEmpty()) {
                cursor.reset(liveCursor.sequence);
            } else {
                cursor.reset(queue.getHead().sequence);
            }
        }

        public void start() {
            if (!isStarted) {
                isStarted = true;
                // If we're behind the live cursor add to the trailing consumer
                // list:
                if (updateCursor()) {
                    trailingConsumers.addLast(this);
                    notifyReady();
                }
            }
        }

        public void stop() {
            // If started remove this from any dispatch list
            if (isStarted) {
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

            // Update cursor to see if we're still behind
            if (updateCursor()) {
                QueueElement next = cursor.getNext();

                // If the next element is paged out,
                // Add to the list of restoring consumers
                if (next.pagedOutCount > 0) {
                    unlink();
                    restoringConsumers.addLast(this);
                } else {
                    offer(next, null);
                }
            }
        }

        /**
         * Advances the liveCursor to the next available element. And checks
         * whether or not this consumer is caught up to the live cursor
         * 
         * @return true if the cursor is behind the live cursor.
         */
        public final boolean updateCursor() {
            // Advance to the next available element:
            cursor.getNext();

            // Are we now live?
            if (cursor.compareTo(liveCursor) >= 0 || queue.isEmpty()) {
                cursor.deactivate();
                unlink();
                liveConsumers.addLast(this);
                return false;
            }
            return true;
        }

        public final int offer(QueueElement qe, Cursor live) {

            // If we are already passed this element return NO_MATCH:
            if (cursor.sequence > qe.sequence) {
                return NO_MATCH;
            }

            // If this element isn't matched, NO_MATCH:
            if (!sub.matches(qe.elem)) {
                cursor.skip(qe);
                return NO_MATCH;
            }

            // If the sub doesn't remove on dispatch set an ack listener:
            Subscription.SubscriptionDeliveryCallback callback = sub.isRemoveOnDispatch() ? null : qe;

            // See if the sink has room:
            if (sub.offer(qe.elem, this, callback)) {
                if (!sub.isBrowser()) {
                    qe.setAcquired(this);
                    loader.releaseMemory(qe);

                    // If this came from the live cursor, update it
                    // if we acquired the element:
                    if (live != null) {
                        live.skip(qe);
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
                trailingConsumers.addLast(this);
                notifyReady();
            }
        }

        public String toString() {
            return sub + ", " + cursor;
        }
    }

    class Cursor implements Comparable<Cursor> {

        private final String name;
        QueueElement current = null;
        long sequence = -1;
        boolean paging = false;
        long restoreBlock = -1;
        long requestedBlock = -1;

        public Cursor(String name) {
            this.name = name;
        }

        public final void reset(long sequence) {
            updateSequence(sequence);
            current = null;
        }

        public void deactivate() {
            if (paging) {
                loader.removeBlockInterest(this);
                requestedBlock = -1;
            }
            current = null;
        }

        private final void updateSequence(final long newSequence) {
            this.sequence = newSequence;
            // long newBlock = sequence / RESTORE_BLOCK_SIZE;
            // if (newBlock != restoreBlock) {
            // restoreBlock = newBlock;
            // }

            if (DEBUG && sequence > nextSequenceNumber) {
                new Exception(this + "cursor overflow").printStackTrace();
            }
        }

        private final void checkPageIn() {
            if (current != null && current.isPagedOut()) {
                if (current.restoreBlock != requestedBlock) {
                    if (paging) {
                        loader.removeBlockInterest(this);
                    }
                    requestedBlock = current.restoreBlock;
                    paging = true;
                    loader.addBlockInterest(this, current);
                }
            } else if (paging && requestedBlock != sequence / RESTORE_BLOCK_SIZE) {
                loader.removeBlockInterest(this);
                requestedBlock = -1;
                paging = false;
            }
        }

        /**
         * @return true if their is a paged in, unacquired element that is ready
         *         for dispatch
         */
        public final boolean isReady() {
            getNext();
            // Possible when the queue is empty
            if (current == null || current.isAcquired() || current.isPagedOut()) {
                return false;
            }
            return true;
        }

        /**
         * Sets the cursor to the next sequence number after the provided
         * element:
         */
        public final void skip(QueueElement elem) {
            QueueElement next = elem.isLinked() ? elem.getNext() : null;
            if (next != null) {
                updateSequence(next.sequence);
                current = next;
            } else {
                current = null;
                updateSequence(sequence + 1);
            }
        }

        public final QueueElement getNext() {
            if (queue.isEmpty() || queue.getTail().sequence < sequence) {
                current = null;
                return null;
            }

            if (queue.getTail().sequence == sequence) {
                current = queue.getTail();
            }

            // Get a pointer to the next element (make sure
            // that our next pointer is linked, it could have
            // been paged out):
            if (current == null || !current.isLinked()) {
                current = queue.upper(sequence, true);
                if (current == null) {
                    return null;
                }
            }

            // Skip acquired elements:
            while (current.isAcquired()) {
                QueueElement last = current;
                current = current.getNext();

                // If the next element is null, increment our sequence
                // and return:
                if (current == null) {
                    updateSequence(last.getSequence() + 1);
                    return null;
                }

                // If we're paged out break, this isn't the
                // next, but it means that we need to page
                // in:
                if (current.isPagedOut()) {
                    break;
                }
            }

            if (current.sequence < sequence) {
                return null;
            } else {
                updateSequence(current.sequence);
            }
            checkPageIn();
            return current;
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

        public String toString() {
            return "Cursor: " + sequence + " [" + name + "]";
        }
    }

    class QueueElement extends SortedLinkedListNode<QueueElement> implements Subscription.SubscriptionDeliveryCallback {

        V elem;
        SubscriptionContext owner;
        final long sequence;
        long restoreBlock;

        // When a queue element is paged out, the first element
        // in a range of paged out elements keeps track of the count
        // and size of paged out elements.
        int pagedOutCount = 0;
        long pagedOutSize = 0;
        int size = 0;

        public QueueElement(V elem, long sequence) {
            this.elem = elem;
            if (elem != null) {
                size = sizeLimiter.getElementSize(elem);
            }
            this.sequence = sequence;
            this.restoreBlock = sequence / RESTORE_BLOCK_SIZE;
        }

        public void setAcquired(SubscriptionContext owner) {
            this.owner = owner;
            sizeController.elementDispatched(elem);
        }

        public final void acknowledge() {
            synchronized (mutex) {
                unlink();
                totalQueueCount--;
                if (isPagedOut()) {
                    return;
                } else if (store.isElemPersistent(elem) || store.isFromStore(elem)) {
                    store.deleteQueueElement(queueDescriptor, elem);
                }
            }
        }

        public final void unacquire(ISourceController<?> source) {
            // TODO reenqueue and update cursors back to this position.
            // If there are subscriptions with selectors this could get
            // tricky to avoid reevaluating already evaluated selectors.
            throw new UnsupportedOperationException();
        }

        /**
         * Pages this element out to free memory.
         * 
         * Memory is freed if upon the return of this call the passed in
         * sizeController was not blocked. If the sizeController was blocked
         * then memory is not freed until it is next unblocked.
         */
        private void pageOut(ISourceController<?> controller) {
            // See if we can page this out to save memory:
            //
            // Disqualifiers:
            // - If this is already paged out then nothing to do
            // - We don't page out acquired elements, the memory
            // is accounted for by the owner, and we keep them
            // in memory here, to make sure we don't try to pull
            // it back in for another consumer
            // - If there is a cursor active in this element's
            // restore block don't page out, memory is accounted
            // for in the cursor's sizeLimiter
            if (pagedOutCount > 0 || owner != null || loader.inLoadQueue(this)) {
                return;
            }

            // If the element is not persistent then we'll need to request a
            // save:
            if (!store.isFromStore(elem) && !store.isElemPersistent(elem)) {
                try {
                    store.persistQueueElement(queueDescriptor, controller, elem, sequence, false);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }

            pagedOutCount = 1;
            pagedOutSize = size;
            elem = null;

            // Collapse adjacent paged out elements:
            QueueElement next = getNext();
            QueueElement prev = getPrevious();
            // If the next element is paged out
            // replace it with this
            if (next != null && next.pagedOutCount > 0) {
                pagedOutCount += next.pagedOutCount;
                pagedOutSize += next.pagedOutSize;
                next.unlink();
            }
            // If the previous elem is paged out unlink this
            // entry:
            if (prev != null && prev.pagedOutCount > 0) {
                prev.pagedOutCount += pagedOutCount;
                prev.pagedOutSize += pagedOutSize;
                unlink();
            }

            if (DEBUG)
                System.out.println("Paged out element: " + this);
        }

        /**
         * Called to relink a paged in element after this element.
         * 
         * @param qe
         *            The paged in element to relink.
         */
        public QueueElement pagedIn(QueueElement qe, long nextSequence) {
            QueueElement ret = qe;
            // See if we have a pointer to a paged out element:
            if (sequence == qe.sequence) {
                // Already paged in? Shouldn't be.
                if (!isPagedOut()) {
                    throw new IllegalStateException("Can't page in an already paged in element");
                } else {
                    // Otherwise set this element to the paged in one
                    // and add a new QueueElement to hold any additional
                    // paged out elements:
                    elem = qe.elem;
                    size = qe.size;
                    pagedOutCount--;
                    if (pagedOutCount > 0) {
                        if (nextSequence == -1) {
                            throw new IllegalStateException("Shouldn't have paged out elements at the end of the queue");
                        }
                        qe = new QueueElement(null, nextSequence);
                        qe.pagedOutCount = pagedOutCount;
                        qe.pagedOutSize = pagedOutSize - size;
                        pagedOutCount = 0;
                        pagedOutSize = 0;
                        try {
                            this.linkAfter(qe);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    ret = this;
                }
            } else {
                // Otherwise simply link this element into the list:
                 queue.add(qe);
                // Decrement pagedOutCount counter of previous element:
                if (qe.prev != null && qe.prev.pagedOutCount > 0) {
                    if(qe.prev.pagedOutCount > 1)
                    {
                        throw new IllegalStateException("Skipped paged in element");    
                    }
                    pagedOutCount = qe.pagedOutCount - 1;
                    qe.prev.pagedOutCount = 0;
                    qe.prev.pagedOutSize = 0;
                }
            }

            if (DEBUG)
                System.out.println("Paged in element: " + this);

            return ret;
        }

        public boolean isPagedOut() {
            return elem == null;
        }

        public boolean isAcquired() {
            return owner != null;
        }

        public String toString() {
            return "QueueElement " + sequence + " pagedOutCount: " + pagedOutCount + " owner: " + owner;
        }

        @Override
        public long getSequence() {
            return sequence;
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
     * tracks cursor activity in the queue, loading elements into memory as they
     * are needed.
     * 
     * @author cmacnaug
     */
    private class ElementLoader implements RestoreListener<V> {

        private LinkedList<QueueStore.RestoredElement<V>> fromDatabase = new LinkedList<QueueStore.RestoredElement<V>>();
        private final HashMap<Long, HashSet<Cursor>> requestedBlocks = new HashMap<Long, HashSet<Cursor>>();

        public boolean inLoadQueue(QueueElement queueElement) {
            return requestedBlocks.containsKey(queueElement.restoreBlock);
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
            if (useMemoryLimiter) {
                if (!qe.isPagedOut()) {
                    memoryController.add(qe, source);

                    if (memoryLimiter.getThrottled()) {

                        qe.pageOut(memoryController);
                        // If we paged it out release memory:
                        if (qe.isPagedOut()) {
                            releaseMemory(qe);
                        }
                    }
                }
            }
        }

        //Updates memory when an element is loaded from the database:
        private final void elementLoaded(QueueElement qe) {
            if (useMemoryLimiter) {
                memoryController.add(qe, null);
            }
        }

        public final void releaseMemory(QueueElement qe) {
            if (useMemoryLimiter) {
                memoryController.elementDispatched(qe);
            }
        }

        public void addBlockInterest(Cursor cursor, QueueElement element) {
            HashSet<Cursor> cursors = requestedBlocks.get(cursor.requestedBlock);
            if (cursors == null) {
                cursors = new HashSet<Cursor>();
                requestedBlocks.put(cursor.requestedBlock, cursors);

                // Max sequence number is the end of this restoreBlock:
                long maxSequence = (cursor.requestedBlock * RESTORE_BLOCK_SIZE) + RESTORE_BLOCK_SIZE;
                // Don't pull in more than is paged out:
                int maxCount = Math.min(element.pagedOutCount, RESTORE_BLOCK_SIZE);
                if(DEBUG)
                    System.out.println(cursor + " requesting restoreBlock:" + cursor.requestedBlock + " from " + element.getSequence() + " to " + maxSequence + " max: " + maxCount);
                store.restoreQueueElements(queueDescriptor, element.getSequence(), maxSequence, maxCount, this);
            }
            cursors.add(cursor);
        }

        public void removeBlockInterest(Cursor cursor) {
            long block = cursor.requestedBlock;
            HashSet<Cursor> cursors = requestedBlocks.get(block);
            if (cursors == null) {
                if (DEBUG)
                    System.out.println(this + " removeBlockInterest, no consumers " + cursor);
            } else {
                if (cursors.remove(cursor)) {
                    if (cursors.isEmpty()) {
                        requestedBlocks.remove(cursor.requestedBlock);
                        //If this is the last cursor active in this block page out the block:
                        if(useMemoryLimiter)
                        {
                            QueueElement qe = queue.upper(RESTORE_BLOCK_SIZE * cursor.requestedBlock, true);
                            while(qe != null && qe.restoreBlock == block)
                            {
                                QueueElement next = qe.getNext();
                                if(!qe.isPagedOut())
                                {
                                    qe.pageOut(memoryController);
                                    // If we paged it out release memory:
                                    if (qe.isPagedOut()) {
                                        System.out.println(this + " removeBlockInterest, released memory for: " + this);
                                        releaseMemory(qe);
                                    }
                                    qe = next;
                                }
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
         * Returns loaded messages or null if none have been loaded.
         * 
         * @throws IOException
         */
        final void processPageInRequests() {
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
                // boolean trailingRestore = false;
                for (QueueStore.RestoredElement<V> restored : restoredElems) {
                    try {
                        V delivery = restored.getElement();
                        QueueElement qe = new QueueElement(delivery, restored.getSequenceNumber());
                        QueueElement lower = queue.lower(qe.sequence, true);
                        qe = lower.pagedIn(qe, restored.getNextSequenceNumber());
                        loader.elementLoaded(qe);

                    } catch (Exception ioe) {
                        ioe.printStackTrace();
                        shutdown();
                    }
                }

                // Add restoring consumers back to trailing consumers:
                if (!restoringConsumers.isEmpty()) {
                    trailingConsumers.addFirst(restoringConsumers);
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
            return "MsgRetriever " + SharedQueue.this;
        }
    }

    public void setKeyMapper(Mapper<K, V> keyMapper) {
        this.keyMapper = keyMapper;
    }

    public void setStore(QueueStore<K, V> store) {
        this.store = store;
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
