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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;

import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowController;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.queue.Subscription.SubscriptionDelivery;
import org.apache.activemq.util.Comparators;
import org.apache.activemq.util.Mapper;
import org.apache.activemq.util.TreeMap;
import org.apache.activemq.util.list.SortedLinkedList;
import org.apache.activemq.util.list.SortedLinkedListNode;

/**
 * @author cmacnaug
 * 
 */
public abstract class CursoredQueue<V> {

    private static final boolean DEBUG = false;

    private final SortedLinkedList<QueueElement<V>> queue = new SortedLinkedList<QueueElement<V>>();
    private HashSet<Cursor<V>> openCursors = new HashSet<Cursor<V>>();

    private long nextSequenceNumber = 0;
    private int totalQueueCount;

    //Dictates the chunk size of messages or place holders
    //pulled in from the database:
    private static final int RESTORE_BLOCK_SIZE = 1000;
    private final PersistencePolicy<V> persistencePolicy;
    private final Mapper<Long, V> expirationMapper;
    private final Expirator expirator;
    private final QueueStore<?, V> queueStore;
    private final ElementLoader loader;
    private final QueueDescriptor queueDescriptor;
    private final Object mutex;

    public CursoredQueue(PersistencePolicy<V> persistencePolicy, Mapper<Long, V> expirationMapper, Flow flow, QueueDescriptor queueDescriptor, QueueStore<?, V> store, Object mutex) {
        this.persistencePolicy = persistencePolicy;
        this.mutex = mutex;
        this.queueStore = store;
        this.queueDescriptor = queueDescriptor;
        this.expirationMapper = expirationMapper;
        loader = new ElementLoader();
        expirator = new Expirator();
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
        // Initialize counts:
        nextSequenceNumber = sequenceMax + 1;
        if (count > 0) {
            totalQueueCount = count;
            // Add a paged out placeholder:
            QueueElement<V> qe = new QueueElement<V>(null, sequenceMin, this);
            qe.loaded = false;
            queue.add(qe);
        }
    }

    protected abstract void requestDispatch();

    protected abstract Object getMutex();

    protected abstract void onElementRemoved(QueueElement<V> qe);

    protected abstract void onElementReenqueued(QueueElement<V> qe, ISourceController<?> controller);

    /**
     * Adds an element to the queue.
     * 
     * @param source
     * @param elem
     */
    public final void add(ISourceController<?> source, V elem) {
        // Create a new queue element with the next sequence number:
        QueueElement<V> qe = new QueueElement<V>(elem, nextSequenceNumber++, this);

        // Add it to the queue:
        queue.add(qe);
        totalQueueCount++;

        //Check with the loader to see if the element should be saved 
        //or paged out:
        loader.elementAdded(source, qe);

        // Register the element with the expirator:
        expirator.elementAdded(qe);
    }

    public void remove(long sequence) {
        QueueElement<V> qe = queue.lower(sequence, true);
        if (qe == null) {
            return;
        } else if (qe.getSequence() == sequence) {
            qe.acknowledge();
        }
        //Otherwise if the element is paged out, create a new
        //holder and mark it for deletion
        else if (persistencePolicy.isPageOutPlaceHolders()) {
            //FIXME, need to track this delete otherwise an in flight message restore
            //might load this element back in.
            getQueueStore().deleteQueueElement(new QueueElement<V>(null, sequence, this));
        }
    }

    /**
     * @return True if the queue needs dispatching.
     */
    public boolean needsDispatch() {
        return loader.needsDispatch() || expirator.needsDispatch();
    }

    public void start() {
        loader.start();
        expirator.start();
    }

    public void stop() {
    }

    /**
     * Should be called when #needsDispatch() returns true to do any
     * housekeeping required by the queue.
     */
    public final void dispatch() {
        expirator.dispatch();
        loader.dispatch();
    }

    protected long getElementExpiration(V elem) {
        return expirationMapper.map(elem);
    }

    protected abstract int getElementSize(V elem);

    /**
     * @return
     */
    final Expirator getExpirator() {
        return expirator;
    }

    /**
     * @return
     */
    final QueueStore<?, V> getQueueStore() {
        return queueStore;
    }

    /**
     * @return
     */
    final QueueDescriptor getDescriptor() {
        return queueDescriptor;
    }

    /**
     * @return
     */
    final ElementLoader getLoader() {
        return loader;
    }

    /**
     * @return
     */
    final PersistencePolicy<V> getPersistencePolicy() {
        return persistencePolicy;
    }

    /**
     * @return True if the queue is empty.
     */
    public boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * @return The first sequence number in the queue (or the next sequence
     *         number if it is empty)
     */
    public long getFirstSequence() {
        if (queue.isEmpty()) {
            return nextSequenceNumber;
        } else {
            return queue.getHead().sequence;
        }
    }

    /**
     * @return The number of elements in the queue.
     */
    public int getEnqueuedCount() {
        return totalQueueCount;
    }

    public interface CursorReadyListener {
        public void onElementReady();
    }

    public final Cursor<V> openCursor(String name, FlowController<QueueElement<V>> memoryController, boolean pageInElements, boolean skipAcquired) {

        FlowController<QueueElement<V>> controller = null;
        if (pageInElements && persistencePolicy.isPagingEnabled()) {
            if (memoryController == null) {
                throw new IllegalArgumentException("Memory controller required for paging enabled queue");
            }
            memoryController.useOverFlowQueue(false);
            controller = memoryController;
        } else {
            controller = null;
        }

        return new Cursor<V>(this, name, skipAcquired, pageInElements, controller);
    }

    static class Cursor<V> implements Comparable<Cursor<V>> {

        private CursorReadyListener readyListener;

        private final String name;
        private CursoredQueue<V> cQueue;
        private final CursoredQueue<V>.ElementLoader loader;
        private final SortedLinkedList<QueueElement<V>> queue;

        private boolean activated = false;;

        // The next element for this cursor, always non null
        // if activated, unless no element available:
        QueueElement<V> current = null;
        // The current sequence number for this cursor,
        // used when inactive or pointing to an element
        // sequence number beyond the queue's limit.
        long sequence = -1;

        //When an element on the queue is reenqueued, this
        //is set to indicate that the cursor should go back 
        //and consider the element:
        long reenqueueSequence = -1;

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

        private boolean throttleToMemory;

        private boolean paused;

        private Cursor(CursoredQueue<V> queue, String name, boolean skipAcquired, boolean pageInElements, IFlowController<QueueElement<V>> memoryController) {
            this.name = name;
            this.queue = queue.queue;
            this.loader = queue.loader;
            this.cQueue = queue;

            this.skipAcquired = skipAcquired;
            this.pageInElements = pageInElements;

            // Set up a limiter if this cursor pages in elements, and memory
            // limit is less than the queue size:
            if (pageInElements) {
                this.memoryController = memoryController;
            } else {
                this.memoryController = null;
            }
            cQueue.openCursors.add(this);
        }

        public void setThrottleToMemoryWhenActive(boolean throttleToMemory) {
            this.throttleToMemory = throttleToMemory;
        }

        /**
         * Offers a queue element to the cursor's memory limiter. The cursor
         * will return true if it is willing to keep the element in memory.
         * 
         * @param qe
         *            The element for which to check.
         * @return
         */
        public final boolean offer(QueueElement<V> qe, ISourceController<?> controller) {
            if (activated && memoryController != null) {
                // Get the next element which will page in as many elements as
                // we can:
                getNext();
                if (lastRef != null) {
                    // Return true if we absorbed it:
                    if (qe.sequence <= lastRef.sequence && qe.sequence >= firstRef.sequence) {
                        return true;
                    }
                    // If our last ref is close to this one reserve the element,
                    // and the
                    // cursor isn't paused
                    else if (!paused && throttleToMemory && qe.getPrevious() == lastRef) {
                        if (addCursorRef(qe, controller)) {
                            return true;
                        }
                    }
                }
                return false;
            }
            // Always accept an element if not memory limited providing we're
            // active:
            return !paused && activated && pageInElements;
        }

        public void close() {
            deactivate();
            cQueue.openCursors.remove(this);
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
         * Indicates that the cursor is paused. A paused cursor will still hold
         * on to memory references when paging is enabled, but will not will not
         * throttle sources, if the memory limit is exceeded.
         */
        public final void pause() {
            paused = true;
        }

        public final void resume() {
            paused = false;
        }

        /**
         * Updates the current ref. We keep a soft ref to the current to keep it
         * in the queue so that we can get at the next without a costly lookup.
         */
        private final void updateCurrent(QueueElement<V> qe) {
            if (qe == current) {
                return;
            }
            // NOTE it's important we reference this element
            // before unloading our current one since unloading
            // the current could result in the new one being
            // unloaded as well
            if (qe != null) {
                qe.addSoftRef();
            }

            if (current != null) {
                current.releaseSoftRef();
            }
            current = qe;
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

                if (reenqueueSequence != -1) {
                    reset(reenqueueSequence);
                    reenqueueSequence = -1;
                }

                if (atEnd()) {
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

        public void onElementUnacquired(QueueElement<V> qe) {
            if (qe.sequence < sequence) {
                if (reenqueueSequence >= 0) {
                    reenqueueSequence = Math.min(reenqueueSequence, qe.sequence);
                } else {
                    reenqueueSequence = qe.sequence;
                }
            }
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

            if (reenqueueSequence != -1) {
                return false;
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
         *            Set the highest sequence number to which this cursor can
         *            advance.
         */
        public void setLimit(long l) {
            limit = l;
        }
    }

    static class QueueElement<V> extends SortedLinkedListNode<QueueElement<V>> implements SubscriptionDelivery<V>, SaveableQueueElement<V> {

        final long sequence;
        final long restoreBlock;
        final CursoredQueue<V> queue;

        V elem;
        private int size = -1;
        long expiration = -1;
        boolean redelivered = false;

        // When this drops to 0 we can page out the
        // element.
        int hardRefs = 0;

        // When this drops to 0 we can unload the element
        // providing it isn't in the load queue:
        int softRefs = 0;

        // Indicates whether this element is loaded or a placeholder:
        // A place holder indicates that one or more elements is paged
        // out at and above this sequence number.
        boolean loaded = true;

        // Indicates that we have requested a save for the element
        boolean savePending = false;
        // Indicates whether the element has been saved in the store.
        boolean saved = false;

        private boolean deleted = false;
        private Subscription<V> owner = null;

        public QueueElement(V elem, long sequence, CursoredQueue<V> queue) {
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

        public QueueElement(RestoredElement<V> restored, CursoredQueue<V> queue) throws Exception {
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

        public final int getLimiterSize() {
            return size;
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

        public final void setAcquired(Subscription<V> owner) {
            this.owner = owner;
        }

        public final void acknowledge() {
            synchronized (queue.getMutex()) {
                delete();
            }
        }

        public void unacquire(ISourceController<?> source) {
            synchronized (queue.getMutex()) {
                if (owner != null) {
                    owner = null;
                    if (!deleted) {
                        redelivered = true;
                        //TODO need to account for this memory space, and check 
                        //load/unload:
                        for (Cursor<V> c : queue.openCursors) {
                            c.onElementUnacquired(this);
                        }
                    }
                }
                queue.requestDispatch();
            }
        }

        public final boolean delete() {
            if (!deleted) {
                deleted = true;
                if (isExpirable()) {
                    queue.getExpirator().elementRemoved(this);
                }

                if (saved) {
                    queue.getQueueStore().deleteQueueElement(this);
                }

                //FIXME need to track deletions when paging is enabled
                //otherwise an in process restore might reload the element
                elem = null;
                unload(null);

                queue.onElementRemoved(this);
                return true;
            }
            return false;
        }

        /**
         * Attempts to unlink this element from the queue
         */
        public final void unload(ISourceController<?> controller) {

            // Don't page out of there is a hard ref to the element
            // or if it is acquired (since we need the element
            // during delete:
            if (!deleted && (hardRefs > 0 || isAcquired())) {
                return;
            }

            // If the element didn't require persistence on enqueue, then
            // we'll need to save it now before paging it out.
            if (elem != null) {
                if (!deleted) {
                    if (!queue.getPersistencePolicy().isPersistent(elem)) {
                        save(controller, true);
                        if (DEBUG)
                            System.out.println("Paging out non-pers element: " + this);
                    }

                    // If save is pending don't unload until the save has
                    // completed
                    if (savePending) {
                        return;
                    }
                }

                if (DEBUG)
                    System.out.println("Paged out element: " + this);
                elem = null;
            }

            QueueElement<V> next = getNext();
            QueueElement<V> prev = getPrevious();

            // See if we can unload this element, don't unload if someone is
            // referencing it:
            if (softRefs == 0) {
                // If deleted unlink this element from the queue, and link
                // together adjacent paged out entries:
                if (deleted) {
                    loaded = false;
                    unlink();
                    // If both next and previous entries are unloaded,
                    // then collapse them:
                    if (next != null && prev != null && next.unlinkable() && !prev.isLoaded()) {
                        next.unlink();
                    }
                }
                // Otherwise as long as the element isn't acquired we can unload
                // it. If it is acquired we keep the soft ref arount to remember
                // that it is.
                else if (!isAcquired() && queue.getLoader().isPageOutPlaceHolders()) {

                    loaded = false;

                    // If the next element is unlinkable then collapse this
                    // one into it:
                    if (next != null && next.unlinkable()) {
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

        public final boolean isRedelivery() {
            return redelivered;
        }

        private boolean unlinkable() {
            return softRefs == 0 && !loaded;
        }

        /**
         * Called to relink a loaded element after this element.
         * 
         * @param qe
         *            The paged in element to relink.
         * @throws Exception
         *             If there was an error creating the loaded element:
         */
        public final QueueElement<V> loadAfter(RestoredElement<V> re, boolean hardRef, boolean softRef) throws Exception {

            QueueElement<V> ret = null;

            // See if this element represents the one being loaded:
            if (sequence == re.getSequenceNumber()) {
                ret = this;
                // If this isn't yet loaded
                if (!loaded) {

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
                    //TODO Need to add redelivery to the store:
                    //this.redelivered = re.getRedelivered();
                    // If the loader asked to add a soft ref do it:
                    if (softRef) {
                        addSoftRef();
                    }
                }

                // If we're paged out set our elem to the restored one:
                if (elem == null && !deleted) {
                    this.elem = re.getElement();
                    if (hardRef && elem != null) {
                        addHardRef();
                    }
                }
                saved = true;
                savePending = false;

            } else {
                ret = new QueueElement<V>(re, queue);
                // Add requested refs:
                if (softRef) {
                    ret.addSoftRef();
                }
                if (hardRef && ret.elem != null) {
                    ret.addHardRef();
                }
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
            return elem == null && !deleted;
        }

        public final boolean isLoaded() {
            return loaded;
        }

        public final boolean isAcquired() {
            return owner != null;
        }

        public Subscription<V> getOwner() {
            return owner;
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
            return "QueueElement " + sequence + " loaded: " + loaded + " elem loaded: " + !isPagedOut() + " owner: " + owner;
        }

        /*
         * (non-Javadoc)
         * 
         * @seeorg.apache.activemq.queue.Subscription.SubscriptionDelivery#
         * getSourceQueueRemovalKey()
         */
        public long getSourceQueueRemovalKey() {
            return sequence;
        }

    }

    private class Expirator {

        private final Cursor<V> cursor = openCursor("Expirator-" + queueDescriptor.getQueueName(), null, false, false);
        // Number of expirable elements in the queue:
        private int count = 0;

        private boolean loaded = false;
        private long recoverySequence;
        private long lastRecoverdSequence;

        private static final int MAX_CACHE_SIZE = 500;
        private long uncachedMin = Long.MAX_VALUE;
        TreeMap<Long, HashSet<QueueElement<V>>> expirationCache = new TreeMap<Long, HashSet<QueueElement<V>>>(Comparators.LONG_COMPARATOR);
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
                        requestDispatch();
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
                        expirationCache.remove(first.getKey());
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
            return "Expirator for " + CursoredQueue.this + " expirable " + count;
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

        private LinkedList<RestoredElement<V>> fromDatabase = new LinkedList<RestoredElement<V>>();
        private final HashMap<Long, HashSet<Cursor<V>>> reservedBlocks = new HashMap<Long, HashSet<Cursor<V>>>();
        private final HashSet<Cursor<V>> pagingCursors = new HashSet<Cursor<V>>();

        private boolean pageOutPlaceHolders = false;
        private Cursor<V> recoveryCursor = null;

        public final void start() {

            // If paging is enabled and we don't keep placeholders in memory
            // then we load on request.
            if (persistencePolicy.isPagingEnabled() && persistencePolicy.isPageOutPlaceHolders()) {
                pageOutPlaceHolders = true;
            } else {
                pageOutPlaceHolders = false;
                if (getEnqueuedCount() > 0) {
                    recoveryCursor = openCursor("Loader", null, false, false);
                    recoveryCursor.setLimit(nextSequenceNumber - 1);
                    recoveryCursor.activate();
                }
            }
        }

        /**
         * Handles paging of the element in the event that there isn't room in
         * memory.
         * 
         * @param source
         *            The source (may be throttled if there isn't room)
         * @param qe
         *            The added element:
         */
        public void elementAdded(ISourceController<?> source, QueueElement<V> qe) {
            // If paging is disabled add a hard ref to keep the
            // element loaded (until it is deleted)
            if (!persistencePolicy.isPagingEnabled()) {
                qe.addHardRef();
            }

            // Persist the element if required:
            if (persistencePolicy.isPersistent(qe.elem)) {
                // For now base decision on whether to delay flush on
                // whether or not there are consumers ready:
                // TODO should actually change this to active cursors:
                boolean delayable = !openCursors.isEmpty();
                qe.save(source, delayable);
            }

            // Check with cursors to see if any of them have room for it
            // in memory:
            if (persistencePolicy.isPagingEnabled()) {

                Collection<Cursor<V>> active = null;

                active = reservedBlocks.get(qe.restoreBlock);

                // If we're set to page out place holders then
                // we need to add a soft ref to keep it from being
                // unloaded if their are already cursors active in the
                // block. 
                if (active != null && pageOutPlaceHolders) {
                    qe.addSoftRef();
                }

                // If there are none, unload the element:
                if (active == null) {
                    qe.unload(source);
                    return;
                }

                boolean accepted = false;
                for (Cursor<V> cursor : active) {

                    if (cursor.offer(qe, source)) {
                        // As long as one cursor can take it we're good:
                        accepted = true;
                        break;
                    }
                }

                // If no cursor accepted it then unload the element:
                if (!accepted) {
                    qe.unload(source);
                }
            }

        }

        public final boolean isPageOutPlaceHolders() {
            return pageOutPlaceHolders;
        }

        /**
         * @param queueElement
         */
        public void pageIn(QueueElement<V> qe) {
            queueStore.restoreQueueElements(queueDescriptor, false, qe.sequence, qe.sequence, 1, this);
        }

        public void reserveBlock(Cursor<V> cursor, long block) {
            HashSet<Cursor<V>> cursors = reservedBlocks.get(block);
            boolean load = recoveryCursor != null && cursor == recoveryCursor;

            if (cursors == null) {
                cursors = new HashSet<Cursor<V>>();
                reservedBlocks.put(block, cursors);
                if (pageOutPlaceHolders) {
                    QueueElement<V> tail = queue.getTail();
                    // If we're at the end of the queue we don't need to
                    // load if we never paged out the block:
                    //                    if (tail != null && tail.isLoaded() && tail.restoreBlock == block && tail.isFirstInBlock()) {
                    //                        load = false;
                    //                    } else {
                    //Otherwise add a soft ref for all element in the block that are already
                    //loaded. 
                    QueueElement<V> qe = queue.upper(RESTORE_BLOCK_SIZE * block, true);
                    while (qe != null && qe.restoreBlock == block) {
                        QueueElement<V> next = qe.getNext();
                        if (qe.isLoaded()) {
                            qe.addSoftRef();
                        } else {
                            //If we find an unloaded element
                            //we'll need to load:
                            load = true;
                        }
                        qe = next;
                    }
                    //                    }
                }
            }
            cursors.add(cursor);

            if (load) {
                if (DEBUG)
                    System.out.println(cursor + " requesting restoreBlock:" + block + " from " + (block * RESTORE_BLOCK_SIZE) + " to " + (block * RESTORE_BLOCK_SIZE + RESTORE_BLOCK_SIZE - 1)
                            + " max: " + RESTORE_BLOCK_SIZE + " queueMax: " + nextSequenceNumber);

                // If paging is enabled only pull in queue records, don't bring in the payload.
                // Each active cursor will have to pull in messages based on available memory.
                queueStore.restoreQueueElements(queueDescriptor, persistencePolicy.isPagingEnabled(), block * RESTORE_BLOCK_SIZE, block * RESTORE_BLOCK_SIZE + RESTORE_BLOCK_SIZE - 1,
                        RESTORE_BLOCK_SIZE, this);
            }
        }

        public void releaseBlock(Cursor<V> cursor, long block) {
            HashSet<Cursor<V>> cursors = reservedBlocks.get(block);
            if (cursors == null) {
                if (DEBUG)
                    System.out.println(this + " removeBlockInterest " + block + ", no cursors" + cursor);
            } else {
                if (cursors.remove(cursor)) {
                    if (cursors.isEmpty()) {
                        reservedBlocks.remove(block);
                        // If this is the last cursor active in the block and paging out
                        // of placeholders is enabled, then release our refs for everything
                        // in the block
                        if (pageOutPlaceHolders) {
                            QueueElement<V> qe = queue.upper(RESTORE_BLOCK_SIZE * block, true);
                            while (qe != null && qe.restoreBlock == block) {
                                QueueElement<V> next = qe.getNext();
                                qe.releaseSoftRef();
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
        final void dispatch() {
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
                        // element it must have been deleted:
                        if (qe == null) {
                            System.out.println("Loaded non-existent element: " + restored.getSequenceNumber());
                            continue;
                        }

                        qe = qe.loadAfter(restored, !persistencePolicy.isPagingEnabled(), pageOutPlaceHolders);

                        if (DEBUG)
                            System.out.println(this + " Loaded loaded" + qe);

                    } catch (Exception ioe) {
                        ioe.printStackTrace();
                        //TODO handle this?
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
                        if (true || DEBUG) {
                            System.out.println("Queue Load completed at " + qe);
                        }

                        recoveryCursor.deactivate();
                        recoveryCursor = null;
                        break;
                    }
                    recoveryCursor.skip(qe);

                }
            }
        }

        public final boolean needsDispatch() {
            if (recoveryCursor != null && recoveryCursor.isReady()) {
                return true;
            }

            synchronized (fromDatabase) {
                return !fromDatabase.isEmpty();
            }
        }

        public void elementsRestored(Collection<RestoredElement<V>> msgs) {
            synchronized (fromDatabase) {
                fromDatabase.addAll(msgs);
            }
            requestDispatch();
        }

        public String toString() {
            return "QueueLoader " + CursoredQueue.this;
        }
    }

    /**
     */
    public void shutdown() {
        stop();
        if (!openCursors.isEmpty()) {
            ArrayList<Cursor<V>> cursors = new ArrayList<Cursor<V>>(openCursors.size());
            for (Cursor<V> cursor : cursors) {
                cursor.close();
            }
        }
    }
}
