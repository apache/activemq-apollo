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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.Service;
import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.Store.Callback;
import org.apache.activemq.broker.store.Store.FatalStoreException;
import org.apache.activemq.broker.store.Store.KeyNotFoundException;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.broker.store.Store.QueueQueryResult;
import org.apache.activemq.broker.store.Store.QueueRecord;
import org.apache.activemq.broker.store.Store.Session;
import org.apache.activemq.dispatch.DispatcherAware;
import org.apache.activemq.dispatch.internal.advanced.AdvancedDispatchSPI;
import org.apache.activemq.flow.AbstractLimitedFlowResource;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.apache.activemq.queue.QueueDescriptor;
import org.apache.activemq.queue.RestoreListener;
import org.apache.activemq.queue.RestoredElement;
import org.apache.activemq.queue.SaveableQueueElement;
import org.apache.activemq.util.FutureListener;
import org.apache.activemq.util.ListenableFuture;
import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.util.list.LinkedNode;
import org.apache.activemq.util.list.LinkedNodeList;

public class BrokerDatabase extends AbstractLimitedFlowResource<BrokerDatabase.OperationBase<?>> implements Service, DispatcherAware {

    private static final boolean DEBUG = false;

    private final Store store;
    private final Flow databaseFlow = new Flow("database", false);

    private final SizeLimiter<OperationBase<?>> storeLimiter;
    private final FlowController<OperationBase<?>> storeController;
    private final int FLUSH_QUEUE_SIZE = 10000 * 1024;

    private AdvancedDispatchSPI dispatcher;
    private Thread flushThread;
    private AtomicBoolean running = new AtomicBoolean(false);
    private DatabaseListener listener;

    private final LinkedNodeList<OperationBase<?>> opQueue;
    private AtomicBoolean notify = new AtomicBoolean(false);
    private Semaphore opsReady = new Semaphore(0);
    private long opSequenceNumber;
    private long flushPointer = -1; // The last seq num for which flush was
    // requested
    private long requestedDelayedFlushPointer = -1; // Set to the last sequence
    // num scheduled for delay
    private long delayedFlushPointer = 0; // The last delayable sequence num
    // requested.
    private long flushDelay = 10;

    private final Runnable flushDelayCallback;
    private boolean storeBypass = true;

    public interface DatabaseListener {
        /**
         * Called if there is a catastrophic problem with the database.
         * 
         * @param ioe
         *            The causing exception.
         */
        public void onDatabaseException(IOException ioe);
    }

    public static interface MessageRecordMarshaller<V> {
        MessageRecord marshal(V element);

        /**
         * Called when a queue element is recovered from the store for a
         * particular queue.
         * 
         * @param mRecord
         *            The message record
         * @param queue
         *            The queue that the element is being restored to (or null
         *            if not being restored for a queue)
         * @return
         */
        V unMarshall(MessageRecord mRecord, QueueDescriptor queue);
    }

    public BrokerDatabase(Store store) {
        this.store = store;
        this.opQueue = new LinkedNodeList<OperationBase<?>>();
        storeLimiter = new SizeLimiter<OperationBase<?>>(FLUSH_QUEUE_SIZE, 0) {

            @Override
            public int getElementSize(OperationBase<?> op) {
                return op.getLimiterSize();
            }
        };

        storeController = new FlowController<OperationBase<?>>(new FlowControllable<OperationBase<?>>() {

            public void flowElemAccepted(ISourceController<OperationBase<?>> controller, OperationBase<?> op) {
                addToOpQueue(op);
            }

            public IFlowResource getFlowResource() {
                return BrokerDatabase.this;
            }

        }, databaseFlow, storeLimiter, opQueue);
        storeController.useOverFlowQueue(false);
        super.onFlowOpened(storeController);

        flushDelayCallback = new Runnable() {
            public void run() {
                flushDelayCallback();
            }
        };
    }

    public synchronized void start() throws Exception {
        if (flushThread == null) {

            running.set(true);
            store.start();
            flushThread = new Thread(new Runnable() {

                public void run() {
                    processOps();
                }

            }, "StoreThread");
            flushThread.start();
        }
    }

    public synchronized void stop() throws Exception {
        if (flushThread != null) {

            synchronized (opQueue) {
                updateFlushPointer(opSequenceNumber + 1);
            }

            running.set(false);
            boolean interrupted = false;
            while (true) {
                opsReady.release();
                try {
                    flushThread.join();
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }

            store.flush();
            store.stop();

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            flushThread = null;
        }
    }

    /**
     * A blocking operation that lists all queues of a given type:
     * 
     * @param type
     *            The queue type
     * @return A list of queues.
     * 
     * @throws Exception
     *             If there was an error listing the queues.
     */
    public Iterator<QueueQueryResult> listQueues(final short type) throws Exception {
        return store.execute(new Callback<Iterator<QueueQueryResult>, Exception>() {

            public Iterator<QueueQueryResult> execute(Session session) throws Exception {
                return session.queueListByType(type, null, Integer.MAX_VALUE);
            }

        }, null);
    }

    /**
     * A blocking operation that lists all entries in the specified map
     * 
     * @param map
     *            The map to list
     * @return A list of map entries
     * 
     * @throws Exception
     *             If there was an error listing the queues.
     */
    public Map<AsciiBuffer, Buffer> listMapEntries(final AsciiBuffer map) throws Exception {
        return store.execute(new Callback<Map<AsciiBuffer, Buffer>, Exception>() {

            public Map<AsciiBuffer, Buffer> execute(Session session) throws Exception {
                HashMap<AsciiBuffer, Buffer> ret = new HashMap<AsciiBuffer, Buffer>();
                try {
                    Iterator<AsciiBuffer> keys = session.mapEntryListKeys(map, null, -1);
                    while (keys.hasNext()) {
                        AsciiBuffer key = keys.next();
                        ret.put(key, session.mapEntryGet(map, key));
                    }
                } catch (Store.KeyNotFoundException knfe) {
                    //No keys then:
                }

                return ret;
            }

        }, null);
    }

    /**
     * @param map
     *            The name of the map to update.
     * @param key
     *            The key in the map to update.
     * @param value
     *            The value to insert.
     */
    public OperationContext<?> updateMapEntry(AsciiBuffer map, AsciiBuffer key, Buffer value) {
        return add(new MapUpdateOperation(map, key, value), null, false);
    }

    /**
     * Executes user supplied {@link Operation}. If the {@link Operation} does
     * not throw any Exceptions, all updates to the store are committed,
     * otherwise they are rolled back. Any exceptions thrown by the
     * {@link Operation} are propagated by this method.
     * 
     * If limiter space on the store processing queue is exceeded, the
     * controller will be blocked.
     * 
     * If this method is called with flush set to
     * <code>false</false> there is no 
     * guarantee made about when the operation will be executed. If <code>flush</code>
     * is <code>true</code> and {@link Operation#isDelayable()} is also
     * <code>true</code> then an attempt will be made to execute the event at
     * the {@link Store}'s configured delay interval.
     * 
     * @param op
     *            The operation to execute
     * @param flush
     *            Whether or not this operation needs immediate processing.
     * @param controller
     *            the source of the operation.
     * @return the {@link OperationContext} associated with the operation
     */
    private <T> OperationContext<T> add(OperationBase<T> op, ISourceController<?> controller, boolean flush) {

        op.flushRequested = flush;
        storeController.add(op, controller);
        return op;
    }

    private final void addToOpQueue(OperationBase<?> op) {
        if (!running.get()) {
            throw new IllegalStateException("BrokerDatabase not started");
        }

        synchronized (opQueue) {
            op.opSequenceNumber = opSequenceNumber++;
            opQueue.addLast(op);
            if (op.flushRequested || storeLimiter.getThrottled()) {
                if (op.isDelayable() && flushDelay > 0) {
                    scheduleDelayedFlush(op.opSequenceNumber);
                } else {
                    updateFlushPointer(op.opSequenceNumber);
                }
            }
        }
    }

    private void updateFlushPointer(long seqNumber) {
        if (seqNumber > flushPointer) {
            flushPointer = seqNumber;
            OperationBase<?> op = opQueue.getHead();
            if (op != null && op.opSequenceNumber <= flushPointer && notify.get()) {
                opsReady.release();
            }
        }
    }

    private void scheduleDelayedFlush(long seqNumber) {
        if (seqNumber < flushPointer) {
            return;
        }

        if (seqNumber > delayedFlushPointer) {
            delayedFlushPointer = seqNumber;
        }

        if (requestedDelayedFlushPointer == -1) {
            requestedDelayedFlushPointer = delayedFlushPointer;
            dispatcher.schedule(flushDelayCallback, flushDelay, TimeUnit.MILLISECONDS);
        }

    }

    private final void flushDelayCallback() {
        synchronized (opQueue) {
            if (flushPointer < requestedDelayedFlushPointer) {
                updateFlushPointer(requestedDelayedFlushPointer);

            }

            // If another delayed flush has been scheduled schedule it:
            requestedDelayedFlushPointer = -1;
            // Schedule next delay if needed:
            if (delayedFlushPointer > flushPointer) {
                scheduleDelayedFlush(delayedFlushPointer);
            } else {
                delayedFlushPointer = -1;
            }

        }
    }

    private final OperationBase<?> getNextOp(boolean wait) {
        if (!wait) {
            synchronized (opQueue) {
                OperationBase<?> op = opQueue.getHead();
                if (op != null && (op.opSequenceNumber <= flushPointer || !op.isDelayable())) {
                    op.unlink();
                    return op;
                }
            }
            return null;
        } else {
            OperationBase<?> op = getNextOp(false);
            if (op == null) {
                notify.set(true);
                op = getNextOp(false);
                try {
                    while (running.get() && op == null) {
                        opsReady.acquireUninterruptibly();
                        op = getNextOp(false);
                    }
                } finally {
                    notify.set(false);
                    opsReady.drainPermits();
                }
            }
            return op;
        }
    }

    private final void processOps() {
        int count = 0;
        Session session = store.getSession();
        while (running.get()) {
            final OperationBase<?> firstOp = getNextOp(true);
            if (firstOp == null) {
                continue;
            }
            count = 0;

            // The first operation we get, triggers a store transaction.
            if (firstOp != null) {
                final LinkedList<Operation<?>> processedQueue = new LinkedList<Operation<?>>();
                boolean locked = false;
                try {

                    Operation<?> op = firstOp;
                    while (op != null) {
                        final Operation<?> toExec = op;
                        if (toExec.beginExecute()) {
                            if (!locked) {
                                session.acquireLock();
                                locked = true;
                            }
                            count++;
                            op.execute(session);
                            processedQueue.add(op);
                            /*
                             * store.execute(new Store.VoidCallback<Exception>()
                             * {
                             * 
                             * @Override public void run(Session session) throws
                             * Exception {
                             * 
                             * // Try to execute the operation against the //
                             * session... try { toExec.execute(session);
                             * processedQueue.add(toExec); } catch
                             * (CancellationException ignore) { //
                             * System.out.println("Cancelled" + // toExec); } }
                             * }, null);
                             */
                        }

                        if (count < 1000) {
                            op = getNextOp(false);
                        } else {
                            op = null;
                        }
                    }
                    // executeOps(firstOp, processedQueue, counter);

                    // If we procecessed some ops, flush and post process:
                    if (!processedQueue.isEmpty()) {

                        if (locked) {
                            session.commit();
                            session.releaseLock();
                            locked = false;
                        }
                        if (DEBUG)
                            System.out.println("Flushing queue after processing: " + processedQueue.size() + " - " + processedQueue);
                        // Sync the store:
                        store.flush();

                        // Post process operations
                        long release = 0;
                        for (Operation<?> processed : processedQueue) {
                            processed.onCommit();
                            // System.out.println("Processed" + processed);
                            release += processed.getLimiterSize();
                        }

                        synchronized (opQueue) {
                            this.storeLimiter.remove(1, release);
                        }
                    }

                } catch (IOException e) {
                    for (Operation<?> processed : processedQueue) {
                        processed.onRollback(e);
                    }
                    onDatabaseException(e);
                } catch (RuntimeException e) {
                    for (Operation<?> processed : processedQueue) {
                        processed.onRollback(e);
                    }
                    IOException ioe = new IOException(e.getMessage());
                    ioe.initCause(e);
                    onDatabaseException(ioe);
                } catch (Exception e) {
                    for (Operation<?> processed : processedQueue) {
                        processed.onRollback(e);
                    }
                    IOException ioe = new IOException(e.getMessage());
                    ioe.initCause(e);
                    onDatabaseException(ioe);
                } finally {
                    if (locked) {
                        try {
                            session.releaseLock();
                        } catch (Exception e) {
                            IOException ioe = new IOException(e.getMessage());
                            ioe.initCause(e);
                            onDatabaseException(ioe);
                        }
                    }
                }
            }
        }
    }

    /*
     * private final void executeOps(final OperationBase op, final
     * LinkedList<Operation> processedQueue, final OpCounter counter) throws
     * FatalStoreException, Exception { store.execute(new
     * Store.VoidCallback<Exception>() {
     * 
     * @Override public void run(Session session) throws Exception {
     * 
     * // Try to execute the operation against the // session... try { if
     * (op.execute(session)) { processedQueue.add(op); } else { counter.count--;
     * } } catch (CancellationException ignore) { System.out.println("Cancelled"
     * + op); }
     * 
     * // See if we can batch up some additional operations // in this
     * transaction. if (counter.count < 100) { OperationBase next =
     * getNextOp(false); if (next != null) { counter.count++; executeOps(next,
     * processedQueue, counter); } } } }, null); }
     */

    /**
     * Adds a queue to the database
     * 
     * @param queue
     *            The queue to add.
     */
    public void addQueue(QueueDescriptor queue) {
        add(new QueueAddOperation(queue), null, false);
    }

    /**
     * Deletes a queue and all of its messages from the database
     * 
     * @param queue
     *            The queue to delete.
     */
    public void deleteQueue(QueueDescriptor queue) {
        add(new QueueDeleteOperation(queue), null, false);
    }

    /**
     * Saves a message for all of the recipients in the
     * {@link BrokerMessageDelivery}.
     * 
     * @param delivery
     *            The delivery.
     * @param source
     *            The source's controller.
     * @throws IOException
     *             If there is an error marshalling the message.
     * @return The {@link OperationContext} associated with the operation
     */
    public OperationContext<?> persistReceivedMessage(BrokerMessageDelivery delivery, ISourceController<?> source) {
        return add(new AddMessageOperation(delivery), source, true);
    }

    /**
     * Saves a Message for a single queue.
     * 
     * @param queueElement
     *            The element to save.
     * @param source
     *            The source initiating the save or null, if there isn't one.
     * @throws IOException
     *             If there is an error marshalling the message.
     * 
     * @return The {@link OperationContext} associated with the operation
     */
    public OperationContext<?> saveMessage(SaveableQueueElement<MessageDelivery> queueElement, ISourceController<?> source, boolean delayable) {
        return add(new AddMessageOperation(queueElement), source, !delayable);
    }

    /**
     * Deletes the given message from the store for the given queue.
     * 
     * @param storeTracking
     *            The tracking number of the element being deleted
     * @param queue
     *            The queue.
     * @return The {@link OperationContext} associated with the operation
     */
    public OperationContext<?> deleteQueueElement(SaveableQueueElement<?> queueElement) {
        return add(new DeleteOperation(queueElement.getSequenceNumber(), queueElement.getQueueDescriptor()), null, false);
    }

    /**
     * Loads a batch of messages for the specified queue. The loaded messages
     * are given the provided {@link MessageRestoreListener}.
     * <p>
     * <b><i>NOTE:</i></b> This method uses the queue sequence number for the
     * message not the store tracking number.
     * 
     * @param queue
     *            The queue for which to load messages
     * @param recordsOnly
     *            True if message body shouldn't be restored
     * @param first
     *            The first queue sequence number to load (-1 starts at
     *            begining)
     * @param maxSequence
     *            The maximum sequence number to load (-1 if no limit)
     * @param max
     *            The maximum number of messages to load (-1 if no limit)
     * @param listener
     *            The listener to which messags should be passed.
     * @return The {@link OperationContext} associated with the operation
     */
    public <T> OperationContext<?> restoreQueueElements(QueueDescriptor queue, boolean recordsOnly, long first, long maxSequence, int maxCount, RestoreListener<T> listener,
            MessageRecordMarshaller<T> marshaller) {
        return add(new RestoreElementsOperation<T>(queue, recordsOnly, first, maxCount, maxSequence, listener, marshaller), null, true);
    }

    private void onDatabaseException(IOException ioe) {
        if (listener != null) {
            listener.onDatabaseException(ioe);
        } else {
            ioe.printStackTrace();
        }
    }

    public interface OperationContext<V> extends ListenableFuture<V> {

        /**
         * Attempts to cancel the store operation. Returns true if the operation
         * could be canceled or false if the operation was already executed by
         * the store.
         * 
         * @return true if the operation could be canceled
         */
        public boolean cancel();

        /**
         * Requests flush for this database operation (overriding a previous
         * delay)
         */
        public void requestFlush();
    }

    /**
     * This interface is used to execute transacted code.
     * 
     * It is used by the {@link Store#execute(Callback)} method, often as
     * anonymous class.
     */
    public interface Operation<V> extends OperationContext<V> {

        /**
         * Called when the saver is about to execute the operation. If true is
         * returned the operation can no longer be canceled.
         * 
         * @return false if the operation has been canceled.
         */
        public boolean beginExecute();

        /**
         * Gets called by the
         * {@link Store#add(Operation, ISourceController, boolean)} method
         * within a transactional context. If any exception is thrown including
         * Runtime exception, the transaction is rolled back.
         * 
         * @param session
         *            provides you access to read and update the persistent
         *            data.
         * @throws Exception
         *             if an system error occured while executing the
         *             operations.
         * @throws RuntimeException
         *             if an system error occured while executing the
         *             operations.
         */
        public void execute(Session session) throws CancellationException, Exception, RuntimeException;

        /**
         * Returns true if this operation can be delayed. This is useful in
         * cases where external events can negate the need to execute the
         * operation. The delay interval is not guaranteed to be honored, if
         * subsequent events or other store flush policy/criteria requires a
         * flush of subsequent events.
         * 
         * @return True if the operation can be delayed.
         */
        public boolean isDelayable();

        /**
         * Returns the size to be used when calculating how much space this
         * operation takes on the store processing queue.
         * 
         * @return The limiter size to be used.
         */
        public int getLimiterSize();

        /**
         * Called after {@link #execute(Session)} is called and the the
         * operation has been committed.
         */
        public void onCommit();

        /**
         * Called after {@link #execute(Session)} is called and the the
         * operation has been rolled back.
         */
        public void onRollback(Throwable error);
    }

    /**
     * This is a convenience base class that can be used to implement
     * Operations. It handles operation cancellation for you.
     */
    abstract class OperationBase<V> extends LinkedNode<OperationBase<?>> implements Operation<V> {
        public boolean flushRequested = false;
        public long opSequenceNumber = -1;

        final protected AtomicBoolean executePending = new AtomicBoolean(true);
        final protected AtomicBoolean cancelled = new AtomicBoolean(false);
        final protected AtomicBoolean executed = new AtomicBoolean(false);
        final protected AtomicReference<FutureListener<? super V>> listener = new AtomicReference<FutureListener<? super V>>();

        protected Throwable error;

        public static final int BASE_MEM_SIZE = 20;

        public boolean cancel(boolean interrupt) {
            return cancel();
        }

        public boolean cancel() {
            if (storeBypass) {
                if (executePending.compareAndSet(true, false)) {
                    cancelled.set(true);
                    // System.out.println("Cancelled: " + this);
                    synchronized (opQueue) {
                        unlink();
                        storeController.elementDispatched(this);
                    }
                    fireListener();
                    return true;
                }
            }
            return cancelled.get();
        }

        public final boolean isCancelled() {
            return cancelled.get();
        }

        public final boolean isExecuted() {
            return executed.get();
        }

        public final boolean isDone() {
            return isCancelled() || isExecuted();
        }

        /**
         * Called when the saver is about to execute the operation. If true is
         * returned the operation can no longer be cancelled.
         * 
         * @return true if operation should be executed
         */
        public final boolean beginExecute() {
            if (executePending.compareAndSet(true, false)) {
                return true;
            } else {
                return false;
            }
        }

        /**
         * Gets called by the
         * {@link Store#add(Operation, ISourceController, boolean)} method
         * within a transactional context. If any exception is thrown including
         * Runtime exception, the transaction is rolled back.
         * 
         * @param session
         *            provides you access to read and update the persistent
         *            data.
         * @throws Exception
         *             if an system error occured while executing the
         *             operations.
         * @throws RuntimeException
         *             if an system error occured while executing the
         *             operations.
         */
        public void execute(Session session) throws Exception, RuntimeException {
            if (DEBUG)
                System.out.println("Executing " + this);
            doExcecute(session);
        }

        abstract protected void doExcecute(Session session);

        public int getLimiterSize() {
            return BASE_MEM_SIZE;
        }

        public boolean isDelayable() {
            return false;
        }

        /**
         * Requests flush for this database operation (overriding a previous
         * delay)
         */
        public void requestFlush() {
            synchronized (opQueue) {
                updateFlushPointer(opSequenceNumber);
            }
        }

        public void onCommit() {
            executed.set(true);
            fireListener();
        }

        /**
         * Called after {@link #execute(Session)} is called and the the
         * operation has been rolled back.
         */
        public void onRollback(Throwable error) {
            executed.set(true);
            if (!fireListener()) {
                error.printStackTrace();
            }
        }

        private final boolean fireListener() {
            FutureListener<? super V> l = this.listener.getAndSet(null);
            if (l != null) {
                l.onFutureComplete(this);
                return true;
            }
            return false;
        }

        public void setFutureListener(FutureListener<? super V> listener) {
            this.listener.set(listener);
            if (isDone()) {
                fireListener();
            }
        }

        /**
         * Subclasses the return a result should override this
         * @return The result.
         */
        protected final V getResult() {
            return null;
        }

        /**
         * Waits if necessary for the computation to complete, and then
         * retrieves its result.
         * 
         * @return the computed result
         * @throws CancellationException
         *             if the computation was cancelled
         * @throws ExecutionException
         *             if the computation threw an exception
         * @throws InterruptedException
         *             if the current thread was interrupted while waiting
         */
        public final V get() throws ExecutionException, InterruptedException  {
            
            try {
                return get(-1, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                //Can't happen.
                throw new AssertionError(e);
            }
        }
        
        /**
         * Waits if necessary for at most the given time for the computation
         * to complete, and then retrieves its result, if available.
         *
         * @param timeout the maximum time to wait
         * @param unit the time unit of the timeout argument
         * @return the computed result
         * @throws CancellationException if the computation was cancelled
         * @throws ExecutionException if the computation threw an
         * exception
         * @throws InterruptedException if the current thread was interrupted
         * while waiting
         * @throws TimeoutException if the wait timed out
         */
        public final V get(long timeout, TimeUnit tu) throws ExecutionException, InterruptedException, TimeoutException {
            if (isCancelled()) {
                throw new CancellationException();
            }
            if (error != null) {
                throw new ExecutionException(error);
            }
            
            //TODO implement blocking?
            if(!isDone())
            {
                throw new UnsupportedOperationException("Blocking result retrieval not yet implemented");
            }
            
            return getResult();
        }

        public String toString() {
            return "DBOp seq: " + opSequenceNumber + "P/C/E: " + executePending.get() + "/" + isCancelled() + "/" + isExecuted();
        }
    }

    private class QueueAddOperation extends OperationBase<Object> {

        private QueueDescriptor qd;

        QueueAddOperation(QueueDescriptor queue) {
            qd = queue;
        }

        @Override
        protected void doExcecute(Session session) {
            try {
                session.queueAdd(qd);
            } catch (KeyNotFoundException e) {
                throw new FatalStoreException(e);
            }
        }

        public String toString() {
            return "QueueAdd: " + qd.getQueueName().toString();
        }
    }

    private class QueueDeleteOperation extends OperationBase<Object> {

        private QueueDescriptor qd;

        QueueDeleteOperation(QueueDescriptor queue) {
            qd = queue;
        }

        @Override
        protected void doExcecute(Session session) {
            session.queueRemove(qd);
        }

        public String toString() {
            return "QueueDelete: " + qd.getQueueName().toString();
        }
    }

    private class DeleteOperation extends OperationBase<Object> {
        private final long queueKey;
        private QueueDescriptor queue;

        public DeleteOperation(long queueKey, QueueDescriptor queue) {
            this.queueKey = queueKey;
            this.queue = queue;
        }

        @Override
        public int getLimiterSize() {
            return BASE_MEM_SIZE + 8;
        }

        @Override
        protected void doExcecute(Session session) {
            try {
                session.queueRemoveMessage(queue, queueKey);
            } catch (KeyNotFoundException e) {
                // TODO Probably doesn't always mean an error, it is possible
                // that
                // the queue has been deleted, in which case its messages will
                // have been deleted, too.
                e.printStackTrace();
            }
        }

        public String toString() {
            return "MessageDelete: " + queue.getQueueName().toString() + " tracking: " + queueKey + " " + super.toString();
        }
    }

    private class RestoreElementsOperation<V> extends OperationBase<V> {
        private QueueDescriptor queue;
        private long firstKey;
        private int maxRecords;
        private long maxSequence;
        private boolean recordsOnly;
        private RestoreListener<V> listener;
        private Collection<RestoredElement<V>> msgs = null;
        private MessageRecordMarshaller<V> marshaller;

        RestoreElementsOperation(QueueDescriptor queue, boolean recordsOnly, long firstKey, int maxRecords, long maxSequence, RestoreListener<V> listener, MessageRecordMarshaller<V> marshaller) {
            this.queue = queue;
            this.recordsOnly = recordsOnly;
            this.firstKey = firstKey;
            this.maxRecords = maxRecords;
            this.maxSequence = maxSequence;
            this.listener = listener;
            this.marshaller = marshaller;
        }

        @Override
        public int getLimiterSize() {
            return BASE_MEM_SIZE + 44;
        }

        @Override
        protected void doExcecute(Session session) {

            Iterator<QueueRecord> records = null;
            try {
                records = session.queueListMessagesQueue(queue, firstKey, maxSequence, maxRecords);
                msgs = new LinkedList<RestoredElement<V>>();
            } catch (KeyNotFoundException e) {
                msgs = new ArrayList<RestoredElement<V>>(0);
                return;
            }

            QueueRecord qRecord = null;
            int count = 0;
            if (records.hasNext()) {
                qRecord = records.next();
            }

            while (qRecord != null) {
                RestoredElementImpl<V> rm = new RestoredElementImpl<V>();
                // TODO should update jms redelivery here.
                rm.qRecord = qRecord;
                rm.queue = queue;
                count++;

                // Set the next sequence number:
                if (records.hasNext()) {
                    qRecord = records.next();
                    rm.nextSequence = qRecord.getQueueKey();
                } else {
                    // Look up the next sequence number:
                    try {
                        records = session.queueListMessagesQueue(queue, qRecord.getQueueKey() + 1, -1L, 1);
                        if (!records.hasNext()) {
                            rm.nextSequence = -1;
                        } else {
                            rm.nextSequence = records.next().getQueueKey();
                        }
                    } catch (KeyNotFoundException e) {
                        rm.nextSequence = -1;
                    }
                    qRecord = null;
                }

                if (!recordsOnly) {
                    try {
                        rm.mRecord = session.messageGetRecord(rm.qRecord.getMessageKey());
                        rm.marshaller = marshaller;
                        msgs.add(rm);
                    } catch (KeyNotFoundException shouldNotHappen) {
                        shouldNotHappen.printStackTrace();
                    }
                } else {
                    msgs.add(rm);
                }
            }

            if (DEBUG)
                System.out.println("Restored: " + count + " messages");
        }

        @Override
        public void onCommit() {
            listener.elementsRestored(msgs);
            super.onCommit();
        }

        public String toString() {
            return "MessageRestore: " + queue.getQueueName().toString() + " first: " + firstKey + " max: " + maxRecords;
        }
    }

    private class AddMessageOperation extends OperationBase<Object> {

        private final BrokerMessageDelivery brokerDelivery;
        private final SaveableQueueElement<MessageDelivery> singleElement;
        private final MessageDelivery delivery;
        private MessageRecord record;
        private LinkedList<SaveableQueueElement<MessageDelivery>> notifyTargets;
        private final boolean delayable;

        public AddMessageOperation(BrokerMessageDelivery delivery) {
            this.brokerDelivery = delivery;
            this.singleElement = null;
            this.delivery = delivery;
            this.delayable = delivery.isFlushDelayable();
            if (!delayable) {
                this.record = delivery.createMessageRecord();
            }
        }

        public AddMessageOperation(SaveableQueueElement<MessageDelivery> queueElement) {
            this.brokerDelivery = null;
            singleElement = queueElement;
            delivery = queueElement.getElement();
            this.record = singleElement.getElement().createMessageRecord();
            delayable = false;
        }

        public boolean isDelayable() {
            return delayable;
        }

        @Override
        public int getLimiterSize() {
            return delivery.getFlowLimiterSize() + BASE_MEM_SIZE + 40;
        }

        @Override
        protected void doExcecute(Session session) {

            if (singleElement == null) {
                brokerDelivery.beginStore();
                Collection<SaveableQueueElement<MessageDelivery>> targets = brokerDelivery.getPersistentQueues();

                if (targets != null && !targets.isEmpty()) {
                    if (record == null) {
                        record = brokerDelivery.createMessageRecord();
                        if (record == null) {
                            throw new RuntimeException("Error creating message record for " + brokerDelivery.getMsgId());
                        }
                    }
                    record.setKey(brokerDelivery.getStoreTracking());
                    session.messageAdd(record);

                    for (SaveableQueueElement<MessageDelivery> target : targets) {
                        try {
                            QueueRecord queueRecord = new QueueRecord();
                            queueRecord.setAttachment(null);
                            queueRecord.setMessageKey(record.getKey());
                            queueRecord.setSize(brokerDelivery.getFlowLimiterSize());
                            queueRecord.setQueueKey(target.getSequenceNumber());
                            session.queueAddMessage(target.getQueueDescriptor(), queueRecord);

                        } catch (KeyNotFoundException e) {
                            e.printStackTrace();
                        }

                        if (target.requestSaveNotify()) {
                            if (notifyTargets == null) {
                                notifyTargets = new LinkedList<SaveableQueueElement<MessageDelivery>>();
                            }
                            notifyTargets.add(target);
                        }
                    }
                } else {
                    // Save with no targets must have been cancelled:
                    // System.out.println("Skipping save for " +
                    // delivery.getStoreTracking());
                }
            } else {

                session.messageAdd(record);
                try {
                    QueueRecord queueRecord = new QueueRecord();
                    queueRecord.setAttachment(null);
                    queueRecord.setMessageKey(record.getKey());
                    queueRecord.setSize(brokerDelivery.getFlowLimiterSize());
                    queueRecord.setQueueKey(singleElement.getSequenceNumber());
                    session.queueAddMessage(singleElement.getQueueDescriptor(), queueRecord);
                } catch (KeyNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void onCommit() {

            // Notify that the message was persisted.
            delivery.onMessagePersisted();

            // Notify any of the targets that requested notify on save:
            if (singleElement != null && singleElement.requestSaveNotify()) {
                singleElement.notifySave();
            } else if (notifyTargets != null) {
                for (SaveableQueueElement<MessageDelivery> notify : notifyTargets) {
                    notify.notifySave();
                }
            }
            
            super.onCommit();
        }

        public String toString() {
            return "AddOperation " + delivery.getStoreTracking() + super.toString();
        }
    }

    private class MapUpdateOperation extends OperationBase<Object> {
        final AsciiBuffer map;
        final AsciiBuffer key;
        final Buffer value;

        MapUpdateOperation(AsciiBuffer mapName, AsciiBuffer key, Buffer value) {
            this.map = mapName;
            this.key = key;
            this.value = value;
        }

        @Override
        public int getLimiterSize() {
            return BASE_MEM_SIZE + map.length + key.length + value.length;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.apollo.broker.BrokerDatabase.OperationBase#doExcecute
         * (org.apache.activemq.broker.store.Store.Session)
         */
        @Override
        protected void doExcecute(Session session) {
            try {
                session.mapEntryPut(map, key, value);
            } catch (KeyNotFoundException e) {
                throw new Store.FatalStoreException(e);
            }
        }
    }

    private class RestoredElementImpl<T> implements RestoredElement<T> {
        QueueRecord qRecord;
        QueueDescriptor queue;
        MessageRecord mRecord;
        MessageRecordMarshaller<T> marshaller;
        long nextSequence;

        public T getElement() throws IOException {
            if (mRecord == null) {
                return null;
            }
            return marshaller.unMarshall(mRecord, queue);
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.queue.QueueStore.RestoredElement#getSequenceNumber
         * ()
         */
        public long getSequenceNumber() {
            return qRecord.getQueueKey();
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.queue.QueueStore.RestoredElement#getStoreTracking
         * ()
         */
        public long getStoreTracking() {
            return qRecord.getMessageKey();
        }

        /*
         * (non-Javadoc)
         * 
         * @seeorg.apache.activemq.queue.QueueStore.RestoredElement#
         * getNextSequenceNumber()
         */
        public long getNextSequenceNumber() {
            return nextSequence;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.queue.QueueStore.RestoredElement#getElementSize()
         */
        public int getElementSize() {
            return qRecord.getSize();
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.queue.QueueStore.RestoredElement#getExpiration()
         */
        public long getExpiration() {
            return qRecord.getTte();
        }
    }

    public long allocateStoreTracking() {
        return store.allocateStoreTracking();
    }

    public AdvancedDispatchSPI getDispatcher() {
        return dispatcher;
    }

    public void setDispatcher(AdvancedDispatchSPI dispatcher) {
        this.dispatcher = dispatcher;
    }

    public Store getStore() {
        return store;
    }

    /**
     * @param sqe
     * @param source
     * @param delayable
     */
    public <T> OperationContext<?> saveQeueuElement(SaveableQueueElement<T> sqe, ISourceController<?> source, boolean delayable, MessageRecordMarshaller<T> marshaller) {
        return add(new AddElementOperation<T>(sqe, delayable, marshaller), source, !delayable);
    }

    private class AddElementOperation<T> extends OperationBase<Object> {

        private final SaveableQueueElement<T> op;
        private MessageRecord record;
        private boolean delayable;
        private final MessageRecordMarshaller<T> marshaller;

        public AddElementOperation(SaveableQueueElement<T> op, boolean delayable, MessageRecordMarshaller<T> marshaller) {
            this.op = op;
            this.delayable = delayable;
            if (!delayable) {
                record = marshaller.marshal(op.getElement());
                this.marshaller = null;
            } else {
                this.marshaller = marshaller;
            }
        }

        public boolean isDelayable() {
            return delayable;
        }

        @Override
        public int getLimiterSize() {
            return op.getLimiterSize() + BASE_MEM_SIZE + 32;
        }

        @Override
        protected void doExcecute(Session session) {

            if (record == null) {
                record = marshaller.marshal(op.getElement());
            }

            session.messageAdd(record);
            try {
                QueueRecord queueRecord = new QueueRecord();
                queueRecord.setAttachment(null);
                queueRecord.setMessageKey(record.getKey());
                queueRecord.setSize(record.getSize());
                queueRecord.setQueueKey(op.getSequenceNumber());
                session.queueAddMessage(op.getQueueDescriptor(), queueRecord);
            } catch (KeyNotFoundException e) {
                e.printStackTrace();
            }
        }

        public String toString() {
            return "AddTxOpOperation " + record.getKey() + super.toString();
        }
    }

    public long getFlushDelay() {
        return flushDelay;
    }

    public void setFlushDelay(long flushDelay) {
        this.flushDelay = flushDelay;
    }

    /**
     * @return true if operations are allowed to bypass the store.
     */
    public boolean isStoreBypass() {
        return storeBypass;
    }

    /**
     * Sets if persistent operations should be allowed to bypass the store.
     * Defaults to true, as this will give you the best performance. In some
     * cases, you want to disable this as the store being used will double as an
     * audit log and you do not want any persistent operations to bypass the
     * store.
     * 
     * When store bypass is disabled, all {@link Operation#cancel()} requests
     * will return false.
     * 
     * @param enable
     *            if true will enable store bypass
     */
    public void setStoreBypass(boolean enable) {
        this.storeBypass = enable;
    }
}
