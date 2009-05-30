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
package org.apache.activemq.broker.db;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.BrokerMessageDelivery;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.protocol.ProtocolHandler;
import org.apache.activemq.broker.protocol.ProtocolHandlerFactory;
import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.Store.Callback;
import org.apache.activemq.broker.store.Store.FatalStoreException;
import org.apache.activemq.broker.store.Store.KeyNotFoundException;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.broker.store.Store.QueueQueryResult;
import org.apache.activemq.broker.store.Store.QueueRecord;
import org.apache.activemq.broker.store.Store.Session;
import org.apache.activemq.broker.store.Store.VoidCallback;
import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.flow.AbstractLimitedFlowResource;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.apache.kahadb.util.LinkedNode;
import org.apache.kahadb.util.LinkedNodeList;

public class BrokerDatabase extends AbstractLimitedFlowResource<BrokerDatabase.OperationBase> {

    private static final boolean DEBUG = false;
    private final Store store;
    private final Flow databaseFlow = new Flow("database", false);

    private final SizeLimiter<OperationBase> storeLimiter;
    private final FlowController<OperationBase> storeController;
    private final int FLUSH_QUEUE_SIZE = 10000 * 1024;

    private final IDispatcher dispatcher;
    private Thread flushThread;
    private AtomicBoolean running = new AtomicBoolean(false);
    private DatabaseListener listener;

    private HashMap<String, ProtocolHandler> protocolHandlers = new HashMap<String, ProtocolHandler>();

    private final LinkedNodeList<OperationBase> opQueue;
    private AtomicBoolean notify = new AtomicBoolean(false);
    private Semaphore opsReady = new Semaphore(0);
    private long opSequenceNumber;
    private long flushPointer = -1; // The last seq num for which flush was
    // requested
    private long requestedDelayedFlushPointer = -1; // Set to the last sequence
    // num scheduled for delay
    private long delayedFlushPointer = 0; // The last delayable sequence num
    // requested.
    private final long FLUSH_DELAY_MS = 10;
    private final Runnable flushDelayCallback;

    public interface DatabaseListener {
        /**
         * Called if there is a catastrophic problem with the database.
         * 
         * @param ioe
         *            The causing exception.
         */
        public void onDatabaseException(IOException ioe);
    }

    public BrokerDatabase(Store store, IDispatcher dispatcher) {
        this.store = store;
        this.dispatcher = dispatcher;
        this.opQueue = new LinkedNodeList<OperationBase>();
        storeLimiter = new SizeLimiter<OperationBase>(FLUSH_QUEUE_SIZE, 0) {

            @Override
            public int getElementSize(OperationBase op) {
                return op.getLimiterSize();
            }
        };

        storeController = new FlowController<OperationBase>(new FlowControllable<OperationBase>() {

            public void flowElemAccepted(ISourceController<OperationBase> controller, OperationBase op) {
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

    public Iterator<QueueQueryResult> listQueues(final short type) throws Exception {
        // TODO Auto-generated method stub
        return store.execute(new Callback<Iterator<QueueQueryResult>, Exception>() {

            public Iterator<QueueQueryResult> execute(Session session) throws Exception {
                // TODO Auto-generated method stub
                return session.queueListByType(type, null, Integer.MAX_VALUE);
            }

        }, null);
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
    private OperationContext add(OperationBase op, ISourceController<?> controller, boolean flush) {

        op.flushRequested = flush;
        storeController.add(op, controller);
        return op;
    }

    private final void addToOpQueue(OperationBase op) {
        if (!running.get()) {
            throw new IllegalStateException("BrokerDatabase not started");
        }

        synchronized (opQueue) {
            op.opSequenceNumber = opSequenceNumber++;
            opQueue.addLast(op);
            if (op.flushRequested || storeLimiter.getThrottled()) {
                if (op.isDelayable() && FLUSH_DELAY_MS > 0) {
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
            OperationBase op = opQueue.getHead();
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
            dispatcher.schedule(flushDelayCallback, FLUSH_DELAY_MS, TimeUnit.MILLISECONDS);
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

    private final OperationBase getNextOp(boolean wait) {
        if (!wait) {
            synchronized (opQueue) {
                OperationBase op = opQueue.getHead();
                if (op != null && (op.opSequenceNumber <= flushPointer || !op.isDelayable())) {
                    op.unlink();
                    return op;
                }
            }
            return null;
        } else {
            OperationBase op = getNextOp(false);
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

        while (running.get()) {
            final OperationBase firstOp = getNextOp(true);
            if (firstOp == null) {
                continue;
            }
            count = 0;

            // The first operation we get, triggers a store transaction.
            if (firstOp != null) {
                final LinkedList<Operation> processedQueue = new LinkedList<Operation>();
                try {

                    Operation op = firstOp;
                    // TODO the recursion here leads to a rather large stack,
                    // refactor.
                    while (op != null) {
                        final Operation toExec = op;
                        if (toExec.beginExecute()) {
                            count++;

                            store.execute(new Store.VoidCallback<Exception>() {
                                @Override
                                public void run(Session session) throws Exception {

                                    // Try to execute the operation against the
                                    // session...
                                    try {
                                        toExec.execute(session);
                                        processedQueue.add(toExec);
                                    } catch (CancellationException ignore) {
                                        // System.out.println("Cancelled" +
                                        // toExec);
                                    }
                                }
                            }, null);
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

                        if (DEBUG)
                            System.out.println("Flushing queue after processing: " + processedQueue.size() + " - " + processedQueue);
                        // Sync the store:
                        store.flush();

                        // Post process operations
                        long release = 0;
                        for (Operation processed : processedQueue) {
                            processed.onCommit();
                            // System.out.println("Processed" + processed);
                            release += processed.getLimiterSize();
                        }

                        synchronized (opQueue) {
                            this.storeLimiter.remove(1, release);
                        }
                    }

                } catch (IOException e) {
                    for (Operation processed : processedQueue) {
                        processed.onRollback(e);
                    }
                    onDatabaseException(e);
                } catch (RuntimeException e) {
                    for (Operation processed : processedQueue) {
                        processed.onRollback(e);
                    }
                    IOException ioe = new IOException(e.getMessage());
                    ioe.initCause(e);
                    onDatabaseException(ioe);
                } catch (Exception e) {
                    for (Operation processed : processedQueue) {
                        processed.onRollback(e);
                    }
                    IOException ioe = new IOException(e.getMessage());
                    ioe.initCause(e);
                    onDatabaseException(ioe);
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
    public OperationContext persistReceivedMessage(BrokerMessageDelivery delivery, ISourceController<?> source) {
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
    public OperationContext saveMessage(SaveableQueueElement<MessageDelivery> queueElement, ISourceController<?> source, boolean delayable) {
        return add(new AddMessageOperation(queueElement), source, delayable);
    }

    /**
     * Deletes the given message from the store for the given queue.
     * 
     * @param delivery
     *            The delivery.
     * @param queue
     *            The queue.
     * @return The {@link OperationContext} associated with the operation
     */
    public OperationContext deleteMessage(MessageDelivery delivery, QueueDescriptor queue) {
        return add(new DeleteMessageOperation(delivery.getStoreTracking(), queue), null, false);
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
    public OperationContext restoreMessages(QueueDescriptor queue, boolean recordsOnly, long first, long maxSequence, int maxCount, RestoreListener<MessageDelivery> listener) {
        return add(new RestoreMessageOperation(queue, recordsOnly, first, maxCount, maxSequence, listener), null, true);
    }

    private void onDatabaseException(IOException ioe) {
        if (listener != null) {
            listener.onDatabaseException(ioe);
        } else {
            ioe.printStackTrace();
        }
    }

    public interface OperationContext {
        /**
         * Attempts to cancel the store operation. Returns true if the operation
         * could be canceled or false if the operation was already executed by
         * the store.
         * 
         * @return true if the operation could be canceled
         */
        public boolean cancel();

        /**
         * @return true if the operation has been executed
         */
        public boolean cancelled();

        /**
         * @return true if the operation has been executed
         */
        public boolean executed();

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
    public interface Operation extends OperationContext {

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
    abstract class OperationBase extends LinkedNode<OperationBase> implements Operation {
        public boolean flushRequested = false;
        public long opSequenceNumber = -1;

        final protected AtomicBoolean executePending = new AtomicBoolean(true);
        final protected AtomicBoolean cancelled = new AtomicBoolean(false);
        final protected AtomicBoolean executed = new AtomicBoolean(false);

        public static final int BASE_MEM_SIZE = 20;

        public boolean cancel() {
            if (executePending.compareAndSet(true, false)) {
                cancelled.set(true);
                // System.out.println("Cancelled: " + this);
                synchronized (opQueue) {
                    unlink();
                    storeController.elementDispatched(this);
                }
                return true;
            }
            return cancelled.get();
        }

        public final boolean cancelled() {
            return cancelled.get();
        }

        public final boolean executed() {
            return executed.get();
        }

        /**
         * Called when the saver is about to execute the operation. If true is
         * returned the operation can no longer be cancelled.
         * 
         * @return true if operation should be executed
         */
        public final boolean beginExecute() {
            if (executePending.compareAndSet(true, false)) {
                executed.set(true);
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
            synchronized (this) {
                updateFlushPointer(opSequenceNumber);
            }
        }

        public void onCommit() {
        }

        /**
         * Called after {@link #execute(Session)} is called and the the
         * operation has been rolled back.
         */
        public void onRollback(Throwable error) {
            error.printStackTrace();
        }

        public String toString() {
            return "DBOp seq: " + opSequenceNumber + "P/C/E: " + executePending.get() + "/" + cancelled() + "/" + executed();
        }
    }

    private class QueueAddOperation extends OperationBase {

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

        @Override
        public void onCommit() {

        }

        public String toString() {
            return "QueueAdd: " + qd.getQueueName().toString();
        }
    }

    private class QueueDeleteOperation extends OperationBase {

        private QueueDescriptor qd;

        QueueDeleteOperation(QueueDescriptor queue) {
            qd = queue;
        }

        @Override
        protected void doExcecute(Session session) {
            session.queueRemove(qd);
        }

        @Override
        public void onCommit() {

        }

        public String toString() {
            return "QueueDelete: " + qd.getQueueName().toString();
        }
    }

    private class DeleteMessageOperation extends OperationBase {
        private final long storeTracking;
        private QueueDescriptor queue;

        public DeleteMessageOperation(long tracking, QueueDescriptor queue) {
            this.storeTracking = tracking;
            this.queue = queue;
        }

        @Override
        public int getLimiterSize() {
            // Might consider bumping this up to avoid too much accumulation?
            return BASE_MEM_SIZE + 8;
        }

        @Override
        protected void doExcecute(Session session) {
            try {
                session.queueRemoveMessage(queue, storeTracking);
            } catch (KeyNotFoundException e) {
                // TODO Probably doesn't always mean an error, it is possible
                // that
                // the queue has been deleted, in which case its messages will
                // have been deleted, too.
                e.printStackTrace();
            }
        }

        @Override
        public void onCommit() {

        }

        public String toString() {
            return "MessageDelete: " + queue.getQueueName().toString() + " tracking: " + storeTracking + " " + super.toString();
        }
    }

    private class RestoreMessageOperation extends OperationBase {
        private QueueDescriptor queue;
        private long firstKey;
        private int maxRecords;
        private long maxSequence;
        private boolean recordsOnly;
        private RestoreListener<MessageDelivery> listener;
        private Collection<RestoredElement<MessageDelivery>> msgs = null;

        RestoreMessageOperation(QueueDescriptor queue, boolean recordsOnly, long firstKey, int maxRecords, long maxSequence, RestoreListener<MessageDelivery> listener) {
            this.queue = queue;
            this.recordsOnly = recordsOnly;
            this.firstKey = firstKey;
            this.maxRecords = maxRecords;
            this.maxSequence = maxSequence;
            this.listener = listener;
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
                msgs = new LinkedList<RestoredElement<MessageDelivery>>();
            } catch (KeyNotFoundException e) {
                msgs = new ArrayList<RestoredElement<MessageDelivery>>(0);
                return;
            }

            QueueRecord qRecord = null;
            int count = 0;
            if (records.hasNext()) {
                qRecord = records.next();
            }

            while (qRecord != null) {
                RestoredMessageImpl rm = new RestoredMessageImpl();
                // TODO should update jms redelivery here.
                rm.qRecord = qRecord;
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
                        rm.handler = protocolHandlers.get(rm.mRecord.getEncoding().toString());
                        if (rm.handler == null) {
                            try {
                                rm.handler = ProtocolHandlerFactory.createProtocolHandler(rm.mRecord.getEncoding().toString());
                                protocolHandlers.put(rm.mRecord.getEncoding().toString(), rm.handler);
                            } catch (Throwable thrown) {
                                throw new RuntimeException("Unknown message format" + rm.mRecord.getEncoding().toString(), thrown);
                            }
                        }
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
        }

        public String toString() {
            return "MessageRestore: " + queue.getQueueName().toString() + " first: " + firstKey + " max: " + maxRecords;
        }
    }

    private class AddMessageOperation extends OperationBase {

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
        }

        public String toString() {
            return "AddOperation " + delivery.getStoreTracking() + super.toString();
        }
    }

    private class RestoredMessageImpl implements RestoredElement<MessageDelivery> {
        QueueRecord qRecord;
        MessageRecord mRecord;
        ProtocolHandler handler;
        long nextSequence;

        public MessageDelivery getElement() throws IOException {
            if (mRecord == null) {
                return null;
            }

            BrokerMessageDelivery delivery = handler.createMessageDelivery(mRecord);
            delivery.setFromDatabase(BrokerDatabase.this, mRecord);
            return delivery;
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
            // TODO Auto-generated method stub
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
        // TODO Auto-generated method stub
        return store.allocateStoreTracking();
    }

}
