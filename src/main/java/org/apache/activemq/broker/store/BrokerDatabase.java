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
package org.apache.activemq.broker.store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.broker.BrokerMessageDelivery;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.protocol.ProtocolHandler;
import org.apache.activemq.broker.protocol.ProtocolHandlerFactory;
import org.apache.activemq.broker.store.Store.Callback;
import org.apache.activemq.broker.store.Store.FatalStoreException;
import org.apache.activemq.broker.store.Store.KeyNotFoundException;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.broker.store.Store.QueueRecord;
import org.apache.activemq.broker.store.Store.Session;
import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.flow.AbstractLimitedFlowResource;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.FlowController;
import org.apache.activemq.flow.IFlowResource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.flow.ISinkController.FlowControllable;
import org.apache.activemq.protobuf.AsciiBuffer;

public class BrokerDatabase extends AbstractLimitedFlowResource<BrokerDatabase.Operation> {

    private final Store store;
    private final Flow databaseFlow = new Flow("database", false);

    private final SizeLimiter<OperationBase> storeLimiter;
    private final FlowController<OperationBase> storeController;

    private final IDispatcher dispatcher;
    private Thread flushThread;
    private final ConcurrentLinkedQueue<OperationBase> opQueue;
    private AtomicBoolean running = new AtomicBoolean(false);
    private DatabaseListener listener;

    private HashMap<String, ProtocolHandler> protocolHandlers = new HashMap<String, ProtocolHandler>();
    private AtomicBoolean notify = new AtomicBoolean(false);
    private Semaphore opsReady = new Semaphore(0);
    private long opSequenceNumber;
    private long flushPointer = 0; // The last seq num for which flush was
    // requested
    private long requestedDelayedFlushPointer = -1; // Set to the last sequence
    // num scheduled for delay
    private long delayedFlushPointer = 0; // The last delayable sequence num
    // requested.
    private final long FLUSH_DELAY_MS = 5;
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

    /**
     * Holder of a restored message to be passed to a
     * {@link MessageRestoreListener}. This allows the demarshalling to be done
     * by the listener instead of the the database worker.
     * 
     * @author cmacnaug
     */
    public interface RestoredMessage {
        MessageDelivery getMessageDelivery() throws IOException;
    }

    public interface MessageRestoreListener {
        public void messagesRestored(Collection<RestoredMessage> msgs);
    }

    public BrokerDatabase(Store store, IDispatcher dispatcher) {
        this.store = store;
        this.dispatcher = dispatcher;
        this.opQueue = new ConcurrentLinkedQueue<OperationBase>();
        storeLimiter = new SizeLimiter<OperationBase>(5000, 0) {

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

    private synchronized final void addToOpQueue(OperationBase op) {
        if (!running.get()) {
            throw new IllegalStateException("BrokerDatabase not started");
        }
        op.opSequenceNumber = opSequenceNumber++;
        opQueue.add(op);
        if (op.flushRequested) {
            if (op.isDelayable() && FLUSH_DELAY_MS > 0) {
                scheduleDelayedFlush(op.opSequenceNumber);
            } else {
                updateFlushPointer(op.opSequenceNumber);
            }
        }
        if (notify.get()) {
            opsReady.release();
        }
    }

    private void updateFlushPointer(long seqNumber) {
        if (seqNumber > flushPointer) {
            if (notify.get()) {
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

    private synchronized final void flushDelayCallback() {
        if (flushPointer < requestedDelayedFlushPointer) {
            updateFlushPointer(requestedDelayedFlushPointer);
            requestedDelayedFlushPointer = -1;
            // Schedule next delay if needed:
            scheduleDelayedFlush(delayedFlushPointer);
        }
    }

    private final OperationBase getNextOp(boolean wait) {
        if (!wait) {
            return opQueue.poll();
        } else {
            OperationBase op = opQueue.poll();
            if (op == null) {
                notify.set(true);
                op = opQueue.poll();
                try {
                    while (running.get() && op == null) {
                        opsReady.acquireUninterruptibly();
                        op = opQueue.poll();
                    }
                } finally {
                    notify.set(false);
                    opsReady.drainPermits();
                }
            }
            return op;
        }
    }

    private class OpCounter {
        int count = 0;
    }

    private final void processOps() {
        final OpCounter counter = new OpCounter();

        while (running.get()) {
            final OperationBase firstOp = getNextOp(true);
            if (firstOp == null) {
                continue;
            }
            counter.count = 1;

            // The first operation we get, triggers a store transaction.
            if (firstOp != null) {
                final LinkedList<Operation> processedQueue = new LinkedList<Operation>();
                try {

                    // TODO the recursion here leads to a rather large stack,
                    // refactor.

                    executeOps(firstOp, processedQueue, counter);

                    // If we procecessed some ops, flush and post process:
                    if (!processedQueue.isEmpty()) {
                        // Sync the store:
                        store.flush();

                        // Post process operations
                        long release = 0;
                        for (Operation processed : processedQueue) {
                            processed.onCommit();
                            release += processed.getLimiterSize();
                        }

                        synchronized (opQueue) {
                            this.storeLimiter.remove(release);
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
                } catch (Exception e) {
                    for (Operation processed : processedQueue) {
                        processed.onRollback(e);
                    }

                }
            }
        }
    }

    private final void executeOps(final OperationBase op, final LinkedList<Operation> processedQueue, final OpCounter counter) throws FatalStoreException, Exception {
        store.execute(new Store.VoidCallback<Exception>() {
            @Override
            public void run(Session session) throws Exception {

                // Try to execute the operation against the
                // session...
                try {
                    if (op.execute(session)) {
                        processedQueue.add(op);
                    } else {
                        counter.count--;
                    }
                } catch (CancellationException ignore) {
                    System.out.println("Cancelled" + op);
                }

                // See if we can batch up some additional operations
                // in this transaction.
                if (counter.count < 100) {
                    OperationBase next = getNextOp(false);
                    if (next != null) {
                        counter.count++;
                        executeOps(next, processedQueue, counter);
                    }
                }
            }
        }, null);
    }

    /**
     * Adds a queue to the database
     * 
     * @param queue
     *            The queue to add.
     */
    public void addQueue(AsciiBuffer queue) {
        add(new QueueAddOperation(queue), null, false);
    }

    /**
     * Deletes a queue and all of its messages from the database
     * 
     * @param queue
     *            The queue to delete.
     */
    public void deleteQueue(AsciiBuffer queue) {
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
    public OperationContext persistReceivedMessage(BrokerMessageDelivery delivery, ISourceController<?> source) throws IOException {
        return add(new AddMessageOperation(delivery), source, delivery.isFlushDelayable());
    }

    /**
     * Saves a Message for a single queue.
     * 
     * @param delivery
     *            The delivery
     * @param queue
     *            The queue
     * @param source
     *            The source initiating the save or null, if there isn't one.
     * @throws IOException
     *             If there is an error marshalling the message.
     * 
     * @return The {@link OperationContext} associated with the operation
     */
    public OperationContext saveMessage(MessageDelivery delivery, AsciiBuffer queue, ISourceController<?> source) throws IOException {
        return add(new AddMessageOperation(delivery, queue), source, false);
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
    public OperationContext deleteMessage(MessageDelivery delivery, AsciiBuffer queue) {
        return add(new DeleteMessageOperation(delivery, queue), null, false);
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
     * @param first
     *            The first queue sequence number to load.
     * @param max
     *            The maximum number of messages to load.
     * @param listener
     *            The listener to which messags should be passed.
     * @return The {@link OperationContext} associated with the operation
     */
    public OperationContext restoreMessages(AsciiBuffer queue, long first, int max, MessageRestoreListener listener) {
        return add(new RestoreMessageOperation(queue, first, max, listener), null, false);
    }

    private void onDatabaseException(IOException ioe) {
        if (listener != null) {
            listener.onDatabaseException(ioe);
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

    }

    /**
     * This interface is used to execute transacted code.
     * 
     * It is used by the {@link Store#execute(Callback)} method, often as
     * anonymous class.
     */
    public interface Operation extends OperationContext {

        /**
         * Gets called by the
         * {@link Store#add(Operation, ISourceController, boolean)} method
         * within a transactional context. If any exception is thrown including
         * Runtime exception, the transaction is rolled back.
         * 
         * @param session
         *            provides you access to read and update the persistent
         *            data.
         * @return the result of the CallableCallback
         * @throws Exception
         *             if an system error occured while executing the
         *             operations.
         * @throws RuntimeException
         *             if an system error occured while executing the
         *             operations.
         */
        public boolean execute(Session session) throws CancellationException, Exception, RuntimeException;

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
    private abstract class OperationBase implements Operation {
        public boolean flushRequested = false;
        public long opSequenceNumber = -1;

        final protected AtomicBoolean executePending = new AtomicBoolean(true);
        final protected AtomicBoolean cancelled = new AtomicBoolean(false);
        final protected AtomicBoolean executed = new AtomicBoolean(false);

        public boolean cancel() {
            if (executePending.compareAndSet(true, false)) {
                cancelled.set(true);
                //System.out.println("Cancelled: " + this);
                synchronized (opQueue) {
                    opQueue.remove(this);
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
         * Gets called by the
         * {@link Store#add(Operation, ISourceController, boolean)} method
         * within a transactional context. If any exception is thrown including
         * Runtime exception, the transaction is rolled back.
         * 
         * @param session
         *            provides you access to read and update the persistent
         *            data.
         * @return True if processed, false otherwise
         * @throws Exception
         *             if an system error occured while executing the
         *             operations.
         * @throws RuntimeException
         *             if an system error occured while executing the
         *             operations.
         */
        public boolean execute(Session session) throws Exception, RuntimeException {
            if (executePending.compareAndSet(true, false)) {
                executed.set(true);
                doExcecute(session);
                return true;
            } else {
                return false;
            }
        }

        abstract protected void doExcecute(Session session);

        public int getLimiterSize() {
            return 0;
        }

        public boolean isDelayable() {
            return false;
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
    }

    private class QueueAddOperation extends OperationBase {

        private AsciiBuffer queue;

        QueueAddOperation(AsciiBuffer queue) {
            this.queue = queue;
        }

        @Override
        public int getLimiterSize() {
            // Might consider bumping this up to avoid too much accumulation?
            return 0;
        }

        @Override
        protected void doExcecute(Session session) {
            session.queueAdd(queue);
        }

        @Override
        public void onCommit() {

        }

        public String toString() {
            return "QueueAdd: " + queue.toString();
        }
    }

    private class QueueDeleteOperation extends OperationBase {

        private AsciiBuffer queue;

        QueueDeleteOperation(AsciiBuffer queue) {
            this.queue = queue;
        }

        @Override
        public int getLimiterSize() {
            // Might consider bumping this up to avoid too much accumulation?
            return 0;
        }

        @Override
        protected void doExcecute(Session session) {
            session.queueRemove(queue);
        }

        @Override
        public void onCommit() {

        }
        
        public String toString() {
            return "QueueDelete: " + queue.toString();
        }
    }

    private class DeleteMessageOperation extends OperationBase {
        private final MessageDelivery delivery;
        private AsciiBuffer queue;

        public DeleteMessageOperation(MessageDelivery delivery, AsciiBuffer queue) {
            this.delivery = delivery;
            this.queue = queue;
        }

        @Override
        public int getLimiterSize() {
            // Might consider bumping this up to avoid too much accumulation?
            return 0;
        }

        @Override
        protected void doExcecute(Session session) {
            try {
                session.queueRemoveMessage(queue, delivery.getStoreTracking());
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
            return "MessageDelete: " + queue.toString() + delivery.getStoreTracking();
        }
    }

    private class RestoreMessageOperation extends OperationBase {
        private AsciiBuffer queue;
        private long firstKey;
        private int maxRecords;
        private MessageRestoreListener listener;
        private Collection<RestoredMessage> msgs = null;

        RestoreMessageOperation(AsciiBuffer queue, long firstKey, int maxRecords, MessageRestoreListener listener) {
            this.queue = queue;
            this.firstKey = firstKey;
            this.maxRecords = maxRecords;
            this.listener = listener;
        }

        @Override
        protected void doExcecute(Session session) {

            Iterator<QueueRecord> records = null;
            try {
                records = session.queueListMessagesQueue(queue, firstKey, maxRecords);

            } catch (KeyNotFoundException e) {
                msgs = new ArrayList<RestoredMessage>(0);
                return;
            }

            while (records.hasNext()) {
                RestoredMessageImpl rm = new RestoredMessageImpl();
                // TODO should update jms redelivery here.
                rm.qRecord = records.next();
                try {
                    rm.mRecord = session.messageGetRecord(rm.qRecord.messageKey);
                    rm.handler = protocolHandlers.get(rm.mRecord.encoding.toString());
                    if (rm.handler == null) {
                        try {
                            rm.handler = ProtocolHandlerFactory.createProtocolHandler(rm.mRecord.encoding.toString());
                            protocolHandlers.put(rm.mRecord.encoding.toString(), rm.handler);
                        } catch (Throwable thrown) {
                            throw new RuntimeException("Unknown message format" + rm.mRecord.encoding.toString(), thrown);
                        }
                    }
                } catch (KeyNotFoundException shouldNotHappen) {
                }
            }
        }

        @Override
        public void onCommit() {
            listener.messagesRestored(msgs);
        }
        
        public String toString() {
            return "MessageRestore: " + queue.toString() + " first: " + firstKey + " max: " + maxRecords;
        }
    }

    private class AddMessageOperation extends OperationBase {

        private final BrokerMessageDelivery brokerDelivery;

        private final MessageDelivery delivery;
        private final AsciiBuffer target;
        private final MessageRecord record;

        private final boolean delayable;

        public AddMessageOperation(BrokerMessageDelivery delivery) throws IOException {
            this.brokerDelivery = delivery;
            this.delivery = delivery;
            target = null;
            this.record = delivery.createMessageRecord();
            this.delayable = delivery.isFlushDelayable();
        }

        public AddMessageOperation(MessageDelivery delivery, AsciiBuffer target) throws IOException {
            this.brokerDelivery = null;
            this.delivery = delivery;
            this.target = target;
            this.record = delivery.createMessageRecord();
            delayable = false;
        }

        public boolean isDelayable() {
            return delayable;
        }
        
        @Override
        public int getLimiterSize() {
            return delivery.getFlowLimiterSize();
        }
        
        @Override
        protected void doExcecute(Session session) {

            if (target == null) {
                brokerDelivery.beginStore();
                Collection<AsciiBuffer> targets = brokerDelivery.getPersistentQueues();

                if (!targets.isEmpty()) {
                    record.setKey(delivery.getStoreTracking());
                    session.messageAdd(record);

                    for (AsciiBuffer target : brokerDelivery.getPersistentQueues()) {
                        try {
                            QueueRecord queueRecord = new QueueRecord();
                            queueRecord.setAttachment(null);
                            queueRecord.setMessageKey(record.getKey());
                            session.queueAddMessage(target, queueRecord);

                        } catch (KeyNotFoundException e) {
                            e.printStackTrace();
                        }
                    }
                } else {
                    // Save with no targets must have been cancelled:
                    // System.out.println("Skipping save for " +
                    // delivery.getStoreTracking());
                }
            } else {

                Long key = session.messageAdd(record);
                try {
                    QueueRecord queueRecord = new QueueRecord();
                    queueRecord.setAttachment(null);
                    queueRecord.setMessageKey(key);
                    session.queueAddMessage(target, queueRecord);
                } catch (KeyNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void onCommit() {
            delivery.onMessagePersisted();
        }

        public String toString() {
            return "AddOperation " + delivery.getStoreTracking() + " seq: " + opSequenceNumber + "P/C/E" + super.executePending.get() + "/" + cancelled() + "/" + executed();
        }
    }

    private class RestoredMessageImpl implements RestoredMessage {
        QueueRecord qRecord;
        MessageRecord mRecord;
        ProtocolHandler handler;

        public MessageDelivery getMessageDelivery() throws IOException {
            return handler.createMessageDelivery(mRecord);
        }
    }

    public long allocateStoreTracking() {
        // TODO Auto-generated method stub
        return store.allocateStoreTracking();
    }
}
