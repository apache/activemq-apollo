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
import java.util.concurrent.CancellationException;
import java.util.concurrent.Semaphore;

import org.apache.activemq.broker.BrokerMessageDelivery;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.protocol.ProtocolHandler;
import org.apache.activemq.broker.protocol.ProtocolHandlerFactory;
import org.apache.activemq.broker.store.Store.Callback;
import org.apache.activemq.broker.store.Store.Session;
import org.apache.activemq.broker.store.Store.Session.MessageRecord;
import org.apache.activemq.broker.store.Store.Session.KeyNotFoundException;
import org.apache.activemq.broker.store.Store.Session.QueueRecord;
import org.apache.activemq.broker.store.memory.MemoryStore;
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.queue.ExclusiveQueue;
import org.apache.activemq.queue.IPollableFlowSource;
import org.apache.activemq.queue.PersistentQueue;
import org.apache.activemq.queue.IPollableFlowSource.FlowReadyListener;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicBoolean;

public class BrokerDatabase {

    private Store store = new MemoryStore();
    private final Flow databaseFlow = new Flow("database", false);

    private final SizeLimiter<Operation> storeLimiter;
    private Thread flushThread;
    private final ExclusiveQueue<Operation> opQueue;
    private AtomicBoolean running = new AtomicBoolean(false);
    private final Semaphore opsReady = new Semaphore(0);
    private final FlowReadyListener<Operation> enqueueListener;
    private DatabaseListener listener;

    private HashMap<String, ProtocolHandler> protocolHandlers = new HashMap<String, ProtocolHandler>();

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
        MessageDelivery getMessageDelivery();
    }

    public interface MessageRestoreListener {
        public void messagesRestored(Collection<RestoredMessage> msgs);
    }

    public BrokerDatabase() {
        storeLimiter = new SizeLimiter<Operation>(1024 * 512, 0) {
            public int getElementSize(Operation op) {
                return op.getLimiterSize();
            }
        };
        opQueue = new ExclusiveQueue<Operation>(databaseFlow, "DataBaseQueue", storeLimiter);
        enqueueListener = new FlowReadyListener<Operation>() {

            public void onFlowReady(IPollableFlowSource<Operation> source) {
                opsReady.release();
            }
        };
    }

    public synchronized void start() {
        if (flushThread == null) {

            running.set(true);
            opsReady.drainPermits();
            flushThread = new Thread(new Runnable() {

                public void run() {
                    processOps();
                }

            }, "StoreThread");
            flushThread.start();
        }
    }

    public synchronized void stop() {
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

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
            flushThread = null;
        }
    }

    /**
     * Saves a message for all of the recipients in the
     * {@link BrokerMessageDelivery}.
     * 
     * @param delivery
     *            The delivery.
     * @param source
     *            The source's controller.
     */
    public void persistReceivedMessage(BrokerMessageDelivery delivery, ISourceController<?> source) {
        add(new AddMessageOperation(delivery), source, true);
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
     */
    public void saveMessage(MessageDelivery delivery, PersistentQueue<MessageDelivery> queue, ISourceController<?> source) {
        add(new AddMessageOperation(delivery, queue), source, false);
    }

    /**
     * Deletes the given message from the store for the given queue.
     * 
     * @param delivery
     *            The delivery.
     * @param queue
     *            The queue.
     */
    public void deleteMessage(MessageDelivery delivery, PersistentQueue<MessageDelivery> queue) {
        opQueue.add(new DeleteMessageOperation(delivery, queue), null);
    }

    public void restoreMessages(PersistentQueue<MessageDelivery> queue, long first, int max, MessageRestoreListener listener) {
        opQueue.add(new RestoreMessageOperation(queue, first, max, listener), null);
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
     */
    private void add(Operation op, ISourceController<?> controller, boolean flush) {
        opQueue.add(op, controller);
    }

    private final void processOps() {
        while (running.get()) {
            final Operation firstOp;
            synchronized (opQueue) {
                firstOp = opQueue.poll();
                if (firstOp == null) {
                    opQueue.addFlowReadyListener(enqueueListener);
                    opsReady.acquireUninterruptibly();
                    continue;
                }
            }

            // The first operation we get, triggers a store transaction.
            if (firstOp != null) {
                final ArrayList<Operation> processedQueue = new ArrayList<Operation>();
                try {
                    store.execute(new Store.VoidCallback<Exception>() {
                        @Override
                        public void run(Session session) throws Exception {

                            // Try to execute the operation against the
                            // session...
                            try {
                                firstOp.execute(session);
                                processedQueue.add(firstOp);
                            } catch (CancellationException ignore) {
                            }

                            // See if we can batch up some additional operations
                            // in
                            // this transaction.

                            Operation op;
                            synchronized (opQueue) {
                                op = opQueue.poll();
                                if (op != null) {
                                    try {
                                        firstOp.execute(session);
                                        processedQueue.add(op);
                                    } catch (CancellationException ignore) {
                                    }
                                }
                            }
                        }
                    }, null);
                    // Wait for the operations to commit.
                    for (Operation processed : processedQueue) {
                        processed.onCommit();
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

    private void onDatabaseException(IOException ioe) {
        if (listener != null) {
            listener.onDatabaseException(ioe);
        }
    }

    /**
     * This interface is used to execute transacted code.
     * 
     * It is used by the {@link Store#execute(Callback)} method, often as
     * anonymous class.
     */
    public interface Operation {

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
         * @throws CancellationException
         *             if the operation has been canceled. If this is thrown,
         *             the {@link #onCommit()} and {@link #onRollback()} methods
         *             will not be called.
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
         * Attempts to cancel the store operation. Returns true if the operation
         * could be canceled or false if the operation was already executed by
         * the store.
         * 
         * @return true if the operation could be canceled
         */
        public boolean cancel();

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
    public abstract class OperationBase implements Operation {
        final private AtomicBoolean executePending = new AtomicBoolean(true);

        public boolean cancel() {
            return executePending.compareAndSet(true, false);
        }

        public void execute(Session session) throws CancellationException {
            if (executePending.compareAndSet(true, false)) {
                doExcecute(session);
            } else {
                throw new CancellationException();
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

        }
    }

    private class DeleteMessageOperation extends OperationBase {
        private final MessageDelivery delivery;
        private PersistentQueue<MessageDelivery> queue;

        public DeleteMessageOperation(MessageDelivery delivery, PersistentQueue<MessageDelivery> queue) {
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
                session.queueRemoveMessage(queue.getPeristentQueueName(), delivery.getStoreTracking());
            } catch (KeyNotFoundException e) {
                // TODO Probably doesn't always mean an error, it is possible
                // that
                // the queue has been deleted, in which case its messages will
                // have been deleted, too.
                e.printStackTrace();
            }
        }

        @Override
        public void onRollback(Throwable error) {
        }

        @Override
        public void onCommit() {
            delivery.onMessagePersisted();
        }
    }

    private class RestoreMessageOperation extends OperationBase {
        private PersistentQueue<MessageDelivery> queue;
        private long firstKey;
        private int maxRecords;
        private MessageRestoreListener listener;
        private Collection<RestoredMessage> msgs = null;

        RestoreMessageOperation(PersistentQueue<MessageDelivery> queue, long firstKey, int maxRecords, MessageRestoreListener listener) {
            this.queue = queue;
            this.firstKey = firstKey;
            this.maxRecords = maxRecords;
            this.listener = listener;
        }

        @Override
        protected void doExcecute(Session session) {

            Iterator<QueueRecord> records = null;
            try {
                records = session.queueListMessagesQueue(queue.getPeristentQueueName(), firstKey, maxRecords);

            } catch (KeyNotFoundException e) {
                msgs = new ArrayList<RestoredMessage>(0);
                return;
            }

            while (records.hasNext()) {
                RestoredMessageImpl rm = new RestoredMessageImpl();
                // TODO should update jms redelivery here.
                rm.qRecord = records.next();
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
            }
        }

        @Override
        public void onRollback(Throwable error) {
        }

        @Override
        public void onCommit() {
            listener.messagesRestored(msgs);
        }
    }

    private class AddMessageOperation extends OperationBase {

        private final BrokerMessageDelivery brokerDelivery;

        private final MessageDelivery delivery;
        private final PersistentQueue<MessageDelivery> target;

        public AddMessageOperation(BrokerMessageDelivery delivery) {
            this.brokerDelivery = delivery;
            this.delivery = delivery;
            target = null;
        }

        public AddMessageOperation(MessageDelivery delivery, PersistentQueue<MessageDelivery> target) {
            this.brokerDelivery = null;
            this.delivery = delivery;
            this.target = target;
        }

        @Override
        public int getLimiterSize() {
            return delivery.getFlowLimiterSize();
        }

        @Override
        protected void doExcecute(Session session) {

            if (target == null) {
                MessageRecord record = delivery.createMessageRecord();
                Long key = session.messageAdd(record);
                brokerDelivery.beginStore(key);

                for (PersistentQueue<MessageDelivery> target : brokerDelivery.getPersistentQueues()) {
                    try {
                        Session.QueueRecord queueRecord = new Session.QueueRecord();
                        queueRecord.setAttachment(null);
                        queueRecord.setMessageKey(key);
                        session.queueAddMessage(target.getPeristentQueueName(), queueRecord);

                    } catch (KeyNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            } else {

                MessageRecord record = delivery.createMessageRecord();
                Long key = session.messageAdd(record);
                try {
                    Session.QueueRecord queueRecord = new Session.QueueRecord();
                    queueRecord.setAttachment(null);
                    queueRecord.setMessageKey(key);
                    session.queueAddMessage(target.getPeristentQueueName(), queueRecord);
                } catch (KeyNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }

        @Override
        public void onRollback(Throwable error) {
            error.printStackTrace();
        }

        @Override
        public void onCommit() {
            delivery.onMessagePersisted();
        }

    }

    private class RestoredMessageImpl implements RestoredMessage {
        QueueRecord qRecord;
        MessageRecord mRecord;
        ProtocolHandler handler;

        public MessageDelivery getMessageDelivery() {
            return handler.createMessageDelivery(mRecord);
        }
    }
}
