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
package org.apache.activemq.apollo.broker

import _root_.java.util.{LinkedHashMap, HashMap}




class TransactionManagerX() {

  var virtualHost:VirtualHost = null

// TODO:
//    private static final Log LOG = LogFactory.getLog(TransactionManager.class);
//    private static final String TX_QUEUE_PREFIX = "TX-";
//    private static final AsciiBuffer TXN_MAP = new AsciiBuffer("TXMAP");
//
//    private final HashMap<Long, Transaction> transactions = new HashMap<Long, Transaction>();
//    private final HashMap<AsciiBuffer, Transaction> transactionsByQueue = new HashMap<AsciiBuffer, Transaction>();
//    private final HashMap<Buffer, XATransaction> xaTransactions = new HashMap<Buffer, XATransaction>();
//
//    private final VirtualHost host;
//    private BrokerDatabase database;
//
//    private final AtomicLong tidGen = new AtomicLong(0);
//    private final TransactionStore txStore;
//
//    private static final int DEFAULT_TX_QUEUE_PAGING_THRESHOLD = 1024 * 64;
//    private static final int DEFAULT_TX_QUEUE_RESUME_THRESHOLD = 1;
//    // Be default we don't page out elements to disk.
//    private static final int DEFAULT_TX_QUEUE_SIZE = DEFAULT_TX_QUEUE_PAGING_THRESHOLD;
//    //private static final int DEFAULT_TX_QUEUE_SIZE = Integer.MAX_VALUE;
//
//    private static final PersistencePolicy<TxOp> DEFAULT_TX_QUEUE_PERSISTENCE_POLICY = new PersistencePolicy<TxOp>() {
//
//        private static final boolean PAGING_ENABLED = DEFAULT_TX_QUEUE_SIZE > DEFAULT_TX_QUEUE_PAGING_THRESHOLD;
//
//        public boolean isPersistent(TxOp elem) {
//            return elem.isPersistent();
//        }
//
//        public boolean isPageOutPlaceHolders() {
//            return false;
//        }
//
//        public boolean isPagingEnabled() {
//            return PAGING_ENABLED;
//        }
//
//        public int getPagingInMemorySize() {
//            return DEFAULT_TX_QUEUE_PAGING_THRESHOLD;
//        }
//
//        public boolean isThrottleSourcesToMemoryLimit() {
//            // Keep the queue in memory.
//            return true;
//        }
//
//        public int getDisconnectedThrottleRate() {
//            // By default don't throttle consumers when disconnected.
//            return 0;
//        }
//
//        public int getRecoveryBias() {
//            return 8;
//        }
//    };
//
//    private static final Mapper<Long, TxOp> EXPIRATION_MAPPER = new Mapper<Long, TxOp>() {
//        public Long map(TxOp element) {
//            return element.getExpiration();
//        }
//    };
//
//    private static final Mapper<Integer, TxOp> SIZE_MAPPER = new Mapper<Integer, TxOp>() {
//        public Integer map(TxOp element) {
//            return element.getLimiterSize();
//        }
//    };
//
//    private static final Mapper<Integer, TxOp> PRIORITY_MAPPER = new Mapper<Integer, TxOp>() {
//        public Integer map(TxOp element) {
//            return element.getPriority();
//        }
//    };
//
//    private static final Mapper<Long, TxOp> KEY_MAPPER = new Mapper<Long, TxOp>() {
//        public Long map(TxOp element) {
//            return element.getStoreTracking();
//        }
//    };
//
//    private static final Mapper<Integer, TxOp> PARTITION_MAPPER = new Mapper<Integer, TxOp>() {
//        public Integer map(TxOp element) {
//            return 1;
//        }
//    };
//
//    TransactionManager(VirtualHost host) {
//        this.host = host;
//        txStore = new TransactionStore(host.getDatabase());
//        database = host.getDatabase();
//    }
//
//    /**
//     * @return The TM's virtual host
//     */
//    public final VirtualHost getVirtualHost() {
//        return host;
//    }
//
//    /**
//     * @param msg
//     * @param controller
//     */
//    public void newMessage(MessageDelivery msg, ISourceController<?> controller) {
//        if (msg.getStoreTracking() == -1) {
//            msg.setStoreTracking(host.getDatabase().allocateStoreTracking());
//        }
//        transactions.get(msg.getTransactionId()).addMessage(msg, controller);
//    }
//
//    /**
//     * Creates a transaction.
//     *
//     * @param xid
//     * @return
//     */
//    public synchronized final Transaction createTransaction(Buffer xid) {
//        Transaction ret;
//
//        long tid = tidGen.incrementAndGet();
//        IQueue<Long, TxOp> opQueue = createTransactionQueue(tid);
//
//        if (xid == null) {
//            ret = new LocalTransaction(this, tid, opQueue);
//        } else {
//            XATransaction xat = new XATransaction(this, tid, xid, opQueue);
//            ret = xat;
//            xaTransactions.put(xid, xat);
//        }
//
//        transactionsByQueue.put(opQueue.getDescriptor().getQueueName(), ret);
//        transactions.put(ret.getTid(), ret);
//
//        return ret;
//    }
//
//    /**
//     * @param buffer
//     * @return
//     */
//    public synchronized Transaction getXATransaction(Buffer buffer) {
//        return xaTransactions.get(buffer);
//    }
//
//    /**
//     *
//     * @throws Exception
//     */
//    public synchronized void loadTransactions() throws Exception {
//
//        tidGen.set(database.allocateStoreTracking());
//
//        Map<AsciiBuffer, Buffer> txns = database.listMapEntries(TXN_MAP);
//
//        // Load shared queues
//        Iterator<QueueQueryResult> results = database.listQueues(BrokerQueueStore.TRANSACTION_QUEUE_TYPE);
//        while (results.hasNext()) {
//            QueueQueryResult loaded = results.next();
//
//            Buffer b = txns.remove(loaded.getDescriptor().getQueueName());
//            if (b == null) {
//                LOG.warn("Recovered orphaned transaction queue: " + loaded.getDescriptor() + " elements: " + loaded.getCount());
//                database.deleteQueue(loaded.getDescriptor());
//            }
//
//            IQueue<Long, TxOp> queue = createRestoredTxQueue(loaded);
//            Transaction tx = loadTransaction(b, queue);
//
//            //TODO if we recover a tx that isn't committed then, we should discard it.
//            if (tx.getState() < Transaction.COMMITED_STATE) {
//                LOG.warn("Recovered unfinished transaction: " + tx);
//            }
//            transactions.put(tx.getTid(), tx);
//            if (tx instanceof XATransaction) {
//                XATransaction xat = XATransaction.class.cast(tx);
//                xaTransactions.put(xat.getXid(), xat);
//            }
//
//            LOG.info("Loaded Queue " + queue.getResourceName() + " Messages: " + queue.getEnqueuedCount() + " Size: " + queue.getEnqueuedSize());
//        }
//
//        if (!txns.isEmpty()) {
//            //TODO Based on transaction state this is generally ok, anyway the orphaned entries should be
//            //deleted:
//            LOG.warn("Recovered transactions without backing queues: " + txns.keySet());
//        }
//    }
//
//    private Transaction loadTransaction(Buffer b, IQueue<Long, TxOp> queue) throws IOException {
//        //TODO move the serialization into the transaction itself:
//        DataByteArrayInputStream bais = new DataByteArrayInputStream(b.getData());
//        byte type = bais.readByte();
//        byte state = bais.readByte();
//        long tid = bais.readLong();
//
//        Transaction tx = null;
//        switch (type) {
//        case Transaction.TYPE_LOCAL:
//            tx = new LocalTransaction(this, tid, queue);
//            break;
//        case Transaction.TYPE_XA:
//            int length = bais.readByte() & 0xFF;
//            Buffer xid = new Buffer(new byte[length]);
//            bais.readFully(xid.data);
//            tx = new XATransaction(this, tid, xid, queue);
//            break;
//        default:
//            throw new IOException("Invalid transaction type: " + type);
//
//        }
//        tx.setState(state, null);
//        return tx;
//
//    }
//
//    public ListenableFuture<?> persistTransaction(Transaction tx) {
//
//        //TODO move the serialization into the transaction itself:
//        DataByteArrayOutputStream baos = new DataByteArrayOutputStream();
//        try {
//            baos.writeByte(tx.getType());
//            baos.writeByte(tx.getState());
//            baos.writeLong(tx.getTid());
//            if (tx.getType() == Transaction.TYPE_XA) {
//                Buffer xid = ((XATransaction) tx).getXid();
//                // An XID max size is around 140 bytes, byte SHOULD be big enough to frame it.
//                baos.writeByte(xid.length & 0xFF);
//                baos.write(xid.data, xid.offset, xid.length);
//            }
//            OperationContext<?> ctx = database.updateMapEntry(TXN_MAP, tx.getBackingQueueName(), new Buffer(baos.getData(), 0, baos.size()));
//            ctx.requestFlush();
//            return ctx;
//        } catch (IOException ioe) {
//            //Shouldn't happen
//            throw new RuntimeException(ioe);
//        }
//    }
//
//    private IQueue<Long, TxOp> createRestoredTxQueue(QueueQueryResult loaded) throws IOException {
//
//        IQueue<Long, TxOp> queue = createTxQueueInternal(loaded.getDescriptor().getQueueName().toString(), loaded.getDescriptor().getQueueType());
//        queue.initialize(loaded.getFirstSequence(), loaded.getLastSequence(), loaded.getCount(), loaded.getSize());
//        return queue;
//    }
//
//    private final IQueue<Long, TxOp> createTransactionQueue(long tid) {
//        IQueue<Long, TxOp> queue = createTxQueueInternal(TX_QUEUE_PREFIX + tid, BrokerQueueStore.TRANSACTION_QUEUE_TYPE);
//        queue.initialize(0, 0, 0, 0);
//        txStore.addQueue(queue.getDescriptor());
//        return queue;
//    }
//
//    private IQueue<Long, TxOp> createTxQueueInternal(final String name, short type) {
//        ExclusivePersistentQueue<Long, TxOp> queue;
//
//        SizeLimiter<TxOp> limiter = new SizeLimiter<TxOp>(DEFAULT_TX_QUEUE_SIZE, DEFAULT_TX_QUEUE_RESUME_THRESHOLD) {
//            @Override
//            public int getElementSize(TxOp elem) {
//                return elem.getLimiterSize();
//            }
//        };
//        queue = new ExclusivePersistentQueue<Long, TxOp>(name, limiter);
//        queue.setStore(txStore);
//        queue.setPersistencePolicy(DEFAULT_TX_QUEUE_PERSISTENCE_POLICY);
//        queue.setExpirationMapper(EXPIRATION_MAPPER);
//        queue.getDescriptor().setApplicationType(type);
//        return queue;
//    }
//
//    final QueueStore<Long, Transaction.TxOp> getTxnStore() {
//        return txStore;
//    }
//
//    private class TransactionStore implements QueueStore<Long, Transaction.TxOp> {
//        private final BrokerDatabase database;
//
//        private final BrokerDatabase.MessageRecordMarshaller<TxOp> TX_OP_MARSHALLER = new BrokerDatabase.MessageRecordMarshaller<TxOp>() {
//            public MessageRecord marshal(TxOp element) {
//                return element.createMessageRecord();
//            }
//
//            public TxOp unMarshall(MessageRecord record, QueueDescriptor queue) {
//                Transaction t = transactionsByQueue.get(queue.getQueueName());
//                return Transaction.createTxOp(record, t);
//            }
//        };
//
//        TransactionStore(BrokerDatabase database) {
//            this.database = database;
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.queue.QueueStore#addQueue(org.apache.activemq
//         * .queue.QueueDescriptor)
//         */
//        public void addQueue(QueueDescriptor queue) {
//            database.addQueue(queue);
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.queue.QueueStore#deleteQueue(org.apache.activemq
//         * .queue.QueueDescriptor)
//         */
//        public void deleteQueue(QueueDescriptor queue) {
//            database.deleteQueue(queue);
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.queue.QueueStore#deleteQueueElement(org.apache
//         * .activemq.queue.QueueDescriptor, java.lang.Object)
//         */
//        public void deleteQueueElement(SaveableQueueElement<TxOp> sqe) {
//            database.deleteQueueElement(sqe);
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.queue.QueueStore#isFromStore(java.lang.Object)
//         */
//        public boolean isFromStore(TxOp elem) {
//            return elem.isFromStore();
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.queue.QueueStore#persistQueueElement(org.apache
//         * .activemq.queue.SaveableQueueElement,
//         * org.apache.activemq.flow.ISourceController, boolean)
//         */
//        public void persistQueueElement(SaveableQueueElement<TxOp> sqe, ISourceController<?> source, boolean delayable) {
//            database.saveQeueuElement(sqe, source, false, TX_OP_MARSHALLER);
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.queue.QueueStore#restoreQueueElements(org.apache
//         * .activemq.queue.QueueDescriptor, boolean, long, long, int,
//         * org.apache.activemq.queue.RestoreListener)
//         */
//        public void restoreQueueElements(QueueDescriptor queue, boolean recordOnly, long firstSequence, long maxSequence, int maxCount, RestoreListener<TxOp> listener) {
//            database.restoreQueueElements(queue, recordOnly, firstSequence, maxSequence, maxCount, listener, TX_OP_MARSHALLER);
//        }
//    }

    def loadTransactions() = {
    }
}


/**
 * Keeps track of all the actions the need to be done when a transaction does a
 * commit or rollback.
 */
abstract class TransactionX {

// TODO:
//    private static final Log LOG = LogFactory.getLog(Transaction.class);
//
//    public static final byte START_STATE = 0; // can go to: 1,2,3
//    public static final byte IN_USE_STATE = 1; // can go to: 2,3, 4
//    public static final byte PREPARED_STATE = 2; // can go to: 3, 4
//    public static final byte COMMITED_STATE = 3;
//    public static final byte ROLLBACK_STATE = 4;
//
//    static final byte TYPE_LOCAL = 0;
//    static final byte TYPE_XA = 1;
//
//    private byte state = START_STATE;
//    private final TransactionManager manager;
//    private final long tid;
//    private final IQueue<Long, TxOp> opQueue;
//    protected HashSet<TransactionListener> listeners;
//
//    private TxProcessor processor;
//
//    Transaction(TransactionManager manager, long tid, IQueue<Long, TxOp> opQueue) {
//        this.manager = manager;
//        this.opQueue = opQueue;
//        opQueue.start();
//        this.tid = tid;
//    }
//
//    /**
//     * @return the unique identifier used by the {@link TransactionManager} to
//     *         identify this {@link Transaction}
//     *
//     */
//    public long getTid() {
//        return tid;
//    }
//
//    public AsciiBuffer getBackingQueueName() {
//        return opQueue.getDescriptor().getQueueName();
//    }
//
//    /**
//     * @return The transaction type e.g. {@link Transaction#TYPE_LOCAL}
//     */
//    public abstract byte getType();
//
//    public void addMessage(MessageDelivery m, ISourceController<?> source) {
//
//        synchronized (this) {
//            switch (state) {
//            case START_STATE:
//            case IN_USE_STATE:
//                opQueue.add(new TxMessage(m, this), source);
//                break;
//            default: {
//                throw new IllegalStateException("Can't add message to finished or prepared transaction");
//            }
//            }
//        }
//    }
//
//    public void addAck(SubscriptionDelivery<MessageDelivery> toAck) {
//        synchronized (this) {
//            switch (state) {
//            case START_STATE:
//            case IN_USE_STATE:
//                IQueue<Long, MessageDelivery> target = manager.getVirtualHost().getQueueStore().getQueue(toAck.getQueueDescriptor().getQueueName());
//                //Queue could be null if it was just deleted:
//                if (target != null) {
//                    long tracking = manager.getVirtualHost().getDatabase().allocateStoreTracking();
//                    opQueue.add(new TxAck(target, toAck.getSourceQueueRemovalKey(), tracking, this), null);
//                }
//                break;
//            default: {
//                throw new IllegalStateException("Can't add message to finished or prepared transaction");
//            }
//            }
//        }
//    }
//
//    public byte getState() {
//        return state;
//    }
//
//    public void setState(byte state, FutureListener<? super Object> listener) {
//        this.state = state;
//        ListenableFuture<?> future = manager.persistTransaction(this);
//        future.setFutureListener(listener);
//    }
//
//    public void prePrepare() throws Exception {
//
//        // Is it ok to call prepare now given the state of the
//        // transaction?
//        switch (state) {
//        case START_STATE:
//        case IN_USE_STATE:
//            break;
//        default:
//            XAException xae = new XAException("Prepare cannot be called now.");
//            xae.errorCode = XAException.XAER_PROTO;
//            throw xae;
//        }
//    }
//
//    protected void fireAfterCommit() throws Exception {
//
//        synchronized (this) {
//            for (TransactionListener listener : listeners) {
//                listener.onCommit(this);
//            }
//        }
//    }
//
//    public void fireAfterRollback() throws Exception {
//        synchronized (this) {
//            for (TransactionListener listener : listeners) {
//                listener.onRollback(this);
//            }
//        }
//    }
//
//    public void fireAfterPrepare() throws Exception {
//        synchronized (this) {
//            for (TransactionListener listener : listeners) {
//                listener.onPrepared(this);
//            }
//        }
//    }
//
//    public String toString() {
//        return super.toString() + "[queue=" + opQueue + "]";
//    }
//
//    public abstract void commit(boolean onePhase, TransactionListener listener) throws XAException, IOException;
//
//    public abstract void rollback(TransactionListener listener) throws XAException, IOException;
//
//    public abstract int prepare(TransactionListener listener) throws XAException, IOException;
//
//    public boolean isPrepared() {
//        return getState() == PREPARED_STATE;
//    }
//
//    public long size() {
//        return opQueue.getEnqueuedCount();
//    }
//
//    public static abstract class TransactionListener {
//        public void onRollback(Transaction t) {
//
//        }
//
//        public void onCommit(Transaction t) {
//
//        }
//
//        public void onPrepared(Transaction t) {
//
//        }
//    }
//
//    interface TxOp {
//        public static final short TYPE_MESSAGE = 0;
//        public static final short TYPE_ACK = 1;
//
//        public short getType();
//
//        public <T> T asType(Class<T> type);
//
//        public void onRollback(ISourceController<?> controller);
//
//        public void onCommit(ISourceController<?> controller);
//
//        public int getLimiterSize();
//
//        public boolean isFromStore();
//
//        public long getStoreTracking();
//
//        public MessageRecord createMessageRecord();
//
//        /**
//         * @return
//         */
//        public boolean isPersistent();
//
//        /**
//         * @return
//         */
//        public Long getExpiration();
//
//        public int getPriority();
//    }
//
//    static class TxMessage implements TxOp {
//        MessageDelivery message;
//        Transaction tx;
//        private boolean fromStore;
//
//        /**
//         * @param m
//         * @param transaction
//         */
//        public TxMessage(MessageDelivery m, Transaction tx) {
//            message = m;
//            this.tx = tx;
//        }
//
//        public <T> T asType(Class<T> type) {
//            if (type == TxMessage.class) {
//                return type.cast(this);
//            } else {
//                return null;
//            }
//        }
//
//        public final short getType() {
//            return TYPE_MESSAGE;
//        }
//
//        public final int getLimiterSize() {
//            return message.getFlowLimiterSize();
//        }
//
//        public final void onCommit(ISourceController<?> controller) {
//            message.clearTransactionId();
//            tx.manager.getVirtualHost().getRouter().route(message, controller, true);
//        }
//
//        public final void onRollback(ISourceController<?> controller) {
//            //Nothing to do here, message just gets dropped:
//            return;
//        }
//
//        public final boolean isFromStore() {
//            return fromStore;
//        }
//
//        public final MessageRecord createMessageRecord() {
//            return message.createMessageRecord();
//        }
//
//        public final long getStoreTracking() {
//            return message.getStoreTracking();
//        }
//
//        public final boolean isPersistent() {
//            return message.isPersistent();
//        }
//
//        public final Long getExpiration() {
//            return message.getExpiration();
//        }
//
//        public final int getPriority() {
//            return message.getPriority();
//        }
//    }
//
//    static class TxAck implements TxOp {
//        public static AsciiBuffer ENCODING = new AsciiBuffer("txack");
//        Transaction tx;
//        IQueue<Long, ?> queue; //Desriptor of the queue on which to delete.
//        long queueSequence; //Sequence number of the element on the queue from which to delete.
//        final long storeTracking; //Store tracking of this delete op.
//        private boolean fromStore;
//        private static final int MEM_SIZE = 8 + 8 + 8 + 8 + 1;
//
//        TxAck(IQueue<Long, ?> queue, long removalKey, long storeTracking, Transaction tx) {
//            this.queue = queue;
//            this.queueSequence = removalKey;
//            this.tx = tx;
//            this.storeTracking = storeTracking;
//        }
//
//        public final short getType() {
//            return TYPE_ACK;
//        }
//
//        public <T> T asType(Class<T> type) {
//            if (type == TxAck.class) {
//                return type.cast(this);
//            } else {
//                return null;
//            }
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see org.apache.activemq.apollo.broker.Transaction.TxOp#onCommit()
//         */
//        public final void onCommit(ISourceController<?> controller) {
//            queue.remove(queueSequence);
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see org.apache.activemq.apollo.broker.Transaction.TxOp#onRollback()
//         */
//        public final void onRollback(ISourceController<?> controller) {
//            //No-Op for now, it is possible that we'd want to unaquire these
//            //in the queue if the client weren't to keep these
//            //around
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.apollo.broker.Transaction.TxOp#getLimiterSize()
//         */
//        public final int getLimiterSize() {
//            return MEM_SIZE;
//        }
//
//        public final boolean isFromStore() {
//            return fromStore;
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.apollo.broker.Transaction.TxOp#getStoreTracking()
//         */
//        public final long getStoreTracking() {
//            return storeTracking;
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.apollo.broker.Transaction.TxOp#createMessageRecord
//         * ()
//         */
//        public final MessageRecord createMessageRecord() {
//            MessageRecord ret = new MessageRecord();
//            ret.setProtocol(TxAck.ENCODING);
//            ret.setId(storeTracking);
//            ret.setSize(MEM_SIZE);
//            ret.setValue(new Buffer(toBytes().getData()));
//            return null;
//        }
//
//        private final Buffer toBytes() {
//            AsciiBuffer queueName = queue.getDescriptor().getQueueName();
//            DataByteArrayOutputStream baos = new DataByteArrayOutputStream(2 + queueName.length + 8);
//            try {
//                baos.writeShort(queueName.length);
//                baos.write(queueName.data, queueName.offset, queueName.length);
//                baos.writeLong(queueSequence);
//            } catch (IOException shouldNotHappen) {
//                throw new RuntimeException(shouldNotHappen);
//            }
//            return baos.toBuffer();
//        }
//
//        private final void fromBytes(byte[] bytes) {
//            DataByteArrayInputStream baos = new DataByteArrayInputStream(bytes);
//            byte[] queueBytes = new byte[baos.readShort()];
//            baos.readFully(queueBytes);
//            AsciiBuffer queueName = new AsciiBuffer(queueBytes);
//            queue = tx.manager.getVirtualHost().getQueueStore().getQueue(queueName);
//            queueSequence = baos.readLong();
//
//        }
//
//        public final static TxAck createFromMessageRecord(MessageRecord record, Transaction tx) {
//            TxAck ret = new TxAck(null, -1, record.getId(), tx);
//            ret.fromBytes(record.getValue().getData());
//            return ret;
//        }
//
//        public final boolean isPersistent() {
//            //TODO This could probably be relaxed when the ack is for non persistent
//            //elements
//            return true;
//        }
//
//        public final Long getExpiration() {
//            return -1L;
//        }
//
//        public final int getPriority() {
//            return 0;
//        }
//    }
//
//    /**
//     * @param record
//     * @return
//     */
//    public static TxOp createTxOp(MessageRecord record, Transaction tx) {
//        if (record.getProtocol().equals(TxAck.ENCODING)) {
//            return TxAck.createFromMessageRecord(record, tx);
//        } else {
//            MessageDelivery delivery = tx.manager.getVirtualHost().getQueueStore().getMessageMarshaller().unMarshall(record, tx.opQueue.getDescriptor());
//            return new TxMessage(delivery, tx);
//        }
//    }
//
//    protected void startTransactionProcessor()
//    {
//        synchronized(this)
//        {
//            if(processor == null)
//            {
//                processor = new TxProcessor();
//                opQueue.addSubscription(processor);
//            }
//        }
//    }
//
//
//    /**
//     * TxProcessor
//     * <p>
//     * Description: The tx processor processes the transaction queue after
//     * commit or rollback.
//     * </p>
//     *
//     * @author cmacnaug
//     * @version 1.0
//     */
//    private class TxProcessor implements Subscription<TxOp> {
//        /*
//         * (non-Javadoc)
//         *
//         * @see org.apache.activemq.queue.Subscription#add(java.lang.Object,
//         * org.apache.activemq.flow.ISourceController,
//         * org.apache.activemq.queue.Subscription.SubscriptionDelivery)
//         */
//        public void add(TxOp element, ISourceController<?> controller, SubscriptionDelivery<TxOp> callback) {
//
//            switch (state) {
//            case COMMITED_STATE: {
//                element.onCommit(controller);
//                if (callback != null) {
//                    callback.acknowledge();
//                }
//                break;
//            }
//            case ROLLBACK_STATE: {
//                element.onRollback(controller);
//                if (callback != null) {
//                    callback.acknowledge();
//                }
//                break;
//            }
//            default: {
//                LOG.error("Illegal state for transaction dispatch: " + this + " state: " + state);
//            }
//            }
//
//            //If we've reached the end of the op queue
//            if (opQueue.getEnqueuedCount() == 0) {
//                opQueue.shutdown(null);
//            }
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see org.apache.activemq.queue.Subscription#hasSelector()
//         */
//        public boolean hasSelector() {
//            return false;
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see org.apache.activemq.queue.Subscription#isBrowser()
//         */
//        public boolean isBrowser() {
//            return false;
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see org.apache.activemq.queue.Subscription#isExclusive()
//         */
//        public boolean isExclusive() {
//            return true;
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.queue.Subscription#isRemoveOnDispatch(java.lang
//         * .Object)
//         */
//        public boolean isRemoveOnDispatch(TxOp elem) {
//            return false;
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see org.apache.activemq.queue.Subscription#matches(java.lang.Object)
//         */
//        public boolean matches(TxOp elem) {
//            return true;
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see org.apache.activemq.queue.Subscription#offer(java.lang.Object,
//         * org.apache.activemq.flow.ISourceController,
//         * org.apache.activemq.queue.Subscription.SubscriptionDelivery)
//         */
//        public boolean offer(TxOp element, ISourceController<?> controller, SubscriptionDelivery<TxOp> callback) {
//            add(element, controller, callback);
//            return true;
//        }
//
//    }
}


/**
 * LocalTransaction
 * <p>
 * Description:
 * </p>
 *
 * @author cmacnaug
 * @version 1.0
 */
class LocalTransactionX extends TransactionX {

//    TODO:
//    LocalTransaction(TransactionManager manager, long tid, IQueue<Long, TxOp> opQueue) {
//        super(manager, tid, opQueue);
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see org.apache.activemq.apollo.broker.Transaction#commit(boolean)
//     */
//    @Override
//    public void commit(boolean onePhase, final TransactionListener listener) throws XAException, IOException {
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("commit: " + this);
//        }
//
//        synchronized(this)
//        {
//            // Get ready for commit.
//            try {
//                prePrepare();
//            } catch (XAException e) {
//                throw e;
//            } catch (Throwable e) {
//                LOG.warn("COMMIT FAILED: ", e);
//                rollback(null);
//                // Let them know we rolled back.
//                XAException xae = new XAException("COMMIT FAILED: Transaction rolled back.");
//                xae.errorCode = XAException.XA_RBOTHER;
//                xae.initCause(e);
//                throw xae;
//            }
//
//            //Add the listener for commit
//            if(listeners == null)
//            {
//                listeners = new HashSet<TransactionListener>();
//            }
//            listeners.add(listener);
//
//            //Update the transaction state to committed,
//            //and on complete process the commit:
//            setState(COMMITED_STATE, new FutureListener<Object>()
//            {
//                public void onFutureComplete(Future<? extends Object> dbCommitResult) {
//                    try {
//                        fireAfterCommit();
//                        startTransactionProcessor();
//                    } catch (InterruptedException e) {
//                        //Shouldn't happen
//                        LOG.warn(new AssertionError(e));
//                    } catch (ExecutionException e) {
//                        LOG.warn("COMMIT FAILED: ", e);
//                    }
//                    catch (Exception e)
//                    {
//                    }
//                }
//            });
//        }
//    }
//
//
//    public int prepare(TransactionListener listener) throws XAException {
//        XAException xae = new XAException("Prepare not implemented on Local Transactions.");
//        xae.errorCode = XAException.XAER_RMERR;
//        throw xae;
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see org.apache.activemq.apollo.broker.Transaction#rollback()
//     */
//    @Override
//    public void rollback(TransactionListener listener) throws XAException, IOException {
//        // TODO Auto-generated method stub
//        throw new UnsupportedOperationException("Not yet implemnted");
//    }
//
//    /* (non-Javadoc)
//     * @see org.apache.activemq.apollo.broker.Transaction#getType()
//     */
//    @Override
//    public byte getType() {
//        return TYPE_LOCAL;
//    }

}

/**
 * XATransaction
 * <p>
 * Description:
 * </p>
 *
 * @author cmacnaug
 * @version 1.0
 */
class XATransactionX extends TransactionX {
// TODO:
//    private final Buffer xid;
//
//    XATransaction(TransactionManager manager, long tid, Buffer xid, IQueue<Long, TxOp> opQueue) {
//        super(manager, tid, opQueue);
//        this.xid = xid;
//    }
//
//    public Buffer getXid() {
//        return xid;
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see org.apache.activemq.apollo.broker.Transaction#commit(boolean)
//     */
//    @Override
//    public void commit(boolean onePhase, TransactionListener listener) throws XAException, IOException {
//        // TODO Auto-generated method stub
//
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see org.apache.activemq.apollo.broker.Transaction#prepare()
//     */
//    @Override
//    public int prepare(TransactionListener listener) throws XAException, IOException {
//        // TODO Auto-generated method stub
//        return 0;
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see org.apache.activemq.apollo.broker.Transaction#rollback()
//     */
//    @Override
//    public void rollback(TransactionListener listener) throws XAException, IOException {
//        // TODO Auto-generated method stub
//
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see org.apache.activemq.apollo.broker.Transaction#getType()
//     */
//    @Override
//    public byte getType() {
//        return TYPE_XA;
//    }
}

