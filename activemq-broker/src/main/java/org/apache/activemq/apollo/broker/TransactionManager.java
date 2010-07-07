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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.broker.store.Store.QueueQueryResult;
import org.apache.activemq.util.ListenableFuture;
import org.apache.activemq.util.Mapper;
import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.util.buffer.DataByteArrayInputStream;
import org.apache.activemq.util.buffer.DataByteArrayOutputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * TransactionManager
 * <p>
 * Description:
 * </p>
 * 
 * @version 1.0
 */
public class TransactionManager {
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

    public TransactionManager(VirtualHost virtualHost) {
    }

    public void loadTransactions() {
    }
}
