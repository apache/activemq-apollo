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

import javax.transaction.xa.XAException;

import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.queue.IQueue;
import org.apache.activemq.queue.Subscription.SubscriptionDelivery;
import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.util.buffer.DataByteArrayInputStream;
import org.apache.activemq.util.buffer.DataByteArrayOutputStream;

/**
 * Keeps track of all the actions the need to be done when a transaction does a
 * commit or rollback.
 * 
 * @version $Revision: 1.5 $
 */
public abstract class Transaction {

    public static final byte START_STATE = 0; // can go to: 1,2,3
    public static final byte IN_USE_STATE = 1; // can go to: 2,3
    public static final byte PREPARED_STATE = 2; // can go to: 3
    public static final byte FINISHED_STATE = 3;

    static final byte TYPE_LOCAL = 0;
    static final byte TYPE_XA = 1;

    private byte state = START_STATE;
    private final TransactionManager manager;
    private final long tid;
    private final IQueue<Long, TxOp> opQueue;

    Transaction(TransactionManager manager, long tid, IQueue<Long, TxOp> opQueue) {
        this.manager = manager;
        this.opQueue = opQueue;
        this.tid = tid;
    }

    /**
     * @return the unique identifier used by the {@link TransactionManager} to
     *         identify this {@link Transaction}
     * 
     */
    public long getTid() {
        return tid;
    }

    public AsciiBuffer getBackingQueueName() {
        return opQueue.getDescriptor().getQueueName();
    }

    /**
     * @return The transaction type e.g. {@link Transaction#TYPE_LOCAL}
     */
    public abstract byte getType();

    public void addMessage(BrokerMessageDelivery m, ISourceController<?> source) {

        synchronized (this) {
            switch (state) {
            case START_STATE:
            case IN_USE_STATE:
                opQueue.add(new TxMessage(m, this), source);
                break;
            default: {
                throw new IllegalStateException("Can't add message to finished or prepared transaction");
            }
            }
        }
    }

    public void addAck(SubscriptionDelivery<MessageDelivery> toAck) {
        synchronized (this) {
            switch (state) {
            case START_STATE:
            case IN_USE_STATE:
                IQueue<Long, MessageDelivery> target = manager.getVirtualHost().getQueueStore().getQueue(toAck.getQueueDescriptor().getQueueName());
                //Queue could be null if it was just deleted:
                if (target != null) {
                    opQueue.add(new TxAck(target, toAck.getSourceQueueRemovalKey(), this), null);
                }
                break;
            default: {
                throw new IllegalStateException("Can't add message to finished or prepared transaction");
            }
            }
        }
    }

    public byte getState() {
        return state;
    }

    public void setState(byte state) {
        this.state = state;
    }

    public void prePrepare() throws Exception {

        // Is it ok to call prepare now given the state of the
        // transaction?
        switch (state) {
        case START_STATE:
        case IN_USE_STATE:
            break;
        default:
            XAException xae = new XAException("Prepare cannot be called now.");
            xae.errorCode = XAException.XAER_PROTO;
            throw xae;
        }

        //TODO:
    }

    protected void fireAfterCommit() throws Exception {

        //TODO
    }

    public void fireAfterRollback() throws Exception {
        //TODO
    }

    public String toString() {
        return super.toString() + "[queue=" + opQueue + "]";
    }

    public abstract void commit(boolean onePhase) throws XAException, IOException;

    public abstract void rollback() throws XAException, IOException;

    public abstract int prepare() throws XAException, IOException;

    public boolean isPrepared() {
        return getState() == PREPARED_STATE;
    }

    public long size() {
        return opQueue.getEnqueuedCount();
    }

    interface TxOp {
        public static final short TYPE_MESSAGE = 0;
        public static final short TYPE_ACK = 1;

        public short getType();

        public <T> T asType(Class<T> type);

        public void onRollback();

        public void onCommit();

        public int getLimiterSize();

        public boolean isFromStore();

        public long getStoreTracking();

        public MessageRecord createMessageRecord();

        /**
         * @return
         */
        public boolean isPersistent();

        /**
         * @return
         */
        public Long getExpiration();

        public int getPriority();
    }

    static class TxMessage implements TxOp {
        MessageDelivery message;
        Transaction tx;
        private boolean fromStore;

        /**
         * @param m
         * @param transaction
         */
        public TxMessage(MessageDelivery m, Transaction tx) {
            message = m;
            this.tx = tx;
        }

        public <T> T asType(Class<T> type) {
            if (type == TxMessage.class) {
                return type.cast(this);
            } else {
                return null;
            }
        }

        public final short getType() {
            return TYPE_MESSAGE;
        }

        public final int getLimiterSize() {
            // TODO Auto-generated method stub
            return message.getFlowLimiterSize();
        }

        public final void onCommit() {

        }

        public final void onRollback() {
            // TODO Auto-generated method stub

        }

        public final boolean isFromStore() {
            return fromStore;
        }

        public final MessageRecord createMessageRecord() {
            return message.createMessageRecord();
        }

        public final long getStoreTracking() {
            return message.getStoreTracking();
        }

        public final boolean isPersistent() {
            return message.isPersistent();
        }

        public final Long getExpiration() {
            return message.getExpiration();
        }

        public final int getPriority() {
            return message.getPriority();
        }
    }

    static class TxAck implements TxOp {
        public static AsciiBuffer ENCODING = new AsciiBuffer("txack");
        Transaction tx;
        IQueue<Long, ?> queue; //Desriptor of the queue on which to delete.
        long queueSequence; //Sequence number of the element on the queue from which to delete.
        long storeTracking; //Store tracking of this delete op.
        private boolean fromStore;
        private static final int MEM_SIZE = 8 + 8 + 8 + 8 + 1;

        TxAck(IQueue<Long, ?> queue, long storeTracking, Transaction tx) {
            this.queue = queue;
            this.storeTracking = storeTracking;
            this.tx = tx;
        }

        public final short getType() {
            return TYPE_ACK;
        }

        public <T> T asType(Class<T> type) {
            if (type == TxAck.class) {
                return type.cast(this);
            } else {
                return null;
            }
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.apollo.broker.Transaction.TxOp#onCommit()
         */
        public final void onCommit() {
            //TODO

        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.apollo.broker.Transaction.TxOp#getLimiterSize()
         */
        public final int getLimiterSize() {
            return MEM_SIZE;
        }

        /*
         * (non-Javadoc)
         * 
         * @see org.apache.activemq.apollo.broker.Transaction.TxOp#onRollback()
         */
        public final void onRollback() {
            // TODO unaquire the element.
        }

        public final boolean isFromStore() {
            return fromStore;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.apollo.broker.Transaction.TxOp#getStoreTracking()
         */
        public final long getStoreTracking() {
            return storeTracking;
        }

        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.activemq.apollo.broker.Transaction.TxOp#createMessageRecord
         * ()
         */
        public final MessageRecord createMessageRecord() {
            MessageRecord ret = new MessageRecord();
            ret.setEncoding(TxAck.ENCODING);
            ret.setKey(storeTracking);
            ret.setSize(MEM_SIZE);
            ret.setBuffer(new Buffer(toBytes().getData()));
            return null;
        }

        private final Buffer toBytes() {
            AsciiBuffer queueName = queue.getDescriptor().getQueueName();
            DataByteArrayOutputStream baos = new DataByteArrayOutputStream(2 + queueName.length + 8);
            try {
				baos.writeShort(queueName.length);
				baos.write(queueName.data, queueName.offset, queueName.length);
				baos.writeLong(queueSequence);
			} catch (IOException shouldNotHappen) {
				throw new RuntimeException(shouldNotHappen);
			}
            return baos.toByteSequence();
        }

        private final void fromBytes(byte[] bytes) {
            DataByteArrayInputStream baos = new DataByteArrayInputStream(bytes);
            byte[] queueBytes = new byte[baos.readShort()];
            baos.readFully(queueBytes);
            AsciiBuffer queueName = new AsciiBuffer(queueBytes);
            queue = tx.manager.getVirtualHost().getQueueStore().getQueue(queueName);

        }

        public final static TxAck createFromMessageRecord(MessageRecord record, Transaction tx) {
            TxAck ret = new TxAck(null, record.getKey(), tx);
            ret.fromBytes(record.getBuffer().getData());
            return ret;
        }

        public final boolean isPersistent() {
            //TODO This could probably be relaxed when the ack is for non persistent
            //elements
            return true;
        }

        public final Long getExpiration() {
            return -1L;
        }

        public final int getPriority() {
            return 0;
        }
    }

    /**
     * @param record
     * @return
     */
    public static TxOp createTxOp(MessageRecord record, Transaction tx) {
        if (record.getEncoding().equals(TxAck.ENCODING)) {
            return TxAck.createFromMessageRecord(record, tx);
        } else {
            MessageDelivery delivery = tx.manager.getVirtualHost().getQueueStore().getMessageMarshaller().unMarshall(record, tx.opQueue.getDescriptor());
            return new TxMessage(delivery, tx);
        }
    }
}
