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

import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;

public abstract class BrokerMessageDelivery implements MessageDelivery {

// TODO:    
//    // True while the message is being dispatched to the delivery targets:
//    boolean dispatching = false;
//
//    // A non null pending save indicates that the message is the
//    // saver queue and that the message
//    OperationContext<?> pendingSave;
//
//    // List of persistent targets for which the message should be saved
//    // when dispatch is complete:
//    HashMap<QueueDescriptor, SaveableQueueElement<MessageDelivery>> persistentTargets;
//    SaveableQueueElement<MessageDelivery> singleTarget;
//
//    long storeTracking = -1;
//    BrokerDatabase store;
//    boolean fromStore = false;
//    boolean enableFlushDelay = true;
//    private int limiterSize = -1;
//    private long tid=-1;
//
//    public void setFromDatabase(BrokerDatabase database, MessageRecord mRecord) {
//        fromStore = true;
//        store = database;
//        storeTracking = mRecord.getKey();
//        limiterSize = mRecord.getSize();
//    }
//
//    public final int getFlowLimiterSize() {
//        if (limiterSize == -1) {
//            limiterSize = getMemorySize();
//        }
//        return limiterSize;
//    }
//
//    /**
//     * When an application wishes to include a message in a broker transaction
//     * it must set this the tid returned by {@link Transaction#getTid()}
//     *
//     * @param tid
//     *            Sets the tid used to identify the transaction at the broker.
//     */
//    public void setTransactionId(long tid) {
//        this.tid = tid;
//    }
//
//    /**
//     * @return The tid used to identify the transaction at the broker.
//     */
//    public final long getTransactionId() {
//        return tid;
//    }
//
//    public final void clearTransactionId() {
//        tid = -1;
//    }
//
//    /**
//     * Subclass must implement this to return their current memory size
//     * estimate.
//     *
//     * @return The memory size of the message.
//     */
//    public abstract int getMemorySize();
//
//    public final boolean isFromStore() {
//        return fromStore;
//    }
//
//    public final void persist(SaveableQueueElement<MessageDelivery> sqe, ISourceController<?> controller, boolean delayable) {
//        synchronized (this) {
//            // Can flush of this message to the store be delayed?
//            if (enableFlushDelay && !delayable) {
//                enableFlushDelay = false;
//            }
//            // If this message is being dispatched then add the queue to the
//            // list of queues for which to save the message when dispatch is
//            // finished:
//            if (dispatching) {
//                addPersistentTarget(sqe);
//                return;
//            }
//            // Otherwise, if it is still in the saver queue, we can add this
//            // queue to the queue list:
//            else if (pendingSave != null) {
//                addPersistentTarget(sqe);
//                if (!delayable) {
//                    pendingSave.requestFlush();
//                }
//                return;
//            }
//        }
//
//        store.saveMessage(sqe, controller, delayable);
//    }
//
//    public final void acknowledge(SaveableQueueElement<MessageDelivery> sqe) {
//        boolean firePersistListener = false;
//        boolean deleted = false;
//        synchronized (this) {
//            // If the message hasn't been saved to the database
//            // then we don't need to issue a delete:
//            if (dispatching || pendingSave != null) {
//
//                deleted = true;
//
//                removePersistentTarget(sqe.getQueueDescriptor());
//                // We get a save context when we place the message in the
//                // database queue. If it has been added to the queue,
//                // and we've removed the last queue, see if we can cancel
//                // the save:
//                if (pendingSave != null && !hasPersistentTargets()) {
//                    if (pendingSave.cancel()) {
//                        pendingSave = null;
//                        if (isPersistent()) {
//                            firePersistListener = true;
//                        }
//                    }
//                }
//            }
//        }
//
//        if (!deleted) {
//            store.deleteQueueElement(sqe);
//        }
//
//        if (firePersistListener) {
//            onMessagePersisted();
//        }
//
//    }
//
//    public final void setStoreTracking(long tracking) {
//        if (storeTracking == -1) {
//            storeTracking = tracking;
//        }
//    }
//
//    public final void beginDispatch(BrokerDatabase database) {
//        this.store = database;
//        dispatching = true;
//        setStoreTracking(database.allocateStoreTracking());
//    }
//
//    public long getStoreTracking() {
//        return storeTracking;
//    }
//
//    public synchronized Collection<SaveableQueueElement<MessageDelivery>> getPersistentQueues() {
//        if (singleTarget != null) {
//            ArrayList<SaveableQueueElement<MessageDelivery>> list = new ArrayList<SaveableQueueElement<MessageDelivery>>(1);
//            list.add(singleTarget);
//            return list;
//        } else if (persistentTargets != null) {
//            return persistentTargets.values();
//        }
//        return null;
//    }
//
//    public void beginStore() {
//        synchronized (this) {
//            pendingSave = null;
//        }
//    }
//
//    private final boolean hasPersistentTargets() {
//        return (persistentTargets != null && !persistentTargets.isEmpty()) || singleTarget != null;
//    }
//
//    private final void removePersistentTarget(QueueDescriptor queue) {
//        if (persistentTargets != null) {
//            persistentTargets.remove(queue);
//            return;
//        }
//
//        if (singleTarget != null && singleTarget.getQueueDescriptor().equals(queue)) {
//            singleTarget = null;
//        }
//    }
//
//    private final void addPersistentTarget(SaveableQueueElement<MessageDelivery> elem) {
//        if (persistentTargets != null) {
//            persistentTargets.put(elem.getQueueDescriptor(), elem);
//            return;
//        }
//
//        if (singleTarget == null) {
//            singleTarget = elem;
//            return;
//        }
//
//        if (elem.getQueueDescriptor() != singleTarget.getQueueDescriptor()) {
//            persistentTargets = new HashMap<QueueDescriptor, SaveableQueueElement<MessageDelivery>>();
//            persistentTargets.put(elem.getQueueDescriptor(), elem);
//            persistentTargets.put(singleTarget.getQueueDescriptor(), singleTarget);
//            singleTarget = null;
//        }
//    }
//
//    public final void finishDispatch(ISourceController<?> controller) throws IOException {
//        boolean firePersistListener = false;
//        synchronized (this) {
//            // If any of the targets requested save then save the message
//            // Note that this could be the case even if the message isn't
//            // persistent if a target requested that the message be spooled
//            // for some other reason such as queue memory overflow.
//            if (hasPersistentTargets()) {
//                pendingSave = store.persistReceivedMessage(this, controller);
//            }
//
//            // If none of the targets required persistence, then fire the
//            // persist listener:
//            if (pendingSave == null || !isPersistent()) {
//                firePersistListener = true;
//            }
//            dispatching = false;
//        }
//
//        if (firePersistListener) {
//            onMessagePersisted();
//        }
//    }
//
//    public final MessageRecord createMessageRecord() {
//
//        MessageRecord record = new MessageRecord();
//        record.setEncoding(getStoreEncoding());
//        record.setBuffer(getStoreEncoded());
//        record.setStreamKey((long) 0);
//        record.setMessageId(getMsgId());
//        record.setSize(getFlowLimiterSize());
//        record.setKey(getStoreTracking());
//        return record;
//    }
//
//    /**
//     * @return A buffer representation of the message to be stored in the store.
//     * @throws
//     */
//    protected abstract Buffer getStoreEncoded();
//
//    /**
//     * @return The encoding scheme used to store the message.
//     */
//    protected abstract AsciiBuffer getStoreEncoding();
//
//    public boolean isFlushDelayable() {
//        // TODO Auto-generated method stub
//        return enableFlushDelay;
//    }
}
