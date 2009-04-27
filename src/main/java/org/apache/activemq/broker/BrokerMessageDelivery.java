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
package org.apache.activemq.broker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.activemq.broker.store.BrokerDatabase;
import org.apache.activemq.broker.store.BrokerDatabase.OperationContext;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.queue.QueueStore;
import org.apache.activemq.queue.QueueStore.QueueDescriptor;

public abstract class BrokerMessageDelivery implements MessageDelivery {

    // True while the message is being dispatched to the delivery targets:
    boolean dispatching = false;

    // A non null pending save indicates that the message is the
    // saver queue and that the message
    OperationContext pendingSave;

    // List of persistent targets for which the message should be saved
    // when dispatch is complete:
    HashMap<QueueStore.QueueDescriptor, Long> persistentTargets;

    long storeTracking = -1;
    BrokerDatabase store;
    boolean fromStore = false;
    boolean enableFlushDelay = true;
    private int limiterSize = -1;

    public void setFromDatabase(BrokerDatabase database, MessageRecord mRecord) {
        fromStore = true;
        store = database;
        storeTracking = mRecord.getKey();
        limiterSize = mRecord.getSize();
    }

    public final int getFlowLimiterSize() {
        if (limiterSize == -1) {
            limiterSize = getMemorySize();
        }
        return limiterSize;
    }

    /**
     * Subclass must implement this to return their current memory size
     * estimate.
     * 
     * @return The memory size of the message.
     */
    public abstract int getMemorySize();

    public final boolean isFromStore() {
        return fromStore;
    }

    public final void persist(QueueStore.QueueDescriptor queue, ISourceController<?> controller, long queueSequence, boolean delayable) throws IOException {
        synchronized (this) {
            // Can flush of this message to the store be delayed?
            if (enableFlushDelay && !delayable) {
                enableFlushDelay = false;
            }
            // If this message is being dispatched then add the queue to the
            // list of queues for which to save the message when dispatch is
            // finished:
            if (dispatching) {
                if (persistentTargets == null) {
                    persistentTargets = new HashMap<QueueStore.QueueDescriptor, Long>();
                }
                persistentTargets.put(queue, queueSequence);
                return;
            }
            // Otherwise, if it is still in the saver queue, we can add this
            // queue to the queue list:
            else if (pendingSave != null) {
                persistentTargets.put(queue, queueSequence);
                if (!delayable) {
                    pendingSave.requestFlush();
                }
                return;
            }
        }

        store.saveMessage(this, queue, queueSequence, controller);
    }

    public final void acknowledge(QueueStore.QueueDescriptor queue) {
        boolean firePersistListener = false;
        boolean deleted = false;
        synchronized (this) {
            // If the message hasn't been saved to the database
            // then we don't need to issue a delete:
            if (dispatching || pendingSave != null) {

                // Remove the queue:
                persistentTargets.remove(queue);
                deleted = true;

                // We get a save context when we place the message in the
                // database queue. If it has been added to the queue,
                // and we've removed the last queue, see if we can cancel
                // the save:
                if (pendingSave != null && persistentTargets.isEmpty()) {
                    if (pendingSave.cancel()) {
                        pendingSave = null;
                        if (isPersistent()) {
                            firePersistListener = true;
                        }
                    }
                }
            }
        }

        if (!deleted) {
            store.deleteMessage(this, queue);
        }

        if (firePersistListener) {
            onMessagePersisted();
        }

    }

    public void beginDispatch(BrokerDatabase database) {
        this.store = database;
        dispatching = true;
        if (storeTracking == -1) {
            storeTracking = database.allocateStoreTracking();
        }
    }

    public long getStoreTracking() {
        return storeTracking;
    }

    public Set<Entry<QueueDescriptor, Long>> getPersistentQueues() {
        return persistentTargets.entrySet();
    }

    public void beginStore() {
        synchronized (this) {
            pendingSave = null;
        }
    }

    public void finishDispatch(ISourceController<?> controller) throws IOException {
        boolean firePersistListener = false;
        synchronized (this) {
            // If any of the targets requested save then save the message
            // Note that this could be the case even if the message isn't
            // persistent if a target requested that the message be spooled
            // for some other reason such as queue memory overflow.
            if (persistentTargets != null && !persistentTargets.isEmpty()) {
                pendingSave = store.persistReceivedMessage(this, controller);
            }

            // If none of the targets required persistence, then fire the
            // persist listener:
            if (pendingSave == null || !isPersistent()) {
                firePersistListener = true;
            }
            dispatching = false;
        }

        if (firePersistListener) {
            onMessagePersisted();
        }
    }

    public boolean isFlushDelayable() {
        // TODO Auto-generated method stub
        return enableFlushDelay;
    }
}
