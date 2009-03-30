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
import java.util.Collection;
import java.util.HashSet;

import org.apache.activemq.broker.store.BrokerDatabase;
import org.apache.activemq.broker.store.BrokerDatabase.OperationContext;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.protobuf.AsciiBuffer;

public abstract class BrokerMessageDelivery implements MessageDelivery {

    HashSet<AsciiBuffer> persistentTargets;
    // Indicates whether or not the message has been saved to the
    // database, if not then in memory updates can be done.
    boolean saved = false;
    long storeTracking = -1;
    BrokerDatabase store;
    boolean fromStore = false;
    boolean enableFlushDelay = true;
    OperationContext saveContext;
    boolean cancelled = false;

    public void setFromStore(boolean val) {
        fromStore = true;
    }

    public final boolean isFromStore() {
        return fromStore;
    }

    public final void persist(AsciiBuffer queue, boolean delayable) throws IOException {

        synchronized (this) {
            if (!saved) {
                if (persistentTargets == null) {
                    persistentTargets = new HashSet<AsciiBuffer>();
                }
                persistentTargets.add(queue);
                return;
            }
            if (!delayable) {
                enableFlushDelay = false;
            }
        }

        // TODO probably need to pass in the saving queue's source controller
        // here and treat it like it is dispatching to the saver queue.
        store.saveMessage(this, queue, null);
    }

    public final void delete(AsciiBuffer queue) {
        boolean firePersistListener = false;
        synchronized (this) {
            if (!saved) {
                persistentTargets.remove(queue);
                if (persistentTargets.isEmpty()) {
                    if (saveContext != null) {

                        if (!cancelled) {
                            if (saveContext.cancel()) {
                                cancelled = true;
                                firePersistListener = true;
                            }

                            saved = true;
                        }
                    }
                }
            } else {
                store.deleteMessage(this, queue);
            }
        }

        if (firePersistListener) {
            onMessagePersisted();
        }

    }

    public void setStoreTracking(long storeTracking) {
        this.storeTracking = storeTracking;
    }

    public long getStoreTracking() {
        return storeTracking;
    }

    public Collection<AsciiBuffer> getPersistentQueues() {
        return persistentTargets;
    }

    public void beginStore() {
        synchronized (this) {
            saved = true;
        }
    }

    public void persistIfNeeded(ISourceController<?> controller) throws IOException {
        boolean firePersistListener = false;
        synchronized (this) {
            boolean saveNeeded = true;
            if (persistentTargets == null || persistentTargets.isEmpty()) {
                saveNeeded = false;
                saved = true;
            }

            // If any of the targets requested save then save the message
            // Note that this could be the case even if the message isn't
            // persistent if a target requested that the message be spooled
            // for some other reason such as queue memory overflow.
            if (saveNeeded) {
                saveContext = store.persistReceivedMessage(this, controller);
            }
            // If none of the targets required persistence, then fire the
            // persist listener:
            else if (isResponseRequired() && isPersistent()) {
                firePersistListener = true;
            }
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
