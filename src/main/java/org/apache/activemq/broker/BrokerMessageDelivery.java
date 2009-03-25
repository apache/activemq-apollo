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

import java.util.Collection;
import java.util.HashSet;

import org.apache.activemq.broker.store.BrokerDatabase;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.queue.PersistentQueue;

public abstract class BrokerMessageDelivery implements MessageDelivery {

    HashSet<PersistentQueue<MessageDelivery>> persistentTargets;
    // Indicates whether or not the message has already been saved
    // if it hasn't in memory updates can be done.
    boolean saved = false;
    long storeTracking = -1;
    BrokerDatabase store;

    public final boolean isFromStore() {
        return false;
    }

    public final void persist(PersistentQueue<MessageDelivery> queue) {

        synchronized (this) {
            if (!saved) {
                if (persistentTargets == null) {
                    persistentTargets = new HashSet<PersistentQueue<MessageDelivery>>();
                }
                persistentTargets.add(queue);
                return;
            }
        }
        
        //TODO probably need to pass in the saving queue's source controller here
        //and treat it like it is dispatching to the saver queue. 
        store.saveMessage(this, queue, null);
    }

    public final void delete(PersistentQueue<MessageDelivery> queue) {
        synchronized (this) {
            if (!saved) {
                persistentTargets.remove(queue);
                return;
            }
        }

        store.deleteMessage(this, queue);
    }

    public synchronized void beginStore(long storeTracking) {
        saved = true;
        this.storeTracking = storeTracking;
    }

    public long getStoreTracking() {
        return storeTracking;
    }

    public Collection<PersistentQueue<MessageDelivery>> getPersistentQueues() {
        return persistentTargets;
    }

    public void persistIfNeeded(ISourceController<?> controller) {
        boolean saveNeeded = false;
        synchronized (this) {
            if (persistentTargets.isEmpty()) {
                saveNeeded = false;
                saved = true;
            }
        }

        if (saveNeeded) {
            store.persistReceivedMessage(this, controller);
        }
    }
}
