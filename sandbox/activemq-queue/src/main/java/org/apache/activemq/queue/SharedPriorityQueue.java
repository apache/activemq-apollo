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
package org.apache.activemq.queue;

import java.util.ArrayList;

import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.flow.PrioritySizeLimiter;
import org.fusesource.hawtbuf.AsciiBuffer;

public class SharedPriorityQueue<K, V> extends PartitionedQueue<K, V> {

    private final ArrayList<SharedQueue<K, V>> partitions = new ArrayList<SharedQueue<K, V>>();
    private final PrioritySizeLimiter<V> limiter;

    public SharedPriorityQueue(String name, PrioritySizeLimiter<V> limiter) {
        super(name);
        queueDescriptor = new QueueDescriptor();
        queueDescriptor.setQueueName(new AsciiBuffer(super.getResourceName()));
        queueDescriptor.setQueueType(QueueDescriptor.SHARED_PRIORITY);
        this.limiter = limiter;
        super.setPartitionMapper(limiter.getPriorityMapper());
        for (int i = 0; i < limiter.getPriorities(); i++) {
            partitions.add(null);
        }
    }

    @Override
    public void shutdown(Runnable onShutdown) {
        try {
            super.shutdown(onShutdown);
        } finally {
            partitions.clear();
        }
    }

    /**
     * Override with more efficient limiter lookup:
     */
    @Override
    public synchronized long getEnqueuedSize() {
        return limiter.getSize();
    }

    public IQueue<K, V> createPartition(int prio) {
        synchronized (this) {
            return getPartition(prio, started);
        }
    }

    protected IQueue<K, V> getPartition(int prio, boolean initialize) {
        synchronized (this) {
            checkShutdown();
            SharedQueue<K, V> queue = partitions.get(prio);
            if (queue == null) {
                queue = new SharedQueue<K, V>(getResourceName() + "$" + prio, limiter.getPriorityLimter(prio), this);
                queue.setAutoRelease(autoRelease);
                queue.setDispatcher(dispatcher);
                queue.setDispatchPriority(basePriority + prio);
                queue.setKeyMapper(keyMapper);
                queue.setStore(store);
                queue.setPersistencePolicy(persistencePolicy);
                queue.setExpirationMapper(expirationMapper);
                queue.getDescriptor().setParent(queueDescriptor.getQueueName());
                queue.getDescriptor().setPartitionId(prio);
                partitions.set(prio, queue);
                if (initialize) {
                    store.addQueue(queue.getDescriptor());
                    queue.initialize(0, 0, 0, 0);
                    onFlowOpened(queue.getFlowControler());
                }

                if (started) {
                    queue.start();
                }

                for (Subscription<V> sub : subscriptions) {
                    queue.addSubscription(sub);
                }

            }
            return queue;
        }
    }
}
