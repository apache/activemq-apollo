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
import java.util.HashSet;

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.flow.AbstractLimitedFlowResource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.PrioritySizeLimiter;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.queue.QueueStore.QueueDescriptor;
import org.apache.activemq.util.Mapper;

public class SharedPriorityQueue<K, V> extends AbstractLimitedFlowResource<V> implements IPartitionedQueue<K, V> {

    private final HashSet<Subscription<V>> subscriptions = new HashSet<Subscription<V>>();
    private final Mapper<Integer, V> priorityMapper;
    private final ArrayList<SharedQueue<K, V>> partitions = new ArrayList<SharedQueue<K, V>>();
    private Mapper<K, V> keyMapper;
    private boolean autoRelease;
    private IDispatcher dispatcher;
    private final PrioritySizeLimiter<V> limiter;
    private QueueStore<K, V> store;
    private boolean started;
    private QueueStore.QueueDescriptor queueDescriptor;

    public SharedPriorityQueue(String name, PrioritySizeLimiter<V> limiter) {
        super(name);
        queueDescriptor = new QueueStore.QueueDescriptor();
        queueDescriptor.setQueueName(new AsciiBuffer(super.getResourceName()));
        queueDescriptor.setQueueType(QueueDescriptor.SHARED_PRIORITY);
        this.limiter = limiter;
        priorityMapper = limiter.getPriorityMapper();
        for (int i = 0; i < limiter.getPriorities(); i++) {
            partitions.add(null);
        }
    }

    public synchronized void start() {
        if (!started) {
            started = true;
            for (SharedQueue<K, V> partition : partitions) {
                if (partition != null)
                    partition.start();
            }
        }
    }

    public synchronized void stop() {
        if (started) {
            started = false;
            for (SharedQueue<K, V> partition : partitions) {
                if (partition != null)
                    partition.stop();
            }
        }
    }

    public void initialize(long sequenceMin, long sequenceMax, int count, long size) {
        // No-op, only partitions should have stored values.
        if (count > 0 || size > 0) {
            throw new IllegalArgumentException("Partioned queues do not themselves hold values");
        }
    }

    public synchronized int getEnqueuedCount() {
        int count = 0;
        for (SharedQueue<K, V> queue : partitions) {
            if (queue != null) {
                count += queue.getEnqueuedCount();
            }
        }
        return count;
    }

    public synchronized long getEnqueuedSize() {
        return limiter.getSize();
    }

    public void setStore(QueueStore<K, V> store) {
        this.store = store;
    }

    public void setResourceName(String resourceName) {
        super.setResourceName(resourceName);
    }

    public void addSubscription(Subscription<V> sub) {
        synchronized (this) {
            subscriptions.add(sub);
            for (SharedQueue<K, V> queue : partitions) {
                if (queue != null) {
                    queue.addSubscription(sub);
                }
            }
        }
    }

    public boolean removeSubscription(Subscription<V> sub) {
        synchronized (this) {
            if (subscriptions.remove(sub)) {
                for (SharedQueue<K, V> queue : partitions) {
                    if (queue != null) {
                        queue.removeSubscription(sub);
                    }
                }
                return true;
            }
        }
        return false;
    }

    public IQueue<K, V> createPartition(int prio) {
        return getPartition(prio, false);
    }

    private IQueue<K, V> getPartition(int prio, boolean initialize) {
        synchronized (this) {
            SharedQueue<K, V> queue = partitions.get(prio);
            if (queue == null) {
                queue = new SharedQueue<K, V>(getResourceName() + "$" + prio, limiter.getPriorityLimter(prio), this);
                queue.setAutoRelease(autoRelease);
                queue.setDispatcher(dispatcher);
                queue.setDispatchPriority(prio);
                queue.setKeyMapper(keyMapper);
                queue.setStore(store);
                queue.getDescriptor().setParent(queueDescriptor.getQueueName());
                queue.getDescriptor().setPartitionId(prio);
                partitions.set(prio, queue);
                onFlowOpened(queue.getFlowControler());
                if (initialize) {
                    store.addQueue(queue.getDescriptor());
                    queue.initialize(0, 0, 0, 0);
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

    public QueueStore.QueueDescriptor getDescriptor() {
        return queueDescriptor;
    }

    public void add(V value, ISourceController<?> source) {
        int prio = priorityMapper.map(value);
        IQueue<K, V> partition = getPartition(prio, true);
        partition.add(value, source);
    }

    public boolean offer(V value, ISourceController<?> source) {
        int prio = priorityMapper.map(value);
        IQueue<K, V> partition = getPartition(prio, true);
        return partition.offer(value, source);
    }

    public void setKeyMapper(Mapper<K, V> keyMapper) {
        this.keyMapper = keyMapper;
    }

    public void setAutoRelease(boolean autoRelease) {
        this.autoRelease = autoRelease;
    }

    public void setDispatcher(IDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        super.setFlowExecutor(dispatcher.createPriorityExecutor(dispatcher.getDispatchPriorities() - 1));
    }
}
