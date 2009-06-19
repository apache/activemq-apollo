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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.dispatch.IDispatcher.DispatchContext;
import org.apache.activemq.flow.AbstractLimitedFlowResource;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.util.Mapper;

abstract public class PartitionedQueue<K, V> extends AbstractFlowQueue<V> implements IPartitionedQueue<K, V> {

    private HashSet<Subscription<V>> subscriptions = new HashSet<Subscription<V>>();
    private HashMap<Integer, IQueue<K, V>> partitions = new HashMap<Integer, IQueue<K, V>>();
    protected Mapper<Integer, V> partitionMapper;
    private QueueStore<K, V> store;
    protected IDispatcher dispatcher;
    private boolean started;
    private boolean shutdown = false;
    protected QueueDescriptor queueDescriptor;
    private int basePriority = 0;

    public PartitionedQueue(String name) {
        super(name);
        queueDescriptor = new QueueDescriptor();
        queueDescriptor.setQueueName(new AsciiBuffer(getResourceName()));
        queueDescriptor.setQueueType(QueueDescriptor.PARTITIONED);
    }

    public QueueDescriptor getDescriptor() {
        return queueDescriptor;
    }

    public IQueue<K, V> getPartition(int partitionKey) {
        boolean save = false;
        IQueue<K, V> rc = null;
        checkShutdown();
        synchronized (partitions) {
            rc = partitions.get(partitionKey);
            if (rc == null) {
                rc = createPartition(partitionKey);
                partitions.put(partitionKey, rc);
                for (Subscription<V> sub : subscriptions) {
                    rc.addSubscription(sub);
                }
            }
        }
        if (save) {
            store.addQueue(rc.getDescriptor());
        }

        return rc;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.queue.IQueue#setDispatchPriority(int)
     */
    public void setDispatchPriority(int priority) {
        synchronized (this) {
            if (basePriority != priority) {
                basePriority = priority;
                if (!shutdown) {
                    for (IQueue<K, V> queue : partitions.values()) {
                        queue.setDispatchPriority(basePriority);
                    }
                }
            }
        }
    }

    public int getEnqueuedCount() {
        checkShutdown();
        synchronized (partitions) {

            int count = 0;
            for (IQueue<K, V> queue : partitions.values()) {
                count += queue.getEnqueuedCount();
            }
            return count;
        }
    }

    public synchronized long getEnqueuedSize() {
        checkShutdown();
        synchronized (partitions) {
            long size = 0;
            for (IQueue<K, V> queue : partitions.values()) {
                if (queue != null) {
                    size += queue.getEnqueuedSize();
                }
            }
            return size;
        }
    }

    public void setStore(QueueStore<K, V> store) {
        this.store = store;
    }

    public void setPersistencePolicy(PersistencePolicy<V> persistencePolicy) {
        // No-Op for now.
    }

    public void setExpirationMapper(Mapper<Long, V> expirationMapper) {
        // No-Op for now.
    }

    abstract public IQueue<K, V> createPartition(int partitionKey);

    public void addPartition(int partitionKey, IQueue<K, V> queue) {
        checkShutdown();
        synchronized (partitions) {
            partitions.put(partitionKey, queue);
            for (Subscription<V> sub : subscriptions) {
                queue.addSubscription(sub);
                queue.setDispatchPriority(basePriority);
            }
        }
    }

    public void initialize(long sequenceMin, long sequenceMax, int count, long size) {
        // No-op, only partitions should have stored values.
        if (count > 0 || size > 0) {
            throw new IllegalArgumentException("Partioned queues do not themselves hold values");
        }
    }

    public synchronized void start() {
        if (!started) {
            checkShutdown();
            started = true;
            for (IQueue<K, V> partition : partitions.values()) {
                if (partition != null)
                    partition.start();
            }
        }
    }

    public synchronized void stop() {
        if (started) {
            started = false;
            for (IQueue<K, V> partition : partitions.values()) {
                if (partition != null)
                    partition.stop();
            }
        }
    }

    public void shutdown(boolean sync) {
        HashMap<Integer, IQueue<K, V>> partitions = null;
        synchronized (this) {
            if (!shutdown) {
                shutdown = true;
                started = false;
            }
            partitions = this.partitions;
            this.partitions = null;
        }

        if (partitions != null) {
            for (IQueue<K, V> partition : partitions.values()) {
                if (partition != null)
                    partition.shutdown(sync);
            }
        }
    }

    public void addSubscription(Subscription<V> sub) {
        checkShutdown();
        synchronized (partitions) {
            subscriptions.add(sub);
            Collection<IQueue<K, V>> values = partitions.values();
            for (IQueue<K, V> queue : values) {
                queue.addSubscription(sub);
            }
        }
    }

    public boolean removeSubscription(Subscription<V> sub) {
        checkShutdown();
        synchronized (partitions) {
            if (subscriptions.remove(sub)) {
                Collection<IQueue<K, V>> values = partitions.values();
                for (IQueue<K, V> queue : values) {
                    queue.removeSubscription(sub);
                }
                return true;
            }
        }
        return false;
    }

    public void setPartitionMapper(Mapper<Integer, V> partitionMapper) {
        this.partitionMapper = partitionMapper;
    }

    public Mapper<Integer, V> getPartitionMapper() {
        return partitionMapper;
    }

    public void add(V value, ISourceController<?> source) {
        int partitionKey = partitionMapper.map(value);
        IQueue<K, V> partition = getPartition(partitionKey);
        partition.add(value, source);
    }

    public boolean offer(V value, ISourceController<?> source) {
        int partitionKey = partitionMapper.map(value);
        IQueue<K, V> partition = getPartition(partitionKey);
        return partition.offer(value, source);
    }

    public void setDispatcher(IDispatcher dispatcher) {
        checkShutdown();
        this.dispatcher = dispatcher;
        synchronized (partitions) {
            Collection<IQueue<K, V>> values = partitions.values();
            for (IQueue<K, V> queue : values) {
                queue.setDispatcher(dispatcher);
            }
        }
    }

    private void checkShutdown() {
        if (shutdown) {
            throw new IllegalStateException(this + " is shutdown");
        }
    }
    
    /* (non-Javadoc)
     * @see org.apache.activemq.queue.IPollableFlowSource#isDispatchReady()
     */
    public boolean isDispatchReady() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.queue.IPollableFlowSource#poll()
     */
    public V poll() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.queue.IPollableFlowSource#pollingDispatch()
     */
    public boolean pollingDispatch() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.flow.ISinkController.FlowControllable#flowElemAccepted(org.apache.activemq.flow.ISourceController, java.lang.Object)
     */
    public void flowElemAccepted(ISourceController<V> source, V elem) {
        // TODO Remove
        throw new UnsupportedOperationException();
        
    }
}
