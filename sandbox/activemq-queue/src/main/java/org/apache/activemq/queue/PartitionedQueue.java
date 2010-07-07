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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.util.Mapper;
import org.fusesource.hawtbuf.AsciiBuffer;

abstract public class PartitionedQueue<K, V> extends AbstractFlowQueue<V> implements IPartitionedQueue<K, V> {

    protected HashSet<Subscription<V>> subscriptions = new HashSet<Subscription<V>>();
    private HashMap<Integer, IQueue<K, V>> partitions = new HashMap<Integer, IQueue<K, V>>();
    protected QueueStore<K, V> store;
    protected Dispatcher dispatcher;
    protected boolean started;
    protected boolean shutdown = false;
    protected QueueDescriptor queueDescriptor;
    protected PersistencePolicy<V> persistencePolicy;
    protected Mapper<Long, V> expirationMapper;
    protected Mapper<K, V> keyMapper;
    protected Mapper<Integer, V> partitionMapper;
    protected int basePriority = 0;

    public PartitionedQueue(String name) {
        super(name);
        queueDescriptor = new QueueDescriptor();
        queueDescriptor.setQueueName(new AsciiBuffer(getResourceName()));
        queueDescriptor.setQueueType(QueueDescriptor.PARTITIONED);
    }

    public QueueDescriptor getDescriptor() {
        return queueDescriptor;
    }

    protected IQueue<K, V> getPartition(int partitionKey) {
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

    abstract public IQueue<K, V> createPartition(int partitionKey);

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
                    for (IQueue<K, V> queue : getPartitions()) {
                        queue.setDispatchPriority(basePriority);
                    }
                }
            }
        }
    }

    public int getEnqueuedCount() {
        checkShutdown();
        synchronized (this) {

            int count = 0;
            for (IQueue<K, V> queue : getPartitions()) {
                count += queue.getEnqueuedCount();
            }
            return count;
        }
    }

    public synchronized long getEnqueuedSize() {
        checkShutdown();
        synchronized (this) {
            long size = 0;
            for (IQueue<K, V> queue : getPartitions()) {
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
        this.persistencePolicy = persistencePolicy;
    }

    public void setExpirationMapper(Mapper<Long, V> expirationMapper) {
        this.expirationMapper = expirationMapper;
    }

    public void initialize(long sequenceMin, long sequenceMax, int count, long size) {
        checkShutdown();
        // No-op, only partitions should have stored values.
        if (count > 0 || size > 0) {
            throw new IllegalArgumentException("Partioned queues do not themselves hold values");
        }
        if (expirationMapper == null) {
            expirationMapper = new Mapper<Long, V>() {

                public Long map(V element) {
                    return -1L;
                }
            };
        }
        if (persistencePolicy == null) {
            persistencePolicy = new PersistencePolicy.NON_PERSISTENT_POLICY<V>();
        }
    }

    public synchronized void start() {
        if (!started) {
            checkShutdown();
            started = true;
            for (IQueue<K, V> partition : getPartitions()) {
                if (partition != null)
                    partition.start();
            }
        }
    }

    public synchronized void stop() {
        if (started) {
            started = false;
            for (IQueue<K, V> partition : getPartitions()) {
                if (partition != null)
                    partition.stop();
            }
        }
    }

    public void shutdown(final Runnable onShutdown) {
        Collection<IQueue<K, V>> partitions = null;
        synchronized (this) {
            if (!shutdown) {
                shutdown = true;
                started = false;
            }
            partitions = getPartitions();
            this.partitions = null;
        }

        if (partitions != null) {

            Runnable wrapper=null;
            if( onShutdown!=null ) {
                final AtomicInteger  countDown = new AtomicInteger(partitions.size());
                wrapper = new Runnable() {
                    public void run() {
                        if( countDown.decrementAndGet()==0 ) {
                            onShutdown.run();
                        }
                    }
                };
            }
            
            for (IQueue<K, V> partition : partitions) {
                if (partition != null)
                    partition.shutdown(wrapper);
            }
        }
    }

    public void addSubscription(Subscription<V> sub) {
        checkShutdown();
        synchronized (this) {
            subscriptions.add(sub);
            Collection<IQueue<K, V>> values = getPartitions();
            for (IQueue<K, V> queue : values) {
                queue.addSubscription(sub);
            }
        }
    }

    public boolean removeSubscription(Subscription<V> sub) {
        checkShutdown();
        synchronized (this) {
            if (subscriptions.remove(sub)) {
                Collection<IQueue<K, V>> values = getPartitions();
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
        getPartition(partitionKey).add(value, source);
    }

    public boolean offer(V value, ISourceController<?> source) {
        int partitionKey = partitionMapper.map(value);
        return getPartition(partitionKey).offer(value, source);
    }

    public void setKeyMapper(Mapper<K, V> keyMapper) {
        this.keyMapper = keyMapper;
    }

    public void setAutoRelease(boolean autoRelease) {
        this.autoRelease = autoRelease;
    }

    public void setDispatcher(Dispatcher dispatcher) {
        checkShutdown();
        this.dispatcher = dispatcher;
        synchronized (this) {
            Collection<IQueue<K, V>> values = getPartitions();
            for (IQueue<K, V> queue : values) {
                queue.setDispatcher(dispatcher);
            }
        }
    }

    protected Collection<IQueue<K, V>> getPartitions() {
        return partitions.values();
    }

    protected void checkShutdown() {
        if (shutdown) {
            throw new IllegalStateException(this + " is shutdown");
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.queue.IPollableFlowSource#isDispatchReady()
     */
    public boolean isDispatchReady() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.queue.IPollableFlowSource#poll()
     */
    public V poll() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.queue.IPollableFlowSource#pollingDispatch()
     */
    public boolean pollingDispatch() {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.flow.ISinkController.FlowControllable#flowElemAccepted
     * (org.apache.activemq.flow.ISourceController, java.lang.Object)
     */
    public void flowElemAccepted(ISourceController<V> source, V elem) {
        // TODO Remove
        throw new UnsupportedOperationException();
    }

    public void remove(long key) {
        throw new UnsupportedOperationException();
    }
}
