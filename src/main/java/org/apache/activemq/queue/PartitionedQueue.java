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

import org.apache.activemq.flow.AbstractLimitedFlowResource;
import org.apache.activemq.flow.ISourceController;

abstract public class PartitionedQueue<P, K, V> extends AbstractLimitedFlowResource<V> implements IQueue<K, V> {

    private HashSet<Subscription<V>> subscriptions = new HashSet<Subscription<V>>();
    private HashMap<P, IQueue<K, V>> partitions = new HashMap<P, IQueue<K, V>>();
    private Mapper<P, V> partitionMapper;

    public IQueue<K, V> getPartition(P partitionKey) {
        synchronized (partitions) {
            IQueue<K, V> rc = partitions.get(partitionKey);
            if (rc == null) {
                rc = cratePartition(partitionKey);
                partitions.put(partitionKey, rc);
                for (Subscription<V> sub : subscriptions) {
                    rc.addSubscription(sub);
                }
            }
            return rc;
        }
    }

    abstract protected IQueue<K, V> cratePartition(P partitionKey);

    public void addSubscription(Subscription<V> sub) {
        synchronized (partitions) {
            subscriptions.add(sub);
            Collection<IQueue<K, V>> values = partitions.values();
            for (IQueue<K, V> queue : values) {
                queue.addSubscription(sub);
            }
        }
    }

    public boolean removeSubscription(Subscription<V> sub) {
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

    public boolean removeByKey(K key) {
        synchronized (partitions) {
            Collection<IQueue<K, V>> values = partitions.values();
            for (IQueue<K, V> queue : values) {
                if (queue.removeByKey(key)) {
                    return true;
                }
            }
        }
        return false;
    }

    public boolean removeByValue(V value) {
        P partitionKey = partitionMapper.map(value);
        IQueue<K, V> partition = getPartition(partitionKey);
        return partition.removeByValue(value);
    }

    public void setPartitionMapper(Mapper<P, V> partitionMapper) {
        this.partitionMapper = partitionMapper;
    }

    public Mapper<P, V> getPartitionMapper() {
        return partitionMapper;
    }

    public void add(V value, ISourceController<?> source) {
        P partitionKey = partitionMapper.map(value);
        IQueue<K, V> partition = getPartition(partitionKey);
        partition.add(value, source);
    }

    public boolean offer(V value, ISourceController<?> source) {
        P partitionKey = partitionMapper.map(value);
        IQueue<K, V> partition = getPartition(partitionKey);
        return partition.offer(value, source);
    }
}
