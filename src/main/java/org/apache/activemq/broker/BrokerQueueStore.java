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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.activemq.broker.store.BrokerDatabase;
import org.apache.activemq.broker.store.Store.QueueQueryResult;
import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.PrioritySizeLimiter;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.queue.IPartitionedQueue;
import org.apache.activemq.queue.IQueue;
import org.apache.activemq.queue.PartitionedQueue;
import org.apache.activemq.queue.QueueStore;
import org.apache.activemq.queue.SharedPriorityQueue;
import org.apache.activemq.queue.SharedQueue;
import org.apache.activemq.queue.SharedQueueOld;
import org.apache.activemq.util.Mapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BrokerQueueStore implements QueueStore<Long, MessageDelivery> {

    private static final Log LOG = LogFactory.getLog(BrokerQueueStore.class);
    private static final boolean USE_OLD_QUEUE = false;
    private static final boolean USE_PRIORITY_QUEUES = true;

    private BrokerDatabase database;
    private IDispatcher dispatcher;

    private final short PARTITION_TYPE = 0;
    private final short SHARED_QUEUE_TYPE = 1;
    //private final short SUBSCRIBER_QUEUE_TYPE = 2;

    private final HashMap<String, IQueue<Long, MessageDelivery>> sharedQueues = new HashMap<String, IQueue<Long, MessageDelivery>>();
    //private final HashMap<String, IFlowQueue<MessageDelivery>> subscriberQueues = new HashMap<String, IFlowQueue<MessageDelivery>>();

    private Mapper<Integer, MessageDelivery> partitionMapper;

    public static final Mapper<Integer, MessageDelivery> PRIORITY_MAPPER = new Mapper<Integer, MessageDelivery>() {
        public Integer map(MessageDelivery element) {
            return element.getPriority();
        }
    };

    static public final Mapper<Long, MessageDelivery> KEY_MAPPER = new Mapper<Long, MessageDelivery>() {
        public Long map(MessageDelivery element) {
            return element.getStoreTracking();
        }
    };

    static public final Mapper<Integer, MessageDelivery> PARTITION_MAPPER = new Mapper<Integer, MessageDelivery>() {
        public Integer map(MessageDelivery element) {
            // we modulo 10 to have at most 10 partitions which the producers
            // gets split across.
            return (int) (element.getProducerId().hashCode() % 10);
        }
    };

    public void setDatabase(BrokerDatabase database) {
        this.database = database;
    }

    public void setDispatcher(IDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public void loadQueues() throws Exception {

        // Load shared queues
        Iterator<QueueQueryResult> results = database.listQueues(SHARED_QUEUE_TYPE);
        while (results.hasNext()) {
            QueueQueryResult loaded = results.next();
            IQueue<Long, MessageDelivery> queue = createRestoredQueue(null, loaded);
            sharedQueues.put(queue.getDescriptor().getQueueName().toString(), queue);
            LOG.info("Loaded Queue " + queue.getResourceName() + " Messages: " + queue.getEnqueuedCount() + " Size: " + queue.getEnqueuedSize());
        }
    }

    private IQueue<Long, MessageDelivery> createRestoredQueue(IPartitionedQueue<Long, MessageDelivery> parent, QueueQueryResult loaded) throws IOException {

        IQueue<Long, MessageDelivery> queue;
        if (parent != null) {
            queue = parent.createPartition(loaded.getDescriptor().getPartitionKey());
        } else {
            queue = createSharedQueueInternal(loaded.getDescriptor().getQueueName().toString(), loaded.getDescriptor().getQueueType());
        }

        queue.initialize(loaded.getFirstSequence(), loaded.getLastSequence(), loaded.getCount(), loaded.getSize());

        // Creat the child queues
        Collection<QueueQueryResult> children = loaded.getPartitions();
        if (children != null) {
            try {
                IPartitionedQueue<Long, MessageDelivery> pQueue = (IPartitionedQueue<Long, MessageDelivery>) queue;
                for (QueueQueryResult child : children) {
                    createRestoredQueue(pQueue, child);
                }
            } catch (ClassCastException cce) {
                LOG.error("Loaded partition for unpartitionable queue: " + queue.getResourceName());
                throw cce;
            }
        }

        return queue;

    }

    public Collection<IQueue<Long, MessageDelivery>> getSharedQueues() {
        Collection<IQueue<Long, MessageDelivery>> c = sharedQueues.values();
        ArrayList<IQueue<Long, MessageDelivery>> ret = new ArrayList<IQueue<Long, MessageDelivery>>(c.size());
        ret.addAll(c);
        return ret;
    }

    public IQueue<Long, MessageDelivery> createSharedQueue(String name) {

        IQueue<Long, MessageDelivery> queue = null;
        synchronized (this) {
            queue = sharedQueues.get(name);
            if (queue == null) {
                queue = createSharedQueueInternal(name, USE_PRIORITY_QUEUES ? QueueDescriptor.SHARED_PRIORITY : QueueDescriptor.SHARED);
                queue.getDescriptor().setApplicationType(SHARED_QUEUE_TYPE);
                queue.initialize(0, 0, 0, 0);
                sharedQueues.put(name, queue);
                addQueue(queue.getDescriptor());
            }
        }

        return queue;
    }

    private IQueue<Long, MessageDelivery> createSharedQueueInternal(final String name, short type) {

        IQueue<Long, MessageDelivery> ret;

        switch (type) {
        case QueueDescriptor.PARTITIONED: {
            PartitionedQueue<Long, MessageDelivery> queue = new PartitionedQueue<Long, MessageDelivery>(name) {
                @Override
                public IQueue<Long, MessageDelivery> createPartition(int partitionKey) {
                    IQueue<Long, MessageDelivery> queue = createSharedQueueInternal(name + "$" + partitionKey, USE_PRIORITY_QUEUES ? QueueDescriptor.SHARED_PRIORITY : QueueDescriptor.SHARED);
                    queue.getDescriptor().setPartitionId(partitionKey);
                    queue.getDescriptor().setParent(this.getDescriptor().getQueueName());
                    return queue;
                }

            };
            queue.setPartitionMapper(partitionMapper);

            ret = queue;
            break;
        }
        case QueueDescriptor.SHARED_PRIORITY: {
            PrioritySizeLimiter<MessageDelivery> limiter = new PrioritySizeLimiter<MessageDelivery>(100, 1, MessageBroker.MAX_PRIORITY);
            limiter.setPriorityMapper(PRIORITY_MAPPER);
            SharedPriorityQueue<Long, MessageDelivery> queue = new SharedPriorityQueue<Long, MessageDelivery>(name, limiter);
            ret = queue;
            queue.setKeyMapper(KEY_MAPPER);
            queue.setAutoRelease(true);
            break;
        }
        case QueueDescriptor.SHARED: {
            SizeLimiter<MessageDelivery> limiter = new SizeLimiter<MessageDelivery>(100, 1);
            if (!USE_OLD_QUEUE) {
                SharedQueue<Long, MessageDelivery> sQueue = new SharedQueue<Long, MessageDelivery>(name, limiter);
                sQueue.setKeyMapper(KEY_MAPPER);
                sQueue.setAutoRelease(true);
                ret = sQueue;
            } else {
                SharedQueueOld<Long, MessageDelivery> sQueue = new SharedQueueOld<Long, MessageDelivery>(name, limiter);
                sQueue.setKeyMapper(KEY_MAPPER);
                sQueue.setAutoRelease(true);
                ret = sQueue;
            }
            break;
        }
        default: {
            throw new IllegalArgumentException("Unknown queue type" + type);
        }
        }
        ret.getDescriptor().setApplicationType(PARTITION_TYPE);
        ret.setDispatcher(dispatcher);
        ret.setStore(this);

        return ret;
    }

    public final void deleteQueueElement(QueueStore.QueueDescriptor descriptor, MessageDelivery elem) {
        elem.acknowledge(descriptor);
    }

    public final boolean isElemPersistent(MessageDelivery elem) {
        return elem.isPersistent();
    }

    public final boolean isFromStore(MessageDelivery elem) {
        return elem.isFromStore();
    }

    public final void persistQueueElement(QueueStore.QueueDescriptor descriptor, ISourceController<?> controller, MessageDelivery elem, long sequence, boolean delayable) throws Exception {
        elem.persist(descriptor, controller, sequence, delayable);
    }

    public final void restoreQueueElements(QueueStore.QueueDescriptor queue, boolean recordsOnly, long firstSequence, long maxSequence, int maxCount,
            org.apache.activemq.queue.QueueStore.RestoreListener<MessageDelivery> listener) {
        database.restoreMessages(queue, recordsOnly, firstSequence, maxSequence, maxCount, listener);
    }

    public final void addQueue(QueueStore.QueueDescriptor queue) {
        database.addQueue(queue);
    }

    public final void deleteQueue(QueueStore.QueueDescriptor queue) {
        database.deleteQueue(queue);
    }
}
