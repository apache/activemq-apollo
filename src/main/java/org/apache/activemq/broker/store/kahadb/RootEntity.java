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
package org.apache.activemq.broker.store.kahadb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.Store.KeyNotFoundException;
import org.apache.activemq.broker.store.Store.QueueQueryResult;
import org.apache.activemq.broker.store.kahadb.Data.MessageAdd;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.queue.QueueStore;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.LongMarshaller;
import org.apache.kahadb.util.Marshaller;

public class RootEntity {

    public final static Marshaller<RootEntity> MARSHALLER = new Marshaller<RootEntity>() {
        public Class<RootEntity> getType() {
            return RootEntity.class;
        }

        public RootEntity readPayload(DataInput is) throws IOException {
            RootEntity rc = new RootEntity();
            rc.state = is.readInt();
            rc.maxMessageKey = is.readLong();
            rc.messageKeyIndex = new BTreeIndex<Long, Location>(is.readLong());
            // rc.locationIndex = new BTreeIndex<Location, Long>(is.readLong());
            rc.destinationIndex = new BTreeIndex<AsciiBuffer, DestinationEntity>(is.readLong());
            rc.messageRefsIndex = new BTreeIndex<Long, Long>(is.readLong());
            if (is.readBoolean()) {
                rc.lastUpdate = Marshallers.LOCATION_MARSHALLER.readPayload(is);
            } else {
                rc.lastUpdate = null;
            }
            return rc;
        }

        public void writePayload(RootEntity object, DataOutput os) throws IOException {
            os.writeInt(object.state);
            os.writeLong(object.maxMessageKey);
            os.writeLong(object.messageKeyIndex.getPageId());
            // os.writeLong(object.locationIndex.getPageId());
            os.writeLong(object.destinationIndex.getPageId());
            os.writeLong(object.messageRefsIndex.getPageId());
            if (object.lastUpdate != null) {
                os.writeBoolean(true);
                Marshallers.LOCATION_MARSHALLER.writePayload(object.lastUpdate, os);
            } else {
                os.writeBoolean(false);
            }
        }
    };

    // The root page the this object's state is stored on.
    // private Page<StoredDBState> page;

    // State information about the index
    private long pageId;
    private int state;
    private Location lastUpdate;
    private boolean loaded;

    // Message Indexes
    private long maxMessageKey;
    private BTreeIndex<Long, Location> messageKeyIndex;
    // private BTreeIndex<Location, Long> locationIndex;
    private BTreeIndex<Long, Long> messageRefsIndex; // Maps message key to ref
    // count:

    // The destinations
    private BTreeIndex<AsciiBuffer, DestinationEntity> destinationIndex;
    private final TreeMap<AsciiBuffer, DestinationEntity> destinations = new TreeMap<AsciiBuffer, DestinationEntity>();

    // /////////////////////////////////////////////////////////////////
    // Lifecycle Methods.
    // /////////////////////////////////////////////////////////////////

    public void allocate(Transaction tx) throws IOException {
        // First time this is created.. Initialize a new pagefile.
        Page<RootEntity> page = tx.allocate();
        pageId = page.getPageId();
        assert pageId == 0;

        state = KahaDBStore.CLOSED_STATE;

        messageKeyIndex = new BTreeIndex<Long, Location>(tx.getPageFile(), tx.allocate().getPageId());
        // locationIndex = new BTreeIndex<Location, Long>(tx.getPageFile(),
        // tx.allocate().getPageId());
        destinationIndex = new BTreeIndex<AsciiBuffer, DestinationEntity>(tx.getPageFile(), tx.allocate().getPageId());
        messageRefsIndex = new BTreeIndex<Long, Long>(tx.getPageFile(), tx.allocate().getPageId());

        page.set(this);
        tx.store(page, MARSHALLER, true);
    }

    public void load(Transaction tx) throws IOException {
        messageKeyIndex.setPageFile(tx.getPageFile());
        messageKeyIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
        messageKeyIndex.setValueMarshaller(Marshallers.LOCATION_MARSHALLER);
        messageKeyIndex.load(tx);
        // Update max message key:
        Entry<Long, Location> last = messageKeyIndex.getLast(tx);
        if (last != null) {
            if (last.getKey() > maxMessageKey) {
                maxMessageKey = last.getKey();
            }
        }
        // locationIndex.setPageFile(tx.getPageFile());
        // locationIndex.setKeyMarshaller(Marshallers.LOCATION_MARSHALLER);
        // locationIndex.setValueMarshaller(LongMarshaller.INSTANCE);
        // locationIndex.load(tx);

        destinationIndex.setPageFile(tx.getPageFile());
        destinationIndex.setKeyMarshaller(Marshallers.ASCII_BUFFER_MARSHALLER);
        destinationIndex.setValueMarshaller(DestinationEntity.MARSHALLER);
        destinationIndex.load(tx);

        messageRefsIndex.setPageFile(tx.getPageFile());
        messageRefsIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
        messageRefsIndex.setValueMarshaller(LongMarshaller.INSTANCE);
        messageRefsIndex.load(tx);

        // Keep the StoredDestinations loaded
        destinations.clear();
        for (Iterator<Entry<AsciiBuffer, DestinationEntity>> iterator = destinationIndex.iterator(tx); iterator.hasNext();) {
            Entry<AsciiBuffer, DestinationEntity> entry = iterator.next();
            entry.getValue().load(tx);
            try {
                addToDestinationCache(entry.getValue());
            } catch (KeyNotFoundException e) {
                //
            }
        }

        // Build up the queue partition hierarchy:
        try {
            constructQueueHierarchy();
        } catch (KeyNotFoundException e) {
            throw new IOException("Inconsistent store", e);
        }
    }

    /**
     * Adds the destination to the destination cache
     * 
     * @param entity
     *            The destination to cache.
     * @throws KeyNotFoundException
     *             If the parent queue could not be found.
     */
    private void addToDestinationCache(DestinationEntity entity) throws KeyNotFoundException {
        QueueStore.QueueDescriptor queue = entity.getDescriptor();

        // If loaded add a reference to us from the parent:
        if (loaded) {
            if (queue.getParent() != null) {
                DestinationEntity parent = destinations.get(queue.getParent());
                if (parent == null) {
                    throw new KeyNotFoundException("Parent queue for " + queue.getQueueName() + " not found");
                }
                parent.addPartition(entity);
            }
        }

        destinations.put(queue.getQueueName(), entity);
    }

    private void removeFromDestinationCache(DestinationEntity entity) {
        QueueStore.QueueDescriptor queue = entity.getDescriptor();

        // If the queue is loaded remove the parent reference:
        if (loaded) {
            if (queue.getParent() != null) {
                DestinationEntity parent = destinations.get(queue.getParent());
                parent.removePartition(entity);
            }
        }
        destinations.remove(queue.getQueueName());
    }

    /**
     * Constructs the mapping of parent queues to child queues.
     * 
     * @throws KeyNotFoundException
     */
    private void constructQueueHierarchy() throws KeyNotFoundException {
        for (DestinationEntity destination : destinations.values()) {
            QueueStore.QueueDescriptor queue = destination.getDescriptor();
            if (queue.getParent() != null) {
                DestinationEntity parent = destinations.get(queue.getParent());
                if (parent == null) {
                    throw new KeyNotFoundException("Parent queue for " + queue.getQueueName() + " not found");
                } else {
                    parent.addPartition(destination);
                }
            }
        }
    }

    public void store(Transaction tx) throws IOException {
        Page<RootEntity> page = tx.load(pageId, null);
        page.set(this);
        tx.store(page, RootEntity.MARSHALLER, true);
    }

    // /////////////////////////////////////////////////////////////////
    // Message Methods.
    // /////////////////////////////////////////////////////////////////
    public long getLastMessageTracking() {
        return maxMessageKey;
    }

    public void messageAdd(Transaction tx, MessageAdd command, Location location) throws IOException {
        long id = command.getMessageKey();
        if (id > maxMessageKey) {
            maxMessageKey = id;
        }
        Location previous = messageKeyIndex.put(tx, id, location);
        if (previous != null) {
            // Message existed.. undo the index update we just did. Chances
            // are it's a transaction replay.
            messageKeyIndex.put(tx, id, previous);
        }
    }

    public void messageRemove(Transaction tx, Long messageKey) throws IOException {
        // Location location = messageKeyIndex.remove(tx, messageKey);
        messageKeyIndex.remove(tx, messageKey);
        // if (location != null) {
        // locationIndex.remove(tx, location);
        // }
    }

    public Location messageGetLocation(Transaction tx, Long messageKey) {
        try {
            return messageKeyIndex.get(tx, messageKey);
        } catch (IOException e) {
            throw new Store.FatalStoreException(e);
        }
    }

    public void addMessageRef(Transaction tx, AsciiBuffer queueName, Long messageKey) {
        try {
            Long refs = messageRefsIndex.get(tx, messageKey);
            if (refs == null) {
                messageRefsIndex.put(tx, messageKey, new Long(1));
            } else {
                messageRefsIndex.put(tx, messageKey, new Long(1 + refs.longValue()));
            }
        } catch (IOException e) {
            throw new Store.FatalStoreException(e);
        }

    }

    public void removeMessageRef(Transaction tx, AsciiBuffer queueName, Long messageKey) {
        try {
            Long refs = messageRefsIndex.get(tx, messageKey);
            if (refs != null) {
                if (refs.longValue() <= 1) {
                    messageRefsIndex.remove(tx, messageKey);
                    // If this is the last record remove, the message
                    messageRemove(tx, messageKey);
                } else {
                    messageRefsIndex.put(tx, messageKey, new Long(refs.longValue() - 1));
                }
            }
        } catch (IOException e) {
            throw new Store.FatalStoreException(e);
        }
    }

    // /////////////////////////////////////////////////////////////////
    // Queue Methods.
    // /////////////////////////////////////////////////////////////////
    public void queueAdd(Transaction tx, QueueStore.QueueDescriptor queue) throws IOException {
        if (destinationIndex.get(tx, queue.getQueueName()) == null) {
            DestinationEntity rc = new DestinationEntity();
            rc.setQueueDescriptor(queue);
            rc.allocate(tx);
            destinationIndex.put(tx, queue.getQueueName(), rc);
            rc.load(tx);
            try {
                addToDestinationCache(rc);
            } catch (KeyNotFoundException e) {
                throw new Store.FatalStoreException("Inconsistent QueueStore: " + e.getMessage(), e);
            }
        }
    }

    public void queueRemove(Transaction tx, QueueStore.QueueDescriptor queue) throws IOException {
        DestinationEntity destination = destinations.get(queue.getQueueName());
        if (destination != null) {
            // Remove the message references.
            // TODO this should probably be optimized.
            Iterator<Entry<Long, Long>> messages = destination.listTrackingNums(tx);
            while (messages.hasNext()) {
                Long messageKey = messages.next().getKey();
                removeMessageRef(tx, queue.getQueueName(), messageKey);
            }
            destinationIndex.remove(tx, queue.getQueueName());
            removeFromDestinationCache(destination);
            destination.deallocate(tx);
        }
    }

    public DestinationEntity getDestination(QueueStore.QueueDescriptor queue) {
        return destinations.get(queue.getQueueName());
    }

    public Iterator<QueueQueryResult> queueList(Transaction tx, short type, QueueStore.QueueDescriptor firstQueue, int max) throws IOException {
        LinkedList<QueueQueryResult> results = new LinkedList<QueueQueryResult>();
        Collection<DestinationEntity> values = (firstQueue == null ? destinations.values() : destinations.tailMap(firstQueue.getQueueName()).values());

        for (DestinationEntity de : values) {
            if (results.size() >= max) {
                break;
            }

            if (type == -1 || de.getDescriptor().getApplicationType() == type) {
                results.add(queryQueue(tx, de));
            }
        }
        return results.iterator();
    }

    private final QueueQueryResult queryQueue(Transaction tx, DestinationEntity de) throws IOException {

        QueueQueryResultImpl result = new QueueQueryResultImpl();
        result.count = de.getCount(tx);
        result.size = de.getSize(tx);
        result.firstSequence = de.getFirstSequence(tx);
        result.lastSequence = de.getLastSequence(tx);
        result.desc = de.getDescriptor().copy();
        Iterator<DestinationEntity> partitions = de.getPartitions();
        if (partitions != null && partitions.hasNext()) {
            result.partitions = new LinkedList<QueueQueryResult>();
            while (partitions.hasNext()) {
                result.partitions.add(queryQueue(tx, destinations.get(partitions.next().getDescriptor().getQueueName())));
            }
        }

        return result;
    }

    public long getPageId() {
        return pageId;
    }

    public void setPageId(long pageId) {
        this.pageId = pageId;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public Location getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(Location lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    private static class QueueQueryResultImpl implements QueueQueryResult {

        QueueStore.QueueDescriptor desc;
        Collection<QueueQueryResult> partitions;
        long size;
        int count;
        long firstSequence;
        long lastSequence;

        public QueueStore.QueueDescriptor getDescriptor() {
            return desc;
        }

        public Collection<QueueQueryResult> getPartitions() {
            return partitions;
        }

        public long getSize() {
            return size;
        }

        public int getCount() {
            return count;
        }

        public long getFirstSequence() {
            return firstSequence;
        }

        public long getLastSequence() {
            return lastSequence;
        }
    }
}