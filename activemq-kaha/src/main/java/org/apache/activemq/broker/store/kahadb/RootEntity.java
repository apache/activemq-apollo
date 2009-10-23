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
import java.util.List;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.Store.KeyNotFoundException;
import org.apache.activemq.broker.store.Store.QueueQueryResult;
import org.apache.activemq.broker.store.Store.SubscriptionRecord;
import org.apache.activemq.broker.store.kahadb.Data.MessageAdd;
import org.apache.activemq.broker.store.kahadb.Data.SubscriptionAdd;
import org.apache.activemq.broker.store.kahadb.Data.SubscriptionAdd.SubscriptionAddBuffer;
import org.apache.activemq.protobuf.InvalidProtocolBufferException;
import org.apache.activemq.queue.QueueDescriptor;
import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.util.marshaller.IntegerMarshaller;
import org.apache.activemq.util.marshaller.LongMarshaller;
import org.apache.activemq.util.marshaller.Marshaller;
import org.apache.activemq.util.marshaller.VariableMarshaller;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.index.BTreeVisitor;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.Transaction;

public class RootEntity {

    //TODO remove this one performance testing is complete. 
    private static final boolean USE_LOC_INDEX = true;

    private static final int VERSION = 0;

    public final static Marshaller<RootEntity> MARSHALLER = new VariableMarshaller<RootEntity>() {
        public RootEntity readPayload(DataInput is) throws IOException {
            RootEntity rc = new RootEntity();
            rc.state = is.readInt();
            is.readInt(); //VERSION 
            rc.maxMessageKey = is.readLong();
            rc.messageKeyIndex = new BTreeIndex<Long, Location>(is.readLong());
            if (USE_LOC_INDEX)
                rc.locationIndex = new BTreeIndex<Integer, Long>(is.readLong());
            rc.destinationIndex = new BTreeIndex<AsciiBuffer, DestinationEntity>(is.readLong());
            rc.messageRefsIndex = new BTreeIndex<Long, Long>(is.readLong());
            rc.subscriptionIndex = new BTreeIndex<AsciiBuffer, Buffer>(is.readLong());
            rc.mapIndex = new BTreeIndex<AsciiBuffer, Long>(is.readLong());
            if (is.readBoolean()) {
                rc.lastUpdate = Marshallers.LOCATION_MARSHALLER.readPayload(is);
            } else {
                rc.lastUpdate = null;
            }
            return rc;
        }

        public void writePayload(RootEntity object, DataOutput os) throws IOException {
            os.writeInt(object.state);
            os.writeInt(VERSION);
            os.writeLong(object.maxMessageKey);
            os.writeLong(object.messageKeyIndex.getPageId());
            if (USE_LOC_INDEX)
                os.writeLong(object.locationIndex.getPageId());
            os.writeLong(object.destinationIndex.getPageId());
            os.writeLong(object.messageRefsIndex.getPageId());
            os.writeLong(object.subscriptionIndex.getPageId());
            os.writeLong(object.mapIndex.getPageId());
            if (object.lastUpdate != null) {
                os.writeBoolean(true);
                Marshallers.LOCATION_MARSHALLER.writePayload(object.lastUpdate, os);
            } else {
                os.writeBoolean(false);
            }
        }

        public int estimatedSize(RootEntity object) {
            throw new UnsupportedOperationException();
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
    private BTreeIndex<Integer, Long> locationIndex;
    private BTreeIndex<Long, Long> messageRefsIndex; // Maps message key to ref
    // count:

    // The destinations
    private BTreeIndex<AsciiBuffer, DestinationEntity> destinationIndex;
    private final TreeMap<AsciiBuffer, DestinationEntity> destinations = new TreeMap<AsciiBuffer, DestinationEntity>();

    // Subscriptions
    private BTreeIndex<AsciiBuffer, Buffer> subscriptionIndex;

    // Maps:
    private BTreeIndex<AsciiBuffer, Long> mapIndex;
    private TreeMap<AsciiBuffer, BTreeIndex<AsciiBuffer, Buffer>> mapCache = new TreeMap<AsciiBuffer, BTreeIndex<AsciiBuffer,Buffer>>();

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
        if (USE_LOC_INDEX)
            locationIndex = new BTreeIndex<Integer, Long>(tx.getPageFile(), tx.allocate().getPageId());
        destinationIndex = new BTreeIndex<AsciiBuffer, DestinationEntity>(tx.getPageFile(), tx.allocate().getPageId());
        messageRefsIndex = new BTreeIndex<Long, Long>(tx.getPageFile(), tx.allocate().getPageId());
        subscriptionIndex = new BTreeIndex<AsciiBuffer, Buffer>(tx.getPageFile(), tx.allocate().getPageId());
        mapIndex = new BTreeIndex<AsciiBuffer, Long>(tx.getPageFile(), tx.allocate().getPageId());

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

        if (USE_LOC_INDEX) {
            locationIndex.setPageFile(tx.getPageFile());
            locationIndex.setKeyMarshaller(IntegerMarshaller.INSTANCE);
            locationIndex.setValueMarshaller(LongMarshaller.INSTANCE);
            locationIndex.load(tx);
        }

        subscriptionIndex.setPageFile(tx.getPageFile());
        subscriptionIndex.setKeyMarshaller(Marshallers.ASCII_BUFFER_MARSHALLER);
        subscriptionIndex.setValueMarshaller(Marshallers.BUFFER_MARSHALLER);
        subscriptionIndex.load(tx);

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
            IOException ioe = new IOException("Inconsistent store");
            ioe.initCause(e);
            throw ioe;
        }

        //Load Maps:
        mapIndex.setPageFile(tx.getPageFile());
        mapIndex.setKeyMarshaller(Marshallers.ASCII_BUFFER_MARSHALLER);
        mapIndex.setValueMarshaller(LongMarshaller.INSTANCE);
        mapIndex.load(tx);

        //Load all of the maps and cache them:
        for (Iterator<Entry<AsciiBuffer, Long>> iterator = mapIndex.iterator(tx); iterator.hasNext();) {
            Entry<AsciiBuffer, Long> entry = iterator.next();
            BTreeIndex<AsciiBuffer, Buffer> map = new BTreeIndex<AsciiBuffer, Buffer>(tx.getPageFile(), entry.getValue());
            map.setKeyMarshaller(Marshallers.ASCII_BUFFER_MARSHALLER);
            map.setValueMarshaller(Marshallers.BUFFER_MARSHALLER);
            map.load(tx);
            mapCache.put(entry.getKey(), map);
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
        QueueDescriptor queue = entity.getDescriptor();

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
        QueueDescriptor queue = entity.getDescriptor();

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
            QueueDescriptor queue = destination.getDescriptor();
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
        } else {
            if (USE_LOC_INDEX) {
                Long refs = locationIndex.get(tx, location.getDataFileId());
                if (refs == null) {
                    locationIndex.put(tx, location.getDataFileId(), new Long(1));
                } else {
                    locationIndex.put(tx, location.getDataFileId(), new Long(refs.longValue() + 1));
                }
            }
        }
    }

    public void messageRemove(Transaction tx, Long messageKey) throws IOException {
        // Location location = messageKeyIndex.remove(tx, messageKey);
        Location location = messageKeyIndex.remove(tx, messageKey);
        if (USE_LOC_INDEX && location != null) {
            Long refs = locationIndex.get(tx, location.getDataFileId());
            if (refs != null) {
                if (refs.longValue() <= 1) {
                    locationIndex.remove(tx, location.getDataFileId());
                } else {
                    locationIndex.put(tx, location.getDataFileId(), new Long(refs.longValue() - 1));
                }
            }
        }
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
    // Client Methods.
    // /////////////////////////////////////////////////////////////////

    /**
     * Returns a list of all of the stored subscriptions.
     * 
     * @param tx
     *            The transaction under which this is to be executed.
     * @return a list of all of the stored subscriptions.
     * @throws IOException
     */
    public Iterator<SubscriptionRecord> listSubsriptions(Transaction tx) throws IOException {

        final LinkedList<SubscriptionRecord> rc = new LinkedList<SubscriptionRecord>();

        subscriptionIndex.visit(tx, new BTreeVisitor<AsciiBuffer, Buffer>() {
            public boolean isInterestedInKeysBetween(AsciiBuffer first, AsciiBuffer second) {
                return true;
            }

            public void visit(List<AsciiBuffer> keys, List<Buffer> values) {
                for (Buffer b : values) {
                    try {
                        rc.add(toSubscriptionRecord(b));
                    } catch (InvalidProtocolBufferException e) {
                        throw new Store.FatalStoreException(e);
                    }
                }
            }

            public boolean isSatiated() {
                return false;
            }
        });

        return rc.iterator();
    }

    /**
     * @param tx
     * @param name
     * @throws IOException
     */
    public void removeSubscription(Transaction tx, AsciiBuffer name) throws IOException {
        subscriptionIndex.remove(tx, name);
    }

    /**
     * @param tx
     * @param name
     * @throws IOException
     */
    public void addSubscription(Transaction tx, SubscriptionAdd subscription) throws IOException {
        subscriptionIndex.put(tx, subscription.getName(), subscription.freeze().toFramedBuffer());
    }

    /**
     * @param name
     * @return
     * @throws IOException
     */
    public SubscriptionRecord getSubscription(Transaction tx, AsciiBuffer name) throws IOException {
        return toSubscriptionRecord(subscriptionIndex.get(tx, name));
    }

    /**
     * Converts a Subscription buffer to a SubscriptionRecord.
     * 
     * @param b
     *            The buffer
     * @return The record.
     * @throws InvalidProtocolBufferException
     */
    private static SubscriptionRecord toSubscriptionRecord(Buffer b) throws InvalidProtocolBufferException {
        if (b == null) {
            return null;
        }

        SubscriptionRecord rc = null;
        if (b != null) {
            SubscriptionAddBuffer sab = SubscriptionAddBuffer.parseFramed(b);
            if (sab != null) {
                rc = new SubscriptionRecord();
                rc.setName(sab.getName());
                rc.setDestination(sab.getDestination());
                rc.setIsDurable(sab.getDurable());
                if (sab.hasAttachment())
                    rc.setAttachment(sab.getAttachment());
                if (sab.hasSelector())
                    rc.setSelector(sab.getSelector());
                if (sab.hasTte())
                    rc.setTte(sab.getTte());

            }
        }
        return rc;
    }

    // /////////////////////////////////////////////////////////////////
    // Queue Methods.
    // /////////////////////////////////////////////////////////////////
    public void queueAdd(Transaction tx, QueueDescriptor queue) throws IOException {
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

    public void queueRemove(Transaction tx, QueueDescriptor queue) throws IOException {
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

    public DestinationEntity getDestination(QueueDescriptor queue) {
        return destinations.get(queue.getQueueName());
    }

    public Iterator<QueueQueryResult> queueList(Transaction tx, short type, QueueDescriptor firstQueue, int max) throws IOException {
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

    // /////////////////////////////////////////////////////////////////
    // Map Methods.
    // /////////////////////////////////////////////////////////////////
    public final void mapAdd(AsciiBuffer key, Transaction tx) throws IOException {
        BTreeIndex<AsciiBuffer, Buffer> map = mapCache.get(key);

        if (map == null) {
            long pageId = tx.allocate().getPageId();
            map = new BTreeIndex<AsciiBuffer, Buffer>(tx.getPageFile(), pageId);
            map.setKeyMarshaller(Marshallers.ASCII_BUFFER_MARSHALLER);
            map.setValueMarshaller(Marshallers.BUFFER_MARSHALLER);
            map.load(tx);
            mapIndex.put(tx, key, pageId);
            mapCache.put(key, map);
        }
    }

    public final void mapRemove(AsciiBuffer key, Transaction tx) throws IOException {
        BTreeIndex<AsciiBuffer, Buffer> map = mapCache.remove(key);
        if (map != null) {
            map.clear(tx);
            map.unload(tx);
            mapIndex.remove(tx, key);
        }
    }

    public final void mapAddEntry(AsciiBuffer name, AsciiBuffer key, Buffer value, Transaction tx) throws IOException {
        BTreeIndex<AsciiBuffer, Buffer> map = mapCache.get(name);
        if (map == null) {
            mapAdd(name, tx);
            map = mapCache.get(name);
        }

        map.put(tx, key, value);

    }

    public final void mapRemoveEntry(AsciiBuffer name, AsciiBuffer key, Transaction tx) throws IOException, KeyNotFoundException {
        BTreeIndex<AsciiBuffer, Buffer> map = mapCache.get(name);
        if (map == null) {
            throw new KeyNotFoundException(name.toString());
        }
        map.remove(tx, key);
    }

    public final Buffer mapGetEntry(AsciiBuffer name, AsciiBuffer key, Transaction tx) throws IOException, KeyNotFoundException {
        BTreeIndex<AsciiBuffer, Buffer> map = mapCache.get(name);
        if (map == null) {
            throw new KeyNotFoundException(name.toString());
        }
        return map.get(tx, key);
    }

    public final Iterator<AsciiBuffer> mapList(AsciiBuffer first, int count, Transaction tx) {
        LinkedList<AsciiBuffer> results = new LinkedList<AsciiBuffer>();

        Collection<AsciiBuffer> values = (first == null ? mapCache.keySet() : mapCache.tailMap(first).keySet());
        for (AsciiBuffer key : values) {
            results.add(key);
        }

        return results.iterator();
    }

    public final Iterator<AsciiBuffer> mapListKeys(AsciiBuffer name, AsciiBuffer first, int count, Transaction tx) throws IOException, KeyNotFoundException {
        BTreeIndex<AsciiBuffer, Buffer> map = mapCache.get(name);
        if (map == null) {
            throw new KeyNotFoundException(name.toString());
        }

        final LinkedList<AsciiBuffer> results = new LinkedList<AsciiBuffer>();

        if (first != null && count > 0) {
            map.visit(tx, new BTreeVisitor.GTEVisitor<AsciiBuffer, Buffer>(first, count) {

                @Override
                protected void matched(AsciiBuffer key, Buffer value) {
                    results.add(key);
                }
            });
        } else {
            Iterator<Entry<AsciiBuffer, Buffer>> iterator = map.iterator(tx);
            while (iterator.hasNext()) {
                Entry<AsciiBuffer, Buffer> e = iterator.next();
                results.add(e.getKey());
            }
        }

        return results.iterator();
    }

    // /////////////////////////////////////////////////////////////////
    // Map Methods.
    // /////////////////////////////////////////////////////////////////

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

        QueueDescriptor desc;
        Collection<QueueQueryResult> partitions;
        long size;
        int count;
        long firstSequence;
        long lastSequence;

        public QueueDescriptor getDescriptor() {
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

    /**
     * @param lastAppendLocation
     * @param tx
     * @return the number of undone index entries
     */
    public int recoverIndex(Location lastAppendLocation, Transaction tx) {

        //TODO check that none of the locations specified by the indexes
        //are past the last update location in the journal. This can happen
        //if the index is flushed before the journal. 
        int count = 0;

        //TODO: It might be better to tie the the index update to the journal write
        //so that we can be sure that all journal entries are on disk prior to 
        //index update. 

        //Scan MessageKey Index to find message keys past the last append 
        //location:
        //        final ArrayList<Long> matches = new ArrayList<Long>();
        //        messageKeyIndex.visit(tx, new BTreeVisitor.GTEVisitor<Location, Long>(lastAppendLocation) {
        //
        //            @Override
        //            protected void matched(Location key, Long value) {
        //                matches.add(value);
        //            }
        //        });

        //        for (Long sequenceId : matches) {
        //        MessageKeys keys = sd.orderIndex.remove(tx, sequenceId);
        //        sd.locationIndex.remove(tx, keys.location);
        //        sd.messageIdIndex.remove(tx, keys.messageId);
        //        count++;
        //        }

        //                 @Override
        //                 protected void matched(Location key, Long value) {
        //                 matches.add(value);
        //                 }
        //                 });
        //                            
        //                            
        //                 for (Long sequenceId : matches) {
        //                 MessageKeys keys = sd.orderIndex.remove(tx, sequenceId);
        //                 sd.locationIndex.remove(tx, keys.location);
        //                 sd.messageIdIndex.remove(tx, keys.messageId);
        //                 undoCounter++;
        //             })

        //        for (DestinationEntity de : destinations.values()) {
        //             final ArrayList<Long> matches = new ArrayList<Long>();
        //             // Find all the Locations that are >= than the last Append Location.
        //             sd.locationIndex.visit(tx, new BTreeVisitor.GTEVisitor<Location,
        //                 Long>(lastAppendLocation) {
        //                 @Override
        //                 protected void matched(Location key, Long value) {
        //                 matches.add(value);
        //                 }
        //                 });
        //                            
        //                            
        //                 for (Long sequenceId : matches) {
        //                 MessageKeys keys = sd.orderIndex.remove(tx, sequenceId);
        //                 sd.locationIndex.remove(tx, keys.location);
        //                 sd.messageIdIndex.remove(tx, keys.messageId);
        //                 undoCounter++;
        //             }
        //        }
        return count;
    }

    /**
     * Go through indexes checking to
     * 
     * @param gcCandidateSet
     * @throws IOException
     */
    final void removeGCCandidates(final TreeSet<Integer> gcCandidateSet, Transaction tx) throws IOException {

        // Don't GC files after the first in progress tx
        Location firstTxLocation = lastUpdate;

        if (firstTxLocation != null) {
            while (!gcCandidateSet.isEmpty()) {
                Integer last = gcCandidateSet.last();
                if (last >= firstTxLocation.getDataFileId()) {
                    gcCandidateSet.remove(last);
                } else {
                    break;
                }
            }
        }

        if (gcCandidateSet.isEmpty()) {
            return;
        }

        if (!USE_LOC_INDEX) {
            return;
        }

        // Go through the location index to see if we can remove gc candidates:
        // Use a visitor to cut down the number of pages that we load
        locationIndex.visit(tx, new BTreeVisitor<Integer, Long>() {
            int last = -1;

            public boolean isInterestedInKeysBetween(Integer first, Integer second) {
                if (first == null) {
                    SortedSet<Integer> subset = gcCandidateSet.headSet(second + 1);
                    if (!subset.isEmpty() && subset.last().equals(second)) {
                        subset.remove(second);
                    }
                    return !subset.isEmpty();
                } else if (second == null) {
                    SortedSet<Integer> subset = gcCandidateSet.tailSet(first);
                    if (!subset.isEmpty() && subset.first().equals(first)) {
                        subset.remove(first);
                    }
                    return !subset.isEmpty();
                } else {
                    SortedSet<Integer> subset = gcCandidateSet.subSet(first, second + 1);
                    if (!subset.isEmpty() && subset.first().equals(first)) {
                        subset.remove(first);
                    }
                    if (!subset.isEmpty() && subset.last().equals(second)) {
                        subset.remove(second);
                    }
                    return !subset.isEmpty();
                }
            }

            public void visit(List<Integer> keys, List<Long> values) {
                for (Integer l : keys) {
                    int fileId = l;
                    if (last != fileId) {
                        gcCandidateSet.remove(fileId);
                        last = fileId;
                    }
                }
            }

            public boolean isSatiated() {
                return !gcCandidateSet.isEmpty();
            }
        });

    }

}