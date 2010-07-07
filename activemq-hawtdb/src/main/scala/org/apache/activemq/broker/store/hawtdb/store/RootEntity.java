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
package org.apache.activemq.broker.store.hawtdb.store;

import org.apache.activemq.apollo.store.QueueRecord;
import org.apache.activemq.apollo.store.QueueStatus;
import org.apache.activemq.apollo.store.SubscriptionRecord;
import org.apache.activemq.broker.store.hawtdb.store.Data.MessageAdd;
import org.apache.activemq.broker.store.hawtdb.store.Data.SubscriptionAdd;
import org.apache.activemq.broker.store.hawtdb.store.Data.SubscriptionAdd.SubscriptionAddBuffer;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.proto.InvalidProtocolBufferException;
import org.fusesource.hawtdb.api.*;
import org.fusesource.hawtdb.internal.journal.Location;
import org.fusesource.hawtdb.util.marshaller.IntegerMarshaller;
import org.fusesource.hawtdb.util.marshaller.LocationMarshaller;
import org.fusesource.hawtdb.util.marshaller.LongMarshaller;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class RootEntity {

    //TODO remove this once performance testing is complete. 
    private static final boolean USE_LOC_INDEX = true;
    private static final int VERSION = 0;


    private static final BTreeIndexFactory<Long, Location> messageKeyIndexFactory = new BTreeIndexFactory<Long, Location>();
    private static final BTreeIndexFactory<Integer, Long> locationIndexFactory = new BTreeIndexFactory<Integer, Long>();
    private static final BTreeIndexFactory<Long, Long> messageRefsIndexFactory = new BTreeIndexFactory<Long, Long>();
    private static final BTreeIndexFactory<Long, DestinationEntity> destinationIndexFactory = new BTreeIndexFactory<Long, DestinationEntity>();
    private static final BTreeIndexFactory<AsciiBuffer, Buffer> subscriptionIndexFactory = new BTreeIndexFactory<AsciiBuffer, Buffer>();
    private static final BTreeIndexFactory<AsciiBuffer, Integer> mapIndexFactory = new BTreeIndexFactory<AsciiBuffer, Integer>();
    private static final BTreeIndexFactory<AsciiBuffer, Buffer> mapInstanceIndexFactory = new BTreeIndexFactory<AsciiBuffer, Buffer>();

    static {
        messageKeyIndexFactory.setKeyMarshaller(LongMarshaller.INSTANCE);
        messageKeyIndexFactory.setValueMarshaller(LocationMarshaller.INSTANCE);
        messageKeyIndexFactory.setDeferredEncoding(true);

        locationIndexFactory.setKeyMarshaller(IntegerMarshaller.INSTANCE);
        locationIndexFactory.setValueMarshaller(LongMarshaller.INSTANCE);
        locationIndexFactory.setDeferredEncoding(true);

        messageRefsIndexFactory.setKeyMarshaller(LongMarshaller.INSTANCE);
        messageRefsIndexFactory.setValueMarshaller(LongMarshaller.INSTANCE);
        messageRefsIndexFactory.setDeferredEncoding(true);

        destinationIndexFactory.setKeyMarshaller(LongMarshaller.INSTANCE);
        destinationIndexFactory.setValueMarshaller(DestinationEntity.MARSHALLER);
        destinationIndexFactory.setDeferredEncoding(true);

        subscriptionIndexFactory.setKeyMarshaller(Marshallers.ASCII_BUFFER_MARSHALLER);
        subscriptionIndexFactory.setValueMarshaller(Marshallers.BUFFER_MARSHALLER);
        subscriptionIndexFactory.setDeferredEncoding(true);

        mapIndexFactory.setKeyMarshaller(Marshallers.ASCII_BUFFER_MARSHALLER);
        mapIndexFactory.setValueMarshaller(IntegerMarshaller.INSTANCE);
        mapIndexFactory.setDeferredEncoding(true);
    }

    // The root page the this object's state is stored on.
    // private Page<StoredDBState> page;

    // State information about the index
    Data data;
    private long maxMessageKey;

    static class Data {
        private int state;
        // Message Indexes
        private long maxMessageKey;
        private Location lastUpdate;
        
        private SortedIndex<Long, Location> messageKeyIndex;
        private SortedIndex<Integer, Long> locationIndex;
        private SortedIndex<Long, Long> messageRefsIndex; // Maps message key to ref
        // count:

        // The destinations
        private SortedIndex<Long, DestinationEntity> destinationIndex;

        // Subscriptions
        private SortedIndex<AsciiBuffer, Buffer> subscriptionIndex;

        // Maps:
        private SortedIndex<AsciiBuffer, Integer> mapIndex;

        public void create(Transaction tx) {
            state = HawtDBManager.CLOSED_STATE;
            messageKeyIndex = messageKeyIndexFactory.create(tx, tx.alloc());
            if (USE_LOC_INDEX)
                locationIndex = locationIndexFactory.create(tx, tx.alloc());
            destinationIndex = destinationIndexFactory.create(tx, tx.alloc());
            messageRefsIndex = messageRefsIndexFactory.create(tx, tx.alloc());
            subscriptionIndex = subscriptionIndexFactory.create(tx, tx.alloc());
            mapIndex = mapIndexFactory.create(tx, tx.alloc());

        }
    }

    EncoderDecoder<Data>  DATA_ENCODER_DECODER = new AbstractStreamEncoderDecoder<Data>() {
        @Override
        protected void encode(Paged paged, DataOutputStream os, Data object) throws IOException {
            os.writeInt(object.state);
            os.writeInt(VERSION);
            os.writeLong(object.maxMessageKey);
            os.writeInt(object.messageKeyIndex.getPage());
            if (USE_LOC_INDEX)
                os.writeInt(object.locationIndex.getPage());
            os.writeInt(object.destinationIndex.getPage());
            os.writeInt(object.messageRefsIndex.getPage());
            os.writeInt(object.subscriptionIndex.getPage());
            os.writeInt(object.mapIndex.getPage());
            if (object.lastUpdate != null) {
                os.writeBoolean(true);
                LocationMarshaller.INSTANCE.writePayload(object.lastUpdate, os);
            } else {
                os.writeBoolean(false);
            }
        }

        @Override
        protected RootEntity.Data decode(Paged paged, DataInputStream is) throws IOException {
            Data rc = new Data();
            rc.state = is.readInt();
            is.readInt(); //VERSION
            rc.maxMessageKey = is.readLong();
            rc.messageKeyIndex = messageKeyIndexFactory.open(paged, is.readInt());
            if (USE_LOC_INDEX)
                rc.locationIndex = locationIndexFactory.open(paged, is.readInt());
            rc.destinationIndex = destinationIndexFactory.open(paged, is.readInt());
            rc.messageRefsIndex = messageRefsIndexFactory.open(paged, is.readInt());
            rc.subscriptionIndex = subscriptionIndexFactory.open(paged, is.readInt());
            rc.mapIndex = mapIndexFactory.open(paged, is.readInt());
            if (is.readBoolean()) {
                rc.lastUpdate = LocationMarshaller.INSTANCE.readPayload(is);
            } else {
                rc.lastUpdate = null;
            }
            return rc;
        }
    };


    // /////////////////////////////////////////////////////////////////
    // Lifecycle Methods.
    // /////////////////////////////////////////////////////////////////

    public void allocate(Transaction tx) throws IOException {
        // First time this is created.. Initialize a new pagefile.
        int pageId = tx.alloc();
        assert pageId == 0;
        data = new Data();
        data.create(tx);
        tx.put(DATA_ENCODER_DECODER, pageId, data);
    }

    public void load(Transaction tx) throws IOException {
        data = tx.get(DATA_ENCODER_DECODER, 0);

        // Update max message key:
        maxMessageKey = data.maxMessageKey;
        Entry<Long, Location> last = data.messageKeyIndex.getLast();
        if (last != null) {
            if (last.getKey() > maxMessageKey) {
                maxMessageKey = last.getKey();
            }
        }

    }

    @Deprecated // TODO: keep data immutable
    public void store(Transaction tx) throws IOException {
        // TODO: need ot make Data immutable..
        tx.put(DATA_ENCODER_DECODER, 0, data);
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
        Location previous = data.messageKeyIndex.put(id, location);
        if (previous != null) {
            // Message existed.. undo the index update we just did. Chances
            // are it's a transaction replay.
            data.messageKeyIndex.put(id, previous);
        } else {
            if (USE_LOC_INDEX) {
                Long refs = data.locationIndex.get(location.getDataFileId());
                if (refs == null) {
                    data.locationIndex.put(location.getDataFileId(), new Long(1));
                } else {
                    data.locationIndex.put(location.getDataFileId(), new Long(refs.longValue() + 1));
                }
            }
        }
    }

    public void messageRemove(Long messageKey) {
        // Location location = messageKeyIndex.remove(tx, messageKey);
        Location location = data.messageKeyIndex.remove(messageKey);
        if (USE_LOC_INDEX && location != null) {
            Long refs = data.locationIndex.get(location.getDataFileId());
            if (refs != null) {
                if (refs.longValue() <= 1) {
                    data.locationIndex.remove(location.getDataFileId());
                } else {
                    data.locationIndex.put(location.getDataFileId(), new Long(refs.longValue() - 1));
                }
            }
        }
    }

    public Location messageGetLocation(Transaction tx, Long messageKey) {
        return data.messageKeyIndex.get(messageKey);
    }

    public void addMessageRef(Transaction tx, Long messageKey) {
        try {
            Long refs = data.messageRefsIndex.get(messageKey);
            if (refs == null) {
                data.messageRefsIndex.put(messageKey, new Long(1));
            } else {
                data.messageRefsIndex.put(messageKey, new Long(1 + refs.longValue()));
            }
        } catch (RuntimeException e) {
            throw new FatalStoreException(e);
        }

    }

    public void removeMessageRef(Transaction tx, Long messageKey) {
        try {
            Long refs = data.messageRefsIndex.get(messageKey);
            if (refs != null) {
                if (refs.longValue() <= 1) {
                    data.messageRefsIndex.remove(messageKey);
                    // If this is the last record remove, the message
                    messageRemove(messageKey);
                } else {
                    data.messageRefsIndex.put(messageKey, new Long(refs.longValue() - 1));
                }
            }
        } catch (RuntimeException e) {
            throw new FatalStoreException(e);
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

        data.subscriptionIndex.visit(new IndexVisitor<AsciiBuffer, Buffer>() {
            public boolean isInterestedInKeysBetween(AsciiBuffer first, AsciiBuffer second) {
                return true;
            }

            public void visit(List<AsciiBuffer> keys, List<Buffer> values) {
                for (Buffer b : values) {
                    try {
                        rc.add(toSubscriptionRecord(b));
                    } catch (InvalidProtocolBufferException e) {
                        throw new FatalStoreException(e);
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
     * @param name
     * @throws IOException
     */
    public void removeSubscription(AsciiBuffer name) throws IOException {
        data.subscriptionIndex.remove(name);
    }

    /**
     * @throws IOException
     */
    public void addSubscription(SubscriptionAdd subscription) throws IOException {
        data.subscriptionIndex.put(subscription.getName(), subscription.freeze().toFramedBuffer());
    }

    /**
     * @param name
     * @return
     * @throws IOException
     */
    public SubscriptionRecord getSubscription(AsciiBuffer name) throws IOException {
        return toSubscriptionRecord(data.subscriptionIndex.get(name));
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
                rc.name = sab.getName();
                rc.destination = sab.getDestination();
                rc.isDurable = sab.getDurable();
                if (sab.hasAttachment())
                    rc.attachment = sab.getAttachment();
                if (sab.hasSelector())
                    rc.selector = sab.getSelector();
                if (sab.hasTte())
                    rc.expiration = sab.getTte();

            }
        }
        return rc;
    }

    // /////////////////////////////////////////////////////////////////
    // Queue Methods.
    // /////////////////////////////////////////////////////////////////
    public void queueAdd(Transaction tx, QueueRecord queue) throws IOException {
        if (data.destinationIndex.get(queue.id) == null) {
            DestinationEntity rc = new DestinationEntity();
            rc.setQueueDescriptor(queue);
            rc.allocate(tx);
            data.destinationIndex.put(queue.id, rc);
        }
    }

    public void queueRemove(Transaction tx, Long queueKey) throws IOException {
        DestinationEntity destination = data.destinationIndex.get(queueKey);
        if (destination != null) {
            // Remove the message references.
            // TODO this should probably be optimized.
            Iterator<Entry<Long, Long>> messages = destination.listTrackingNums(tx);
            while (messages.hasNext()) {
                Long messageKey = messages.next().getKey();
                removeMessageRef(tx, messageKey);
            }
            data.destinationIndex.remove(queueKey);
            destination.deallocate(tx);
        }
    }

    public DestinationEntity getDestination(Long queue) {
        return data.destinationIndex.get(queue);
    }

    public Iterator<org.apache.activemq.apollo.store.QueueStatus> queueList(Transaction tx, AsciiBuffer type, QueueRecord firstQueue, int max) throws IOException {
        LinkedList<org.apache.activemq.apollo.store.QueueStatus> results = new LinkedList<org.apache.activemq.apollo.store.QueueStatus>();

        final Iterator<Entry<Long, DestinationEntity>> i;
        Long x = firstQueue==null? null : (Long)firstQueue.id;
        i = data.destinationIndex.iterator(x);
        while (i.hasNext()) {
            Entry<Long, DestinationEntity> entry = i.next();
            DestinationEntity de = entry.getValue();
            if (results.size() >= max) {
                break;
            }

            if (type == null || type.equals(de.getQueueRecord().queueType) ) {
                results.add(queryQueue(tx, de));
            }
        }
        return results.iterator();
    }

    private final org.apache.activemq.apollo.store.QueueStatus queryQueue(Transaction tx, DestinationEntity de) throws IOException {

        QueueStatus result = new QueueStatus();
        result.count = de.getCount(tx);
        result.size = de.getSize(tx);
        result.first = de.getFirstSequence(tx);
        result.last = de.getLastSequence(tx);
        result.record = de.getQueueRecord();
        
//        Iterator<DestinationEntity> partitions = de.getPartitions();
//        if (partitions != null && partitions.hasNext()) {
//            result.partitions = new LinkedList<org.apache.activemq.apollo.store.QueueStatus>();
//            while (partitions.hasNext()) {
//                result.partitions.add(queryQueue(tx, getDestination(partitions.next().getQueueRecord()) ));
//            }
//        }

        return result;
    }

    // /////////////////////////////////////////////////////////////////
    // Map Methods.
    // /////////////////////////////////////////////////////////////////
    public final void mapAdd(AsciiBuffer key, Transaction tx) throws IOException {
        final Integer page = data.mapIndex.get(key);
        if (page == null) {
            int pageId = tx.alloc();
            SortedIndex<AsciiBuffer, Buffer> map = mapInstanceIndexFactory.create(tx, pageId);
            data.mapIndex.put(key, pageId);
        }
    }

    public final void mapRemove(AsciiBuffer key, Transaction tx) throws IOException {
        final Integer pageId = data.mapIndex.remove(key);
        if (pageId != null) {
            SortedIndex<AsciiBuffer, Buffer> map = mapInstanceIndexFactory.open(tx, pageId);
            map.clear();
            tx.free(pageId);
        }
    }

    public final void mapAddEntry(AsciiBuffer name, AsciiBuffer key, Buffer value, Transaction tx) throws IOException {
        Integer pageId = data.mapIndex.get(name);
        if (pageId == null) {
            pageId = tx.alloc();
            SortedIndex<AsciiBuffer, Buffer> map = mapInstanceIndexFactory.create(tx, pageId);
            data.mapIndex.put(key, pageId);
        }
        SortedIndex<AsciiBuffer, Buffer> map = mapInstanceIndexFactory.open(tx, pageId);
        map.put(key, value);
    }

    public final void mapRemoveEntry(AsciiBuffer name, AsciiBuffer key, Transaction tx) throws IOException, KeyNotFoundException {
        Integer pageId = data.mapIndex.get(name);
        if (pageId == null) {
            throw new KeyNotFoundException(name.toString());
        }
        SortedIndex<AsciiBuffer, Buffer> map = mapInstanceIndexFactory.open(tx, pageId);
        map.remove(key);
    }

    public final Buffer mapGetEntry(AsciiBuffer name, AsciiBuffer key, Transaction tx) throws IOException, KeyNotFoundException {
        Integer pageId = data.mapIndex.get(name);
        if (pageId == null) {
            throw new KeyNotFoundException(name.toString());
        }
        SortedIndex<AsciiBuffer, Buffer> map = mapInstanceIndexFactory.open(tx, pageId);
        return map.get(key);
    }

    public final Iterator<AsciiBuffer> mapList(AsciiBuffer first, int count, Transaction tx) {
        LinkedList<AsciiBuffer> results = new LinkedList<AsciiBuffer>();

        final Iterator<Entry<AsciiBuffer, Integer>> i = data.mapIndex.iterator(first);
        while (i.hasNext()) {
            final Entry<AsciiBuffer, Integer> entry = i.next();
            results.add(entry.getKey());
        }

        return results.iterator();
    }

    public final Iterator<AsciiBuffer> mapListKeys(AsciiBuffer name, AsciiBuffer first, int count, Transaction tx) throws IOException, KeyNotFoundException {
        Integer pageId = data.mapIndex.get(name);
        if (pageId == null) {
            throw new KeyNotFoundException(name.toString());
        }

        SortedIndex<AsciiBuffer, Buffer> map = mapInstanceIndexFactory.open(tx, pageId);
        final LinkedList<AsciiBuffer> results = new LinkedList<AsciiBuffer>();

        if (first != null && count > 0) {
            map.visit(new IndexVisitor.PredicateVisitor<AsciiBuffer, Buffer>(IndexVisitor.PredicateVisitor.gte(first), count){
                @Override
                protected void matched(AsciiBuffer key, Buffer value) {
                    results.add(key);
                }
            });
        } else {
            Iterator<Entry<AsciiBuffer, Buffer>> iterator = map.iterator();
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
    public int getState() {
        return data.state;
    }

    @Deprecated // TODO: keep data immutable
    public void setState(int state) {
        this.data.state = state;
    }

    public Location getLastUpdate() {
        return data.lastUpdate;
    }

    @Deprecated // TODO: keep data immutable
    public void setLastUpdate(Location lastUpdate) {
        this.data.lastUpdate = lastUpdate;
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

        //Scan MessageKey SortedIndex to find message keys past the last append 
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
        Location firstTxLocation = data.lastUpdate;

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
        data.locationIndex.visit(new IndexVisitor<Integer, Long>() {
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
