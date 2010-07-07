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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.Store.DuplicateKeyException;
import org.apache.activemq.broker.store.Store.QueueRecord;
import org.apache.activemq.broker.store.kahadb.Data.QueueAddMessage;
import org.fusesource.hawtdb.api.BTreeIndexFactory;
import org.fusesource.hawtdb.api.SortedIndex;
import org.fusesource.hawtdb.api.Transaction;
import org.fusesource.hawtdb.util.marshaller.*;

public class DestinationEntity {

    private static final BTreeIndexFactory<Long, QueueRecord> queueIndexFactory = new BTreeIndexFactory<Long, QueueRecord>();
    private static final BTreeIndexFactory<Long, Long> trackingIndexFactory = new BTreeIndexFactory<Long, Long>();
    private static final BTreeIndexFactory<Long, Long> statsIndexFactory = new BTreeIndexFactory<Long, Long>();

    static {
        queueIndexFactory.setKeyMarshaller(LongMarshaller.INSTANCE);
        queueIndexFactory.setValueMarshaller(Marshallers.QUEUE_RECORD_MARSHALLER);
        queueIndexFactory.setDeferredEncoding(true);

        trackingIndexFactory.setKeyMarshaller(LongMarshaller.INSTANCE);
        trackingIndexFactory.setValueMarshaller(LongMarshaller.INSTANCE);
        trackingIndexFactory.setDeferredEncoding(true);

        statsIndexFactory.setKeyMarshaller(LongMarshaller.INSTANCE);
        statsIndexFactory.setValueMarshaller(LongMarshaller.INSTANCE);
        statsIndexFactory.setDeferredEncoding(true);
    }

    public final static Marshaller<DestinationEntity> MARSHALLER = new VariableMarshaller<DestinationEntity>() {

        public DestinationEntity readPayload(DataInput dataIn) throws IOException {
            DestinationEntity value = new DestinationEntity();
            value.queueIndex = dataIn.readInt();
            value.trackingIndex =  dataIn.readInt();
            value.descriptor = Marshallers.QUEUE_DESCRIPTOR_MARSHALLER.readPayload(dataIn);
            return value;
        }

        public void writePayload(DestinationEntity value, DataOutput dataOut) throws IOException {
            dataOut.writeInt(value.queueIndex);
            dataOut.writeInt(value.trackingIndex);
            Marshallers.QUEUE_DESCRIPTOR_MARSHALLER.writePayload(value.descriptor, dataOut);
        }

        public int estimatedSize(DestinationEntity object) {
            throw new UnsupportedOperationException();
        }

    };

    public Class<DestinationEntity> getType() {
        return DestinationEntity.class;
    }

    private int queueIndex;
    private int trackingIndex;
    private int statsIndex;

    // Descriptor for this queue:
    private QueueDescriptor descriptor;

    // Child Partitions:
    private HashSet<DestinationEntity> partitions;

    // /////////////////////////////////////////////////////////////////
    // Lifecycle Methods.
    // /////////////////////////////////////////////////////////////////
    public void allocate(Transaction tx) throws IOException {
        queueIndex = tx.alloc();
        queueIndexFactory.create(tx, queueIndex);

        trackingIndex = tx.alloc();
        trackingIndexFactory.create(tx, trackingIndex);

        statsIndex = tx.alloc();
        statsIndexFactory.create(tx, statsIndex);
        setStats(tx, 0,0);
    }

    public void deallocate(Transaction tx) throws IOException {
        queueIndex(tx).clear();
        tx.free(trackingIndex);

        trackingIndex(tx).clear();
        tx.free(queueIndex);

        statsIndex(tx).clear();
        tx.free(statsIndex);
    }

    private SortedIndex<Long, QueueRecord> queueIndex(Transaction tx) {
        return queueIndexFactory.open(tx, queueIndex);
    }
    private SortedIndex<Long, Long> trackingIndex(Transaction tx) {
        return trackingIndexFactory.open(tx, trackingIndex);
    }
    private SortedIndex<Long, Long> statsIndex(Transaction tx) {
        return statsIndexFactory.open(tx, statsIndex);
    }

    private static final boolean unlimited(Number val) {
        return val == null || val.longValue() < 0;
    }

    // /////////////////////////////////////////////////////////////////
    // Message Methods.
    // /////////////////////////////////////////////////////////////////

    public static final Long SIZE_STAT = 0L;
    public static final Long COUNT_STAT = 0L;

    public long getSize(Transaction tx) throws IOException {
        return statsIndex(tx).get(SIZE_STAT);
    }

    public int getCount(Transaction tx) throws IOException {
        return (int)(long)statsIndex(tx).get(COUNT_STAT);
    }

    public long getFirstSequence(Transaction tx) throws IOException {
        Entry<Long, QueueRecord> entry = queueIndex(tx).getFirst();
        if( entry!=null ) {
            return entry.getValue().getQueueKey();
        } else {
            return 0;
        }
    }

    public long getLastSequence(Transaction tx) throws IOException {
        Entry<Long, QueueRecord> entry = queueIndex(tx).getLast();
        if( entry!=null ) {
            return entry.getValue().getQueueKey();
        } else {
            return 0;
        }
    }

    public void setQueueDescriptor(QueueDescriptor queue) {
        descriptor = queue;
    }

    public QueueDescriptor getDescriptor() {
        return descriptor;
    }

    public void addPartition(DestinationEntity destination) {
        if (partitions == null) {
            partitions = new HashSet<DestinationEntity>();
        }

        partitions.add(destination);
    }

    public void removePartition(DestinationEntity queue) {
        if (partitions == null) {
            return;
        }

        partitions.remove(queue);
        if (partitions.isEmpty()) {
            partitions = null;
        }
    }

    public Iterator<DestinationEntity> getPartitions() {
        if (partitions == null) {
            return null;
        } else {
            return partitions.iterator();
        }
    }

    public void add(Transaction tx, QueueAddMessage command) throws IOException, DuplicateKeyException {

        Long existing = trackingIndex(tx).put(command.getMessageKey(), command.getQueueKey());
        if (existing == null) {
            QueueRecord value = new QueueRecord();
            value.setAttachment(command.getAttachment());
            value.setMessageKey(command.getMessageKey());
            value.setQueueKey(command.getQueueKey());
            value.setSize(command.getMessageSize());

            QueueRecord rc = queueIndex(tx).put(value.getQueueKey(), value);
            if (rc == null) {
                // TODO It seems a little inefficient to continually serialize
                // the queue size. It might be better to update this only at
                // commit
                // timeespecially if we start doing multiple adds per
                // transaction.
                // It is also possible that we might want to remove this update
                // altogether in favor of scanning the whole queue at recovery
                // time (at the cost of startup time)
                addStats(tx, 1, command.getMessageSize());
            } else {
                throw new Store.FatalStoreException(new Store.DuplicateKeyException("Duplicate sequence number " + command.getQueueKey() + " for " + descriptor.getQueueName()));
            }
        } else {
            throw new Store.DuplicateKeyException("Duplicate tracking " + command.getMessageKey() + " for " + descriptor.getQueueName());
        }
    }

    private void addStats(Transaction tx, int count, int size) {
        SortedIndex<Long, Long> index = statsIndex(tx);
        index.put(COUNT_STAT, index.get(COUNT_STAT)+count);
        index.put(SIZE_STAT, index.get(SIZE_STAT)+size);
    }
    
    private void setStats(Transaction tx, int count, int size) {
        SortedIndex<Long, Long> index = statsIndex(tx);
        index.put(COUNT_STAT, new Long(count));
        index.put(SIZE_STAT, new Long(size));
    }

    /**
     * Removes a queue record returning the corresponding element tracking number.
     * @param tx The transaction under which to do the removal
     * @param queueKey The queue key
     * @return The store tracking. 
     * @throws IOException
     */
    public long remove(Transaction tx, long queueKey) throws IOException {
        QueueRecord qr = queueIndex(tx).remove(queueKey);
        if(qr != null)
        {
            trackingIndex(tx).remove(qr.getMessageKey());
            addStats(tx, -1, -qr.getSize());
            return qr.getMessageKey();
        }
        return -1;
    }

    public Iterator<QueueRecord> listMessages(Transaction tx, Long firstQueueKey, Long maxQueueKey, final int max) throws IOException {
        Collection<QueueRecord> rc;
        if (unlimited(max)) {
            rc = new LinkedList<QueueRecord>();
        } else {
            rc = new ArrayList<QueueRecord>(max);
        }
        
        Iterator<Entry<Long, QueueRecord>> iterator;
        if (unlimited(firstQueueKey)) {
            iterator = queueIndex(tx).iterator();

        } else {
            iterator = queueIndex(tx).iterator(firstQueueKey);
        }
        boolean sequenceLimited = !unlimited(maxQueueKey);
        boolean countLimited = !unlimited(max);
        while (iterator.hasNext()) {
            if (countLimited && rc.size() >= max) {
                break;
            }
            Map.Entry<Long, QueueRecord> entry = iterator.next();
            if (sequenceLimited && entry.getValue().getQueueKey() > maxQueueKey) {
                break;
            }
            rc.add(entry.getValue());
        }

        return rc.iterator();
    }

    public Iterator<Entry<Long, Long>> listTrackingNums(Transaction tx) throws IOException {
        return trackingIndex(tx).iterator();
    }


}