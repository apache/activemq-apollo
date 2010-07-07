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

import org.apache.activemq.apollo.store.QueueRecord;
import org.apache.activemq.apollo.store.QueueEntryRecord;
import org.apache.activemq.broker.store.hawtdb.Codecs;
import org.apache.activemq.broker.store.hawtdb.model.AddQueueEntry;
import org.fusesource.hawtbuf.codec.LongCodec;
import org.fusesource.hawtbuf.codec.Codec;
import org.fusesource.hawtbuf.codec.VariableCodec;
import org.fusesource.hawtdb.api.BTreeIndexFactory;
import org.fusesource.hawtdb.api.SortedIndex;
import org.fusesource.hawtdb.api.Transaction;

public class DestinationEntity {

    private static final BTreeIndexFactory<Long, QueueEntryRecord> queueIndexFactory = new BTreeIndexFactory<Long, QueueEntryRecord>();
    private static final BTreeIndexFactory<Long, Long> trackingIndexFactory = new BTreeIndexFactory<Long, Long>();
    private static final BTreeIndexFactory<Long, Long> statsIndexFactory = new BTreeIndexFactory<Long, Long>();

    static {
        queueIndexFactory.setKeyCodec(LongCodec.INSTANCE);
        queueIndexFactory.setValueCodec(Codecs.QUEUE_RECORD_CODEC);
        queueIndexFactory.setDeferredEncoding(true);

        trackingIndexFactory.setKeyCodec(LongCodec.INSTANCE);
        trackingIndexFactory.setValueCodec(LongCodec.INSTANCE);
        trackingIndexFactory.setDeferredEncoding(true);

        statsIndexFactory.setKeyCodec(LongCodec.INSTANCE);
        statsIndexFactory.setValueCodec(LongCodec.INSTANCE);
        statsIndexFactory.setDeferredEncoding(true);
    }

    public final static Codec<DestinationEntity> CODEC = new VariableCodec<DestinationEntity>() {

        public DestinationEntity decode(DataInput dataIn) throws IOException {
            DestinationEntity value = new DestinationEntity();
            value.queueIndex = dataIn.readInt();
            value.trackingIndex =  dataIn.readInt();
            value.record = Codecs.QUEUE_DESCRIPTOR_CODEC.decode(dataIn);
            return value;
        }

        public void encode(DestinationEntity value, DataOutput dataOut) throws IOException {
            dataOut.writeInt(value.queueIndex);
            dataOut.writeInt(value.trackingIndex);
            Codecs.QUEUE_DESCRIPTOR_CODEC.encode(value.record, dataOut);
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
    private QueueRecord record;

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

    private SortedIndex<Long, QueueEntryRecord> queueIndex(Transaction tx) {
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
        Entry<Long, QueueEntryRecord> entry = queueIndex(tx).getFirst();
        if( entry!=null ) {
            return entry.getValue().queueKey;
        } else {
            return 0;
        }
    }

    public long getLastSequence(Transaction tx) throws IOException {
        Entry<Long, QueueEntryRecord> entry = queueIndex(tx).getLast();
        if( entry!=null ) {
            return entry.getValue().queueKey;
        } else {
            return 0;
        }
    }

    public void setQueueDescriptor(QueueRecord queue) {
        record = queue;
    }

    public QueueRecord getQueueRecord() {
        return record;
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

    public void add(Transaction tx, AddQueueEntry.Getter command) throws IOException, DuplicateKeyException {

        Long existing = trackingIndex(tx).put(command.getMessageKey(), command.getQueueKey());
        if (existing == null) {
            QueueEntryRecord value = new QueueEntryRecord();
            value.attachment = command.getAttachment();
            value.messageKey = command.getMessageKey();
            value.queueKey = command.getQueueKey();
            value.size = command.getSize();

            QueueEntryRecord rc = queueIndex(tx).put(value.queueKey, value);
            if (rc == null) {
                // TODO It seems a little inefficient to continually serialize
                // the queue size. It might be better to update this only at
                // commit
                // timeespecially if we start doing multiple adds per
                // transaction.
                // It is also possible that we might want to remove this update
                // altogether in favor of scanning the whole queue at recovery
                // time (at the cost of startup time)
                addStats(tx, 1, command.getSize());
            } else {
                throw new FatalStoreException(new DuplicateKeyException("Duplicate sequence number " + command.getQueueKey() + " for " + record.name));
            }
        } else {
            throw new DuplicateKeyException("Duplicate tracking " + command.getMessageKey() + " for " + record.name);
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
        QueueEntryRecord qr = queueIndex(tx).remove(queueKey);
        if(qr != null)
        {
            trackingIndex(tx).remove(qr.messageKey);
            addStats(tx, -1, -qr.size);
            return qr.messageKey;
        }
        return -1;
    }

    public Iterator<QueueEntryRecord> listMessages(Transaction tx, Long firstQueueKey, Long maxQueueKey, final int max) throws IOException {
        Collection<QueueEntryRecord> rc;
        if (unlimited(max)) {
            rc = new LinkedList<QueueEntryRecord>();
        } else {
            rc = new ArrayList<QueueEntryRecord>(max);
        }
        
        Iterator<Entry<Long, QueueEntryRecord>> iterator;
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
            Map.Entry<Long, QueueEntryRecord> entry = iterator.next();
            if (sequenceLimited && entry.getValue().queueKey > maxQueueKey) {
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