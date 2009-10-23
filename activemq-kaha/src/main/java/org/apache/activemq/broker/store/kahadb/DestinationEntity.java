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

import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.Store.DuplicateKeyException;
import org.apache.activemq.broker.store.Store.QueueRecord;
import org.apache.activemq.broker.store.kahadb.Data.QueueAddMessage;
import org.apache.activemq.queue.QueueDescriptor;
import org.apache.activemq.util.marshaller.LongMarshaller;
import org.apache.activemq.util.marshaller.Marshaller;
import org.apache.activemq.util.marshaller.VariableMarshaller;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.Transaction;

public class DestinationEntity {

    public final static Marshaller<DestinationEntity> MARSHALLER = new VariableMarshaller<DestinationEntity>() {

        public DestinationEntity readPayload(DataInput dataIn) throws IOException {
            DestinationEntity value = new DestinationEntity();
            value.queueIndex = new BTreeIndex<Long, QueueRecord>(dataIn.readLong());
            value.trackingIndex = new BTreeIndex<Long, Long>(dataIn.readLong());
            value.descriptor = Marshallers.QUEUE_DESCRIPTOR_MARSHALLER.readPayload(dataIn);
            value.metaData = new Page<DestinationMetaData>(dataIn.readLong());
            return value;
        }

        public void writePayload(DestinationEntity value, DataOutput dataOut) throws IOException {
            dataOut.writeLong(value.queueIndex.getPageId());
            dataOut.writeLong(value.trackingIndex.getPageId());
            Marshallers.QUEUE_DESCRIPTOR_MARSHALLER.writePayload(value.descriptor, dataOut);
            dataOut.writeLong(value.metaData.getPageId());
        }

        public int estimatedSize(DestinationEntity object) {
            throw new UnsupportedOperationException();
        }

    };

    public final static Marshaller<DestinationMetaData> META_DATA_MARSHALLER = new VariableMarshaller<DestinationMetaData>() {
        public DestinationMetaData readPayload(DataInput dataIn) throws IOException {
            DestinationMetaData value = new DestinationMetaData();
            value.count = dataIn.readInt();
            value.size = dataIn.readLong();
            return value;
        }

        public void writePayload(DestinationMetaData value, DataOutput dataOut) throws IOException {
            dataOut.writeInt(value.count);
            dataOut.writeLong(value.size);
        }

        public int estimatedSize(DestinationMetaData object) {
            throw new UnsupportedOperationException();
        }
    };

    public Class<DestinationEntity> getType() {
        return DestinationEntity.class;
    }

    private BTreeIndex<Long, QueueRecord> queueIndex;
    private BTreeIndex<Long, Long> trackingIndex;

    // Descriptor for this queue:
    private QueueDescriptor descriptor;

    // Child Partitions:
    private HashSet<DestinationEntity> partitions;

    // Holds volatile queue meta data
    private Page<DestinationMetaData> metaData;

    // /////////////////////////////////////////////////////////////////
    // Lifecycle Methods.
    // /////////////////////////////////////////////////////////////////
    public void allocate(Transaction tx) throws IOException {
        queueIndex = new BTreeIndex<Long, QueueRecord>(tx.allocate());
        trackingIndex = new BTreeIndex<Long, Long>(tx.allocate());
        metaData = tx.allocate();
        metaData.set(new DestinationMetaData());
        tx.store(metaData, META_DATA_MARSHALLER, true);
    }

    public void deallocate(Transaction tx) throws IOException {
        queueIndex.clear(tx);
        trackingIndex.clear(tx);
        tx.free(trackingIndex.getPageId());
        tx.free(queueIndex.getPageId());
        tx.free(metaData.getPageId());
        queueIndex = null;
        trackingIndex = null;
        metaData = null;
    }

    public void load(Transaction tx) throws IOException {
        if (queueIndex.getPageFile() == null) {

            queueIndex.setPageFile(tx.getPageFile());
            queueIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
            queueIndex.setValueMarshaller(Marshallers.QUEUE_RECORD_MARSHALLER);
            queueIndex.load(tx);
        }

        if (trackingIndex.getPageFile() == null) {

            trackingIndex.setPageFile(tx.getPageFile());
            trackingIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
            trackingIndex.setValueMarshaller(LongMarshaller.INSTANCE);
            trackingIndex.load(tx);
        }

        tx.load(metaData, META_DATA_MARSHALLER);
    }

    private static final boolean unlimited(Number val) {
        return val == null || val.longValue() < 0;
    }

    private DestinationMetaData getMetaData(Transaction tx) throws IOException {
        tx.load(metaData, META_DATA_MARSHALLER);
        return metaData.get();
    }

    // /////////////////////////////////////////////////////////////////
    // Message Methods.
    // /////////////////////////////////////////////////////////////////
    
    public long getSize(Transaction tx) throws IOException {
        return getMetaData(tx).size;
    }

    public int getCount(Transaction tx) throws IOException {
        return getMetaData(tx).count;
    }

    public long getFirstSequence(Transaction tx) throws IOException {
        return getMetaData(tx).count == 0 ? 0 : queueIndex.getFirst(tx).getValue().getQueueKey();
    }

    public long getLastSequence(Transaction tx) throws IOException {
        return getMetaData(tx).count == 0 ? 0 : queueIndex.getLast(tx).getValue().getQueueKey();
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

        Long existing = trackingIndex.put(tx, command.getMessageKey(), command.getQueueKey());
        if (existing == null) {
            QueueRecord value = new QueueRecord();
            value.setAttachment(command.getAttachment());
            value.setMessageKey(command.getMessageKey());
            value.setQueueKey(command.getQueueKey());
            value.setSize(command.getMessageSize());

            QueueRecord rc = queueIndex.put(tx, value.getQueueKey(), value);
            if (rc == null) {
                // TODO It seems a little inefficient to continually serialize
                // the queue size. It might be better to update this only at
                // commit
                // timeespecially if we start doing multiple adds per
                // transaction.
                // It is also possible that we might want to remove this update
                // altogether in favor of scanning the whole queue at recovery
                // time (at the cost of startup time)
                getMetaData(tx).update(1, command.getMessageSize());
                tx.store(metaData, META_DATA_MARSHALLER, true);
            } else {
                throw new Store.FatalStoreException(new Store.DuplicateKeyException("Duplicate sequence number " + command.getQueueKey() + " for " + descriptor.getQueueName()));
            }
        } else {
            throw new Store.DuplicateKeyException("Duplicate tracking " + command.getMessageKey() + " for " + descriptor.getQueueName());
        }
    }

    /**
     * Removes a queue record returning the corresponding element tracking number.
     * @param tx The transaction under which to do the removal
     * @param queueKey The queue key
     * @return The store tracking. 
     * @throws IOException
     */
    public long remove(Transaction tx, long queueKey) throws IOException {
        QueueRecord qr = queueIndex.remove(tx, queueKey);
        if(qr != null)
        {
            trackingIndex.remove(tx, qr.getMessageKey());
            getMetaData(tx).update(-1, -qr.getSize());
            tx.store(metaData, META_DATA_MARSHALLER, true);
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
            iterator = queueIndex.iterator(tx);

        } else {
            iterator = queueIndex.iterator(tx, firstQueueKey);
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
        return trackingIndex.iterator(tx);
    }

    public static class DestinationMetaData {
        int count;
        long size;

        public void update(int count, long size) {
            this.count += count;
            this.size += size;
        }

        public void set(int count, long size) {
            this.count = count;
            this.size = size;
        }
    }
}