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
package org.apache.activemq.broker.store.memory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.Store.DuplicateKeyException;
import org.apache.activemq.broker.store.Store.Session;
import org.apache.activemq.broker.store.Store.SubscriptionRecord;
import org.apache.activemq.queue.QueueDescriptor;
import org.apache.activemq.util.Comparators;
import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.util.buffer.ByteArrayOutputStream;

/**
 * An in memory implementation of the {@link Store} interface. It does not
 * properly roll back operations if an error occurs in the middle of a
 * transaction and it does not persist changes across restarts.
 */
public class MemoryStore implements Store {

    private MemorySession session = new MemorySession();
    private AtomicLong trackingGen = new AtomicLong(0);
    private ReentrantLock updateLock = new ReentrantLock();

    /**
     * @return a unique sequential store tracking number.
     */
    public long allocateStoreTracking() {
        return trackingGen.incrementAndGet();
    }

    public boolean isTransactional() {
        return false;
    }

    static private class Stream {

        private ByteArrayOutputStream baos = new ByteArrayOutputStream();
        private Buffer data;

        public void write(Buffer buffer) {
            if (baos == null) {
                throw new IllegalStateException("Stream closed.");
            }
            baos.write(buffer.data, buffer.offset, buffer.length);
        }

        public void close() {
            if (baos == null) {
                throw new IllegalStateException("Stream closed.");
            }
            data = baos.toBuffer();
            baos = null;
        }

        public Buffer read(int offset, int max) {
            if (data == null) {
                throw new IllegalStateException("Stream not closed.");
            }
            if (offset > data.length) {
                // Invalid offset.
                return new Buffer(data.data, 0, 0);
            }
            offset += data.offset;
            max = Math.min(max, data.length - offset);
            return new Buffer(data.data, offset, max);
        }

    }

    static private class StoredQueue {
        QueueDescriptor descriptor;

        TreeMap<Long, QueueRecord> records = new TreeMap<Long, QueueRecord>(Comparators.LONG_COMPARATOR);
        int count = 0;
        long size = 0;
        HashMap<QueueDescriptor, StoredQueue> partitions;
        StoredQueue parent;

        StoredQueue(QueueDescriptor descriptor) {
            this.descriptor = descriptor.copy();
        }

        public void add(QueueRecord record) {
            records.put(record.getQueueKey(), record);
            count++;
            size += record.getSize();
        }

        public long remove(Long queueKey) {
            QueueRecord record = records.remove(queueKey);
            if (record != null) {
                count--;
                size -= record.getSize();
                return record.getMessageKey();
            }
            return -1;
        }

        public Iterator<QueueRecord> list(Long firstQueueKey, long maxSequence, int max) {
            Collection<QueueRecord> list;
            if (max < 0) {
                list = new LinkedList<QueueRecord>();
            } else {
                list = new ArrayList<QueueRecord>(max);
            }

            for (Long key : records.tailMap(firstQueueKey).keySet()) {
                if ((max >= 0 && list.size() >= max) || (maxSequence >= 0 && key > maxSequence)) {
                    break;
                }
                list.add(records.get(key));
            }
            return list.iterator();
        }

        public int getCount() {
            return count;
        }

        public long getSize() {
            return size;
        }

        public void setParent(StoredQueue parent) {
            this.parent = parent;
            if (parent == null) {
                this.descriptor.setParent(null);
            } else {
                this.descriptor.setParent(parent.getQueueName());
            }
        }

        public StoredQueue getParent() {
            return parent;
        }

        public void addPartition(StoredQueue child) {
            if (partitions == null) {
                partitions = new HashMap<QueueDescriptor, StoredQueue>();
            }

            partitions.put(child.getDescriptor(), child);
        }

        public boolean removePartition(StoredQueue name) {
            if (partitions == null) {
                return false;
            }

            StoredQueue old = partitions.remove(name);
            if (old != null) {
                return true;
            }
            return false;
        }

        public Iterator<StoredQueue> getPartitions() {
            if (partitions == null) {
                return null;
            }

            return partitions.values().iterator();
        }

        public AsciiBuffer getQueueName() {
            return descriptor.getQueueName();
        }

        public QueueDescriptor getDescriptor() {
            return descriptor;
        }

        public QueueQueryResultImpl query() {
            QueueQueryResultImpl result = new QueueQueryResultImpl();
            result.count = count;
            result.size = size;
            result.firstSequence = records.isEmpty() ? 0 : records.get(records.firstKey()).getQueueKey();
            result.lastSequence = records.isEmpty() ? 0 : records.get(records.lastKey()).getQueueKey();
            result.desc = descriptor.copy();
            if (this.partitions != null) {
                ArrayList<QueueQueryResult> childResults = new ArrayList<QueueQueryResult>(partitions.size());
                for (StoredQueue child : partitions.values()) {
                    childResults.add(child.query());
                }
                result.partitions = childResults;
            }

            return result;
        }

    }

    /**
     * @return A new store Session.
     */
    public Session getSession() {
        return session;
    }

    static private class RemoveOp {
        QueueDescriptor queue;
        Long queueKey;

        public RemoveOp(QueueDescriptor queue, Long queueKey) {
            this.queue = queue;
            this.queueKey = queueKey;
        }
    }

    static private class Transaction {
        private ArrayList<Long> adds = new ArrayList<Long>(100);
        private ArrayList<RemoveOp> removes = new ArrayList<RemoveOp>(100);

        public void commit(MemorySession session) throws KeyNotFoundException {
            for (RemoveOp op : removes) {
                session.queueRemoveMessage(op.queue, op.queueKey);
            }
        }

        public void rollback(MemorySession session) {
            for (Long op : adds) {
                session.messageRemove(op);
            }
        }

        public void addMessage(Long messageKey) {
            adds.add(messageKey);
        }

        public void removeMessage(QueueDescriptor queue, Long queueKey) {
            removes.add(new RemoveOp(queue, queueKey));
        }
    }

    private static class MessageRecordHolder {
        final MessageRecord record;
        int refs = 0;

        public MessageRecordHolder(MessageRecord record) {
            this.record = record;
        }
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

    private class MemorySession implements Session {

        long streamSequence;

        private HashMap<Long, MessageRecordHolder> messages = new HashMap<Long, MessageRecordHolder>();

        private TreeMap<AsciiBuffer, TreeMap<AsciiBuffer, Buffer>> maps = new TreeMap<AsciiBuffer, TreeMap<AsciiBuffer, Buffer>>();
        private TreeMap<Long, Stream> streams = new TreeMap<Long, Stream>(Comparators.LONG_COMPARATOR);
        private TreeMap<AsciiBuffer, StoredQueue> queues = new TreeMap<AsciiBuffer, StoredQueue>();
        private TreeMap<Buffer, Transaction> transactions = new TreeMap<Buffer, Transaction>();

        private HashMap<AsciiBuffer, SubscriptionRecord> subscriptions = new HashMap<AsciiBuffer, SubscriptionRecord>();

        /**
         * Commits work done on the Session, if {@link Store#isTransactional()}
         * is true.
         */
        public void commit() {
            //NOOP
        }

        /**
         * Rolls back work done on the Session since the last call to
         * {@link #acquireLock()}
         */
        public void rollback() {
            throw new UnsupportedOperationException();
        }

        /**
         * Indicates callers intent to start a transaction. If the store is
         * transaction, the caller must call {@link #commit()} when the done
         * operating on the Session prior to a mandatory call to
         * {@link #releaseLock()}
         */
        public void acquireLock() {
            updateLock.lock();
        }

        /**
         * Indicates caller is done with the transaction, if not committed then
         * the transaction will be rolled back (providing the store is
         * transactional.
         */
        public void releaseLock() {
            updateLock.unlock();
        }

        // //////////////////////////////////////////////////////////////////////////////
        // Message related methods.
        // ///////////////////////////////////////////////////////////////////////////////
        public void messageAdd(MessageRecord record) {
            long key = record.getKey();
            if (key < 0) {
                throw new IllegalArgumentException("Key not set");
            }
            MessageRecordHolder holder = new MessageRecordHolder(record);
            MessageRecordHolder old = messages.put(key, holder);
            if (old != null) {
                messages.put(key, old);
            }
        }

        public void messageRemove(Long key) {
            messages.remove(key);
        }

        public MessageRecord messageGetRecord(Long key) {
            MessageRecordHolder holder = messages.get(key);
            if (holder != null) {
                return holder.record;
            }
            return null;
        }

        ////////////////////////////////////////////////////////////////
        //Client related methods
        ////////////////////////////////////////////////////////////////

        /**
         * Adds a subscription to the store.
         * 
         * @throws DuplicateKeyException
         *             if a subscription with the same name already exists
         * 
         */
        public void addSubscription(SubscriptionRecord record) throws DuplicateKeyException {
            SubscriptionRecord old = subscriptions.put(record.getName(), record);
            if (old != null && !old.equals(record)) {
                subscriptions.put(old.getName(), old);
                throw new DuplicateKeyException(record.getName() + " already exists!");
            }
        }

        /**
         * Updates a subscription in the store. If the subscription does not
         * exist then it will simply be added.
         */
        public void updateSubscription(SubscriptionRecord record) {
            subscriptions.put(record.getName(), record);
        }

        /**
         * Removes a subscription with the given name from the store.
         */
        public void removeSubscription(AsciiBuffer name) {
            subscriptions.remove(name);
        }

        /**
         * @return A list of subscriptions
         */
        public Iterator<SubscriptionRecord> listSubscriptions() {
            ArrayList<SubscriptionRecord> rc = new ArrayList<SubscriptionRecord>(subscriptions.size());
            rc.addAll(subscriptions.values());
            return rc.iterator();
        }

        // //////////////////////////////////////////////////////////////////////////////
        // Queue related methods.
        // ///////////////////////////////////////////////////////////////////////////////
        public void queueAdd(QueueDescriptor desc) throws KeyNotFoundException {
            StoredQueue queue = queues.get(desc.getQueueName());
            if (queue == null) {
                queue = new StoredQueue(desc);
                // Add to the parent:
                AsciiBuffer parent = desc.getParent();

                // If the parent doesn't exist create it:
                if (parent != null) {
                    StoredQueue parentQueue = queues.get(parent);
                    if (parentQueue == null) {
                        throw new KeyNotFoundException("No parent " + parent + " for " + desc.getQueueName().toString());
                    }

                    parentQueue.addPartition(queue);
                    queue.setParent(parentQueue);
                }

                // Add the queue:
                queues.put(desc.getQueueName(), queue);
            }
        }

        public void queueRemove(QueueDescriptor desc) {
            StoredQueue queue = queues.get(desc.getQueueName());
            if (queue != null) {
                // Remove message references:
                for (QueueRecord record : queue.records.values()) {
                    deleteMessageReference(record.getMessageKey());
                }

                // Remove parent reference:
                StoredQueue parent = queue.getParent();
                if (parent != null) {
                    parent.removePartition(queue);
                }

                // Delete partitions
                Iterator<StoredQueue> partitions = queue.getPartitions();
                if (partitions != null) {
                    while (partitions.hasNext()) {
                        QueueDescriptor child = partitions.next().getDescriptor();
                        queueRemove(child);
                    }
                }

                // Remove the queue:
                queues.remove(desc.getQueueName());
            }
        }

        // Queue related methods.
        public Iterator<QueueQueryResult> queueListByType(short type, QueueDescriptor firstQueue, int max) {
            return queueListInternal(firstQueue, type, max);
        }

        public Iterator<QueueQueryResult> queueList(QueueDescriptor firstQueue, int max) {
            return queueListInternal(firstQueue, (short) -1, max);
        }

        private Iterator<QueueQueryResult> queueListInternal(QueueDescriptor firstQueue, short type, int max) {
            Collection<StoredQueue> tailResults;
            LinkedList<QueueQueryResult> results = new LinkedList<QueueQueryResult>();
            if (firstQueue == null) {
                tailResults = queues.values();
            } else {
                tailResults = queues.tailMap(firstQueue.getQueueName()).values();
            }

            for (StoredQueue sq : tailResults) {
                if (max >= 0 && results.size() >= max) {
                    break;
                }
                if (type != -1 && sq.descriptor.getApplicationType() != type) {
                    continue;
                }
                results.add(sq.query());
            }

            return results.iterator();
        }

        public void queueAddMessage(QueueDescriptor queue, QueueRecord record) throws KeyNotFoundException {
            get(queues, queue.getQueueName()).add(record);
            MessageRecordHolder holder = messages.get(record.getMessageKey());
            if (holder != null) {
                holder.refs++;
            }
        }

        public void queueRemoveMessage(QueueDescriptor queue, Long queueKey) throws KeyNotFoundException {
            long msgKey = get(queues, queue.getQueueName()).remove(queueKey);
            if(msgKey >= 0)
            {   
                deleteMessageReference(msgKey);
            }
        }

        private void deleteMessageReference(Long msgKey) {
            MessageRecordHolder holder = messages.get(msgKey);
            if (holder != null) {
                holder.refs--;
                if (holder.refs <= 0) {
                    messages.remove(msgKey);
                }
            }
        }

        public Iterator<QueueRecord> queueListMessagesQueue(QueueDescriptor queue, Long firstQueueKey, Long maxQueueKey, int max) throws KeyNotFoundException {
            return get(queues, queue.getQueueName()).list(firstQueueKey, maxQueueKey, max);
        }

        // //////////////////////////////////////////////////////////////////////////////
        // Simple Key Value related methods could come in handy to store misc
        // data.
        // ///////////////////////////////////////////////////////////////////////////////
        public void mapAdd(AsciiBuffer mapName) {
            if (maps.containsKey(mapName)) {
                return;
            }
            maps.put(mapName, new TreeMap<AsciiBuffer, Buffer>());
        }

        public void mapRemove(AsciiBuffer mapName) {
            maps.remove(mapName);
        }

        public Iterator<AsciiBuffer> mapList(AsciiBuffer first, int max) {
            return list(maps, first, max);
        }

        public Buffer mapEntryGet(AsciiBuffer mapName, AsciiBuffer key) throws KeyNotFoundException {
            TreeMap<AsciiBuffer, Buffer> map = get(maps, mapName);
            return map.get(key);
        }

        public void mapEntryRemove(AsciiBuffer mapName, AsciiBuffer key) throws KeyNotFoundException {
            TreeMap<AsciiBuffer, Buffer> map = get(maps, mapName);
            map.remove(key);
        }

        public void mapEntryPut(AsciiBuffer mapName, AsciiBuffer key, Buffer value) {
            TreeMap<AsciiBuffer, Buffer> map = maps.get(mapName);
            if (map == null) {
                mapAdd(mapName);
                map = maps.get(mapName);
            }
            map.put(key, value);
        }

        public Iterator<AsciiBuffer> mapEntryListKeys(AsciiBuffer mapName, AsciiBuffer first, int max) throws KeyNotFoundException {
            return list(get(maps, mapName), first, max);
        }

        // ///////////////////////////////////////////////////////////////////////////////
        // Stream related methods
        // ///////////////////////////////////////////////////////////////////////////////
        public Long streamOpen() {
            Long id = ++streamSequence;
            streams.put(id, new Stream());
            return id;
        }

        public void streamWrite(Long streamKey, Buffer buffer) throws KeyNotFoundException {
            get(streams, streamKey).write(buffer);
        }

        public void streamClose(Long streamKey) throws KeyNotFoundException {
            get(streams, streamKey).close();
        }

        public Buffer streamRead(Long streamKey, int offset, int max) throws KeyNotFoundException {
            return get(streams, streamKey).read(offset, max);
        }

        public boolean streamRemove(Long streamKey) {
            return streams.remove(streamKey) != null;
        }

        // ///////////////////////////////////////////////////////////////////////////////
        // Transaction related methods
        // ///////////////////////////////////////////////////////////////////////////////
        public void transactionAdd(Buffer txid) {
            transactions.put(txid, new Transaction());
        }

        public void transactionCommit(Buffer txid) throws KeyNotFoundException {
            remove(transactions, txid).commit(this);
        }

        public void transactionRollback(Buffer txid) throws KeyNotFoundException {
            remove(transactions, txid).rollback(this);
        }

        public Iterator<Buffer> transactionList(Buffer first, int max) {
            return list(transactions, first, max);
        }

        public void transactionAddMessage(Buffer txid, Long messageKey) throws KeyNotFoundException {
            get(transactions, txid).addMessage(messageKey);
            MessageRecordHolder holder = messages.get(messageKey);
            if (holder != null) {
                holder.refs++;
            }
        }

        public void transactionRemoveMessage(Buffer txid, QueueDescriptor queue, Long messageKey) throws KeyNotFoundException {
            get(transactions, txid).removeMessage(queue, messageKey);
            MessageRecordHolder holder = messages.get(messageKey);
            if (holder != null) {
                holder.refs--;
                if (holder.refs <= 0) {
                    messages.remove(messageKey);
                }
            }
        }
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }

    public <R, T extends Exception> R execute(Callback<R, T> callback, Runnable runnable) throws T {
        R rc = callback.execute(session);
        if (runnable != null) {
            runnable.run();
        }
        return rc;
    }

    public void flush() {
    }

    static private <Key, Value> Iterator<Key> list(TreeMap<Key, Value> map, Key first, int max) {
        ArrayList<Key> rc = new ArrayList<Key>(max);
        Set<Key> keys = (first == null ? map : map.tailMap(first)).keySet();
        for (Key buffer : keys) {
            if (rc.size() >= max) {
                break;
            }
            rc.add(buffer);
        }
        return rc.iterator();
    }

    static private <Key, Value> Value get(TreeMap<Key, Value> map, Key key) throws KeyNotFoundException {
        Value value = map.get(key);
        if (value == null) {
            throw new KeyNotFoundException(key.toString());
        }
        return value;
    }

    static private <Key, Value> Value remove(TreeMap<Key, Value> map, Key key) throws KeyNotFoundException {
        Value value = map.remove(key);
        if (value == null) {
            throw new KeyNotFoundException(key.toString());
        }
        return value;
    }

    public void setStoreDirectory(File directory) {
        // NOOP
    }

    public File getStoreDirectory() {
        return null;
    }

    public void setDeleteAllMessages(boolean val) {
        // NOOP
    }

}
