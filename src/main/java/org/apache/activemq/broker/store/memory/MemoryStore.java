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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import java.io.File;

import org.apache.activemq.broker.store.Store;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;


/**
 * An in memory implementation of the {@link Store} interface. It does not
 * properly roll back operations if an error occurs in the middle of a
 * transaction and it does not persist changes across restarts.
 */
public class MemoryStore implements Store {

    private MemorySession session = new MemorySession();
    private AtomicLong trackingGen = new AtomicLong(0);

    /**
     * @return a unique sequential store tracking number.
     */
    public long allocateStoreTracking() {
        return trackingGen.incrementAndGet();
    }

    static private class Stream {

        private ByteArrayOutputStream baos = new ByteArrayOutputStream();
        private ByteSequence data;

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
            data = baos.toByteSequence();
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
        long sequence;
        TreeMap<Long, QueueRecord> records = new TreeMap<Long, QueueRecord>();
        // Maps tracking to sequence number:
        HashMap<Long, Long> trackingMap = new HashMap<Long, Long>();

        public Long add(QueueRecord record) {
            long sequenceKey = ++sequence;
            record.setQueueKey(sequenceKey);
            records.put(sequenceKey, record);
            trackingMap.put(record.getMessageKey(), record.getQueueKey());
            return sequenceKey;
        }

        public boolean remove(Long msgKey) {
            Long sequenceKey = trackingMap.remove(msgKey);
            if (sequenceKey != null) {
                records.remove(sequenceKey);
                return true;
            }
            return false;
        }

        public Iterator<QueueRecord> list(Long firstQueueKey, int max) {
            ArrayList<QueueRecord> list = new ArrayList<QueueRecord>(max);
            for (Long key : records.tailMap(firstQueueKey).keySet()) {
                if (list.size() >= max) {
                    break;
                }
                list.add(records.get(key));
            }
            return list.iterator();
        }
    }

    static private class RemoveOp {
        AsciiBuffer queue;
        Long messageKey;

        public RemoveOp(AsciiBuffer queue, Long messageKey) {
            this.queue = queue;
            this.messageKey = messageKey;
        }
    }

    static private class Transaction {
        private ArrayList<Long> adds = new ArrayList<Long>(100);
        private ArrayList<RemoveOp> removes = new ArrayList<RemoveOp>(100);

        public void commit(MemorySession session) throws KeyNotFoundException {
            for (RemoveOp op : removes) {
                session.queueRemoveMessage(op.queue, op.messageKey);
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

        public void removeMessage(AsciiBuffer queue, Long messageKey) {
            removes.add(new RemoveOp(queue, messageKey));
        }
    }

    private static class MessageRecordHolder {
        final MessageRecord record;
        int refs = 0;

        public MessageRecordHolder(MessageRecord record) {
            this.record = record;
        }
    }

    private class MemorySession implements Session {

        long streamSequence;

        private HashMap<Long, MessageRecordHolder> messages = new HashMap<Long, MessageRecordHolder>();

        private TreeMap<AsciiBuffer, TreeMap<AsciiBuffer, Buffer>> maps = new TreeMap<AsciiBuffer, TreeMap<AsciiBuffer, Buffer>>();
        private TreeMap<Long, Stream> streams = new TreeMap<Long, Stream>();
        private TreeMap<AsciiBuffer, StoredQueue> queues = new TreeMap<AsciiBuffer, StoredQueue>();
        private TreeMap<Buffer, Transaction> transactions = new TreeMap<Buffer, Transaction>();

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

        // //////////////////////////////////////////////////////////////////////////////
        // Queue related methods.
        // ///////////////////////////////////////////////////////////////////////////////
        public void queueAdd(AsciiBuffer queueName) {
            StoredQueue queue = queues.get(queueName);
            if (queue == null) {
                queue = new StoredQueue();
                queues.put(queueName, queue);
            }
        }

        public void queueRemove(AsciiBuffer queueName) {
            StoredQueue queue = queues.get(queueName);
            if (queue != null) {
                queues.remove(queueName);
            }
        }

        public Iterator<AsciiBuffer> queueList(AsciiBuffer firstQueueName, int max) {
            return list(queues, firstQueueName, max);
        }

        public Long queueAddMessage(AsciiBuffer queueName, QueueRecord record) throws KeyNotFoundException {
            Long sequenceKey = get(queues, queueName).add(record);
            MessageRecordHolder holder = messages.get(record.getMessageKey());
            if (holder != null) {
                holder.refs++;
            }
            return sequenceKey;

        }

        public void queueRemoveMessage(AsciiBuffer queueName, Long msgKey) throws KeyNotFoundException {
            if (get(queues, queueName).remove(msgKey)) {
                MessageRecordHolder holder = messages.get(msgKey);
                if (holder != null) {
                    holder.refs--;
                    if (holder.refs <= 0) {
                        messages.remove(msgKey);
                    }
                }
            }
        }

        public Iterator<QueueRecord> queueListMessagesQueue(AsciiBuffer queueName, Long firstQueueKey, int max) throws KeyNotFoundException {
            return get(queues, queueName).list(firstQueueKey, max);
        }

        // //////////////////////////////////////////////////////////////////////////////
        // Simple Key Value related methods could come in handy to store misc
        // data.
        // ///////////////////////////////////////////////////////////////////////////////
        public boolean mapAdd(AsciiBuffer mapName) {
            if (maps.containsKey(mapName)) {
                return false;
            }
            maps.put(mapName, new TreeMap<AsciiBuffer, Buffer>());
            return true;
        }

        public boolean mapRemove(AsciiBuffer mapName) {
            return maps.remove(mapName) != null;
        }

        public Iterator<AsciiBuffer> mapList(AsciiBuffer first, int max) {
            return list(maps, first, max);
        }

        public Buffer mapEntryGet(AsciiBuffer mapName, AsciiBuffer key) throws KeyNotFoundException {
            return get(maps, mapName).get(key);
        }

        public Buffer mapEntryRemove(AsciiBuffer mapName, AsciiBuffer key) throws KeyNotFoundException {
            return get(maps, mapName).remove(key);
        }

        public Buffer mapEntryPut(AsciiBuffer mapName, AsciiBuffer key, Buffer value) throws KeyNotFoundException {
            return get(maps, mapName).put(key, value);
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

        public void transactionRemoveMessage(Buffer txid, AsciiBuffer queue, Long messageKey) throws KeyNotFoundException {
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

    public void setDeleteAllMessages(boolean val) {
        // TODO Auto-generated method stub
        
    }

}
