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
import java.util.TreeMap;

import org.apache.activemq.broker.store.Store;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;


public class MemoryStore implements Store {

    MemorySession session = new MemorySession();

    private class MemorySession implements Session {
        
        long messageSequence;
        
        private HashMap<Long, MessageRecord> messages = new HashMap<Long, MessageRecord>();
        private HashMap<AsciiBuffer, Long> messagesKeys = new HashMap<AsciiBuffer, Long>();
        
        private class StoredQueue {
            long sequence;
            TreeMap<Long, QueueRecord> records = new TreeMap<Long, QueueRecord>();
        }
        
        private TreeMap<AsciiBuffer, StoredQueue> queues = new TreeMap<AsciiBuffer, StoredQueue>();


        // //////////////////////////////////////////////////////////////////////////////
        // Message related methods.
        // ///////////////////////////////////////////////////////////////////////////////
        public Long messageAdd(MessageRecord record) {
            Long key = ++messageSequence;
            record.setKey(key);
            messages.put(key, record);
            messagesKeys.put(record.getMessageId(), key);
            return key;
        }
 
        public Long messageGetKey(AsciiBuffer messageId) {
            return messagesKeys.get(messageId);
        }

        public MessageRecord messageGetRecord(Long key) {
            return messages.get(key);
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

        public Long queueAddMessage(AsciiBuffer queueName, QueueRecord record) throws QueueNotFoundException {
            StoredQueue queue = queues.get(queueName);
            if (queue != null) {
                Long key = ++queue.sequence;
                record.setQueueKey(key);
                queue.records.put(key, record);
                return key;
            } else {
                throw new QueueNotFoundException(queueName.toString());
            }
        }

        public void queueRemoveMessage(AsciiBuffer queueName, Long queueKey) throws QueueNotFoundException {
            StoredQueue queue = queues.get(queueName);
            if (queue == null) {
                throw new QueueNotFoundException(queueName.toString());
            }
            queue.records.remove(queueKey);
        }

        public Iterator<AsciiBuffer> queueList(AsciiBuffer firstQueueName, int max) {
            ArrayList<AsciiBuffer> list = new ArrayList<AsciiBuffer>(queues.size());
            for (AsciiBuffer queue : queues.tailMap(firstQueueName).keySet()) {
                if( list.size() >= max ) {
                    break;
                }
                list.add(queue);
            }
            return list.iterator();
        }

        public Iterator<QueueRecord> queueListMessagesQueue(AsciiBuffer queueName, Long firstQueueKey, int max) {
            ArrayList<QueueRecord> list = new ArrayList<QueueRecord>(max);
            StoredQueue queue = queues.get(queueName);
            if (queue != null) {
                for (Long key : queue.records.tailMap(firstQueueKey).keySet() ) {
                    if (list.size() >= max) {
                        break;
                    }
                    list.add(queue.records.get(key));
                }
            }
            return list.iterator();
        }

        public boolean queueRemove(AsciiBuffer queueName) {
            StoredQueue queue = queues.get(queueName);
            if (queue != null) {
                queue.records.clear();
                queues.remove(queueName);
                return true;
            }
            return false;
        }


        // //////////////////////////////////////////////////////////////////////////////
        // Simple Key Value related methods could come in handy to store misc
        // data.
        // ///////////////////////////////////////////////////////////////////////////////
        public Buffer mapGet(AsciiBuffer map, Buffer key) {
            throw new UnsupportedOperationException();
        }

        public Iterator<AsciiBuffer> mapList(AsciiBuffer first, int max) {
            throw new UnsupportedOperationException();
        }

        public Iterator<Buffer> mapListKeys(AsciiBuffer map, Buffer first, int max) {
            throw new UnsupportedOperationException();
        }

        public Buffer mapRemove(AsciiBuffer map, Buffer key) {
            throw new UnsupportedOperationException();
        }

        public Buffer mapSet(AsciiBuffer map, Buffer key, Buffer value) {
            throw new UnsupportedOperationException();
        }

        // ///////////////////////////////////////////////////////////////////////////////
        // Stream related methods
        // ///////////////////////////////////////////////////////////////////////////////
        public Long streamOpen() {
            throw new UnsupportedOperationException();
        }
        public void streamWrite(Long key, Buffer message) {
            throw new UnsupportedOperationException();
        }
        public void streamClose(Long key) {
            throw new UnsupportedOperationException();
        }
        public Buffer streamRead(Long key, int offset, int max) {
            throw new UnsupportedOperationException();
        }
        public boolean streamRemove(Long key) {
            throw new UnsupportedOperationException();
        }

        // ///////////////////////////////////////////////////////////////////////////////
        // Transaction related methods
        // ///////////////////////////////////////////////////////////////////////////////
        public Iterator<AsciiBuffer> transactionList(AsciiBuffer first, int max) {
            throw new UnsupportedOperationException();
        }
        public void transactionAdd(AsciiBuffer txid) {
            throw new UnsupportedOperationException();
        }
        public void transactionAddMessage(AsciiBuffer txid, Long messageKey) {
            throw new UnsupportedOperationException();
        }
        public void transactionRemoveMessage(AsciiBuffer txid, AsciiBuffer queue, Long messageKey) {
            throw new UnsupportedOperationException();
        }
        public boolean transactionCommit(AsciiBuffer txid) {
            throw new UnsupportedOperationException();
        }
        public boolean transactionRollback(AsciiBuffer txid) {
            throw new UnsupportedOperationException();
        }


    }

    public <R, T extends Exception> R execute(Callback<R, T> callback, Runnable runnable) throws T {
        R rc = callback.execute(session);
        if( runnable!=null ) {
            runnable.run();
        }
        return rc;
    }

    public void flush() {
    }
}
