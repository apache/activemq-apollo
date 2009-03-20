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
        private HashMap<RecordKey, Buffer> messages = new HashMap<RecordKey, Buffer>();
        private HashMap<AsciiBuffer, TreeMap<RecordKey, Buffer>> queues = new HashMap<AsciiBuffer, TreeMap<RecordKey, Buffer>>();

        // private HashMap<String, LinkedList<RecordKey>> queues = new
        // HashMap<String, LinkedList<RecordKey>>();

        public void beginTx() {

        }

        public void commitTx() {

        }

        public void rollback() {
            throw new UnsupportedOperationException();
        }

        // //////////////////////////////////////////////////////////////////////////////
        // Message related methods.
        // ///////////////////////////////////////////////////////////////////////////////
        public RecordKey messageAdd(AsciiBuffer messageId, Buffer message) {
            RecordKey key = new MemoryRecordKey(messageId);
            messages.put(key, message);
            return key;
        }

        public Buffer messageGet(RecordKey key) {
            return messages.get(key);
        }
 
        public RecordKey messageGetKey(AsciiBuffer messageId) {
            MemoryRecordKey key = new MemoryRecordKey(messageId);
            return messages.containsKey(key) ? key : null;
        }

        // //////////////////////////////////////////////////////////////////////////////
        // Queue related methods.
        // ///////////////////////////////////////////////////////////////////////////////
        public void queueAdd(AsciiBuffer queue) {
            TreeMap<RecordKey, Buffer> messages = queues.get(queue);
            if (messages == null) {
                messages = new TreeMap<RecordKey, Buffer>();
                queues.put(queue, messages);
            }
        }

        public void queueAddMessage(AsciiBuffer queue, RecordKey key, Buffer attachment) throws QueueNotFoundException, DuplicateKeyException {
            TreeMap<RecordKey, Buffer> messages = queues.get(queue);
            if (messages != null) {
                if (messages.put(key, attachment) != null) {
                    throw new DuplicateKeyException("");
                }
            } else {
                throw new QueueNotFoundException(queue.toString());
            }
        }

        public void queueRemoveMessage(AsciiBuffer queue, RecordKey key) throws QueueNotFoundException {
            TreeMap<RecordKey, Buffer> messages = queues.get(queue);
            if (messages != null) {
                messages.remove(key);
            } else {
                throw new QueueNotFoundException(queue.toString());
            }
        }

        public Iterator<AsciiBuffer> queueList(AsciiBuffer first) {
            ArrayList<AsciiBuffer> list = new ArrayList<AsciiBuffer>(queues.size());
            for (AsciiBuffer queue : queues.keySet()) {
                list.add(queue);
            }
            return list.iterator();
        }

        public Iterator<Buffer> queueListMessagesQueue(AsciiBuffer queue, RecordKey firstRecord, int max) {
            ArrayList<Buffer> list = new ArrayList<Buffer>(max);
            TreeMap<RecordKey, Buffer> messages = queues.get(queue.toString());
            if (messages != null) {
                for (RecordKey key : messages.tailMap(firstRecord).keySet() ) {
                    list.add(messages.get(key));
                    if (list.size() == max) {
                        break;
                    }
                }
            }
            return list.iterator();
        }

        public boolean queueRemove(AsciiBuffer queue) {
            TreeMap<RecordKey, Buffer> messages = queues.get(queue.toString());
            if (messages != null) {
                Iterator<RecordKey> msgKeys = messages.keySet().iterator();
                while (msgKeys.hasNext()) {
                    RecordKey msgKey = msgKeys.next();
                    try {
                        queueRemoveMessage(queue, msgKey);
                    } catch (QueueNotFoundException e) {
                        // Can't happen.
                    }
                }
                queues.remove(queue.toString());

                return true;
            }
            return false;
        }

        /*
         * public void queueUpdateMessageAttachment(AsciiBuffer queue, RecordKey
         * key, Buffer attachment) { // TODO Auto-generated method stub }
         * 
         * public Buffer queueGetMessageAttachment(AsciiBuffer queue, RecordKey
         * key) throws QueueNotFoundException { TreeMap<RecordKey, Buffer>
         * messages = queues.get(queue); if (messages != null) {
         * messages.add(key); } else { throw new
         * QueueNotFoundException(queue.toString()); } }
         */

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
        // Message Chunking related methods
        // ///////////////////////////////////////////////////////////////////////////////
        public void messageChunkAdd(RecordKey key, Buffer message) {
            throw new UnsupportedOperationException();
        }

        public void messageChunkClose(RecordKey key) {
            throw new UnsupportedOperationException();
        }

        public Buffer messageChunkGet(RecordKey key, int offset, int max) {
            throw new UnsupportedOperationException();
        }

        public RecordKey messageChunkOpen(AsciiBuffer messageId, Buffer txid, Buffer message) {
            throw new UnsupportedOperationException();
        }

    }

    final private class MemoryRecordKey implements RecordKey {
        final AsciiBuffer messageId;

        MemoryRecordKey(AsciiBuffer messageId) {
            this.messageId = messageId;
        }
        
        @Override
        public int hashCode() {
            return messageId.hashCode();
        }
        
        @Override
        public boolean equals(Object obj) {
            if( obj == null || obj.getClass()!=MemoryRecordKey.class )
                return false;
            if( this == obj )
                return true;
            MemoryRecordKey key = (MemoryRecordKey)obj;
            return messageId.equals(key.messageId);
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
