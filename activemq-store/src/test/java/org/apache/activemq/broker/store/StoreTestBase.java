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
package org.apache.activemq.broker.store;

import java.io.IOException;
import java.util.Iterator;

import junit.framework.TestCase;

import org.apache.activemq.broker.store.Store.FatalStoreException;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.broker.store.Store.QueueQueryResult;
import org.apache.activemq.broker.store.Store.QueueRecord;
import org.apache.activemq.broker.store.Store.Session;
import org.apache.activemq.broker.store.Store.VoidCallback;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.queue.QueueDescriptor;

public abstract class StoreTestBase extends TestCase {

    private Store store;

    abstract protected Store createStore(boolean delete);

    abstract protected boolean isStoreTransactional();

    abstract protected boolean isStorePersistent();

    @Override
    protected void setUp() throws Exception {
        store = createStore(true);
        store.start();
    }

    @Override
    protected void tearDown() throws Exception {
        if (store != null) {
            store.stop();
        }
    }

    public void testMessageAdd() throws Exception {
        final MessageRecord expected = new MessageRecord();
        expected.setBuffer(new Buffer("buffer"));
        expected.setEncoding(new AsciiBuffer("encoding"));
        expected.setMessageId(new AsciiBuffer("1000"));
        expected.setKey(store.allocateStoreTracking());
        expected.setSize(expected.getBuffer().getLength());
        
        store.execute(new VoidCallback<Exception>() {
            public void run(Session session) throws Exception {
                session.messageAdd(expected);
            }
        }, null);

        store.execute(new VoidCallback<Exception>() {
            @Override
            public void run(Session session) throws Exception {
                MessageRecord actual = session.messageGetRecord(expected.getKey());
                assertEquals(expected, actual);
            }
        }, null);
    }

    public void testQueueAdd() throws Exception {
        final QueueDescriptor expected = new QueueDescriptor();
        expected.setQueueName(new AsciiBuffer("testQueue"));
        expected.setApplicationType((short)1);
        
        store.execute(new VoidCallback<Exception>() {
            @Override
            public void run(Session session) throws Exception {
                session.queueAdd(expected);
            }
        }, null);

        //Test that the queue was created:
        checkQueue(expected, 0, 0);
        
        if(isStorePersistent())
        {
            //Restart the store and make sure the queue is still there
            store.stop();
            store = createStore(false);
            store.start();
            
            //Test that the queue was persisted
            checkQueue(expected, 0, 0);
        }
    }
    
    public void testQueueMessageAdd() throws Exception {
        final QueueDescriptor queue = new QueueDescriptor();
        queue.setQueueName(new AsciiBuffer("testQueue"));
        queue.setApplicationType((short)1);
        
        final MessageRecord message = new MessageRecord();
        message.setBuffer(new Buffer("buffer"));
        message.setEncoding(new AsciiBuffer("encoding"));
        message.setMessageId(new AsciiBuffer("1000"));
        message.setKey(store.allocateStoreTracking());
        message.setSize(message.getBuffer().getLength());
        
        final QueueRecord qRecord = new QueueRecord();
        qRecord.setMessageKey(message.getKey());
        qRecord.setQueueKey(1L);
        qRecord.setSize(message.getSize());
        
        store.execute(new VoidCallback<Exception>() {
            @Override
            public void run(Session session) throws Exception {
                session.queueAdd(queue);
                session.messageAdd(message);
                session.queueAddMessage(queue, qRecord);
            }
        }, null);

        checkQueue(queue, message.getSize(), 1);
        checkMessageRestore(queue, qRecord, message);
        
        //Restart the store and make sure the queue is still there
        if(isStorePersistent())
        {
            store.stop();
            store = createStore(false);
            store.start();
            
            //Test that the queue was persisted
            checkQueue(queue, message.getSize(), 1);
            checkMessageRestore(queue, qRecord, message);
        }
    }

    private void checkQueue(final QueueDescriptor queue, final long expectedSize, final long expectedCount) throws FatalStoreException, Exception
    {
        store.execute(new VoidCallback<Exception>() {
            @Override
            public void run(Session session) throws Exception {
                Iterator<QueueQueryResult> list = session.queueList(null, 100);
                assertTrue(list.hasNext());
                QueueQueryResult actual = list.next();
                assertEquals(queue, actual.getDescriptor());
                assertEquals(expectedSize, actual.getSize());
                assertEquals(expectedCount, actual.getCount());
            }
        }, null);
    }
    
    private void checkMessageRestore(final QueueDescriptor queue, final QueueRecord qRecord, final MessageRecord message ) throws FatalStoreException, Exception
    {
        store.execute(new VoidCallback<Exception>() {
            @Override
            public void run(Session session) throws Exception {
                Iterator<QueueRecord> qRecords = session.queueListMessagesQueue(queue, 0L, -1L, -1);
                assertTrue(qRecords.hasNext());
                QueueRecord qr = qRecords.next();
                assertEquals(qRecord.getQueueKey(), qr.getQueueKey());
                assertEquals(qRecord.getMessageKey(), message.getKey());
                MessageRecord record = session.messageGetRecord(qr.getMessageKey());
                assertEquals(record, message);
            }
        }, null);
    }
    
    public void testStoreExecuteExceptionPassthrough() throws Exception {
        try {
            store.execute(new VoidCallback<Exception>() {
                @Override
                public void run(Session session) throws Exception {
                    QueueDescriptor qd = new QueueDescriptor();
                    qd.setQueueName(new AsciiBuffer("test"));
                    session.queueAdd(qd);
                    throw new IOException("Expected");
                }
            }, null);
            fail("Expected IOException");
        } catch (IOException e) {
        }

        // If the store implementation is transactional, then the work done
        // should
        // have been rolled back.
        if (isStoreTransactional()) {
            store.execute(new VoidCallback<Exception>() {
                @Override
                public void run(Session session) throws Exception {
                    Iterator<QueueQueryResult> list = session.queueList(null, 100);
                    assertFalse(list.hasNext());
                }
            }, null);
        }
    }

    static void assertEquals(MessageRecord expected, MessageRecord actual) {
        assertEquals(expected.getBuffer(), actual.getBuffer());
        assertEquals(expected.getEncoding(), actual.getEncoding());
        assertEquals(expected.getMessageId(), actual.getMessageId());
        assertEquals(expected.getStreamKey(), actual.getStreamKey());
        assertEquals(expected.getSize(), actual.getSize());
    }
    
    static void assertEquals(QueueDescriptor expected, QueueDescriptor actual) {
        assertEquals(expected.getParent(), actual.getParent());
        assertEquals(expected.getQueueType(), actual.getQueueType());
        assertEquals(expected.getApplicationType(), actual.getApplicationType());
        assertEquals(expected.getPartitionKey(), actual.getPartitionKey());
        assertEquals(expected.getQueueName(), actual.getQueueName());
        //TODO test partitions?
        
    }

}
