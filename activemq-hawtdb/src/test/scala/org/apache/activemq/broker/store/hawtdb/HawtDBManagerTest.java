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
package org.apache.activemq.broker.store.hawtdb;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.TreeMap;

import junit.framework.TestCase;

import org.apache.activemq.apollo.store.*;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;
import org.junit.Assert;

public class HawtDBManagerTest extends TestCase {

//    private HawtDBManager store;
//
//    protected HawtDBManager createStore(boolean delete) {
//        HawtDBManager rc = new HawtDBManager();
//        rc.setStoreDirectory(new File("target/test-data/kahadb-store-test"));
//        rc.setDeleteAllMessages(delete);
//        return rc;
//    }
//
//    protected boolean isStoreTransactional() {
//        return true;
//    }
//
//    protected boolean isStorePersistent() {
//        return true;
//    }
//
//    @Override
//    protected void setUp() throws Exception {
//        store = createStore(true);
//        store.start();
//    }
//
//    @Override
//    protected void tearDown() throws Exception {
//        if (store != null) {
//            store.stop();
//        }
//    }
//
//    public void testMessageAdd() throws Exception {
//        final MessageRecord expected = new MessageRecord();
//        expected.value = new AsciiBuffer("buffer").buffer();
//        expected.protocol = new AsciiBuffer("encoding");
//        expected.key = store.allocateStoreTracking();
//        expected.size = expected.value.getLength();
//
//        store.execute(new VoidCallback<Exception>() {
//            public void run(HawtDBSession session) throws Exception {
//                session.messageAdd(expected);
//            }
//        }, null);
//
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                MessageRecord actual = session.messageGetRecord(expected.key);
//                assertEquals(expected, actual);
//            }
//        }, null);
//    }
//
//    public void testQueueAdd() throws Exception {
//        final QueueRecord expected = new QueueRecord();
//        expected.name = new AsciiBuffer("testQueue");
//        expected.queueType = new AsciiBuffer("testType");
//
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                session.queueAdd(expected);
//            }
//        }, null);
//
//        //Test that the queue was created:
//        checkQueue(expected, 0, 0);
//
//        if (isStorePersistent()) {
//            //Restart the store and make sure the queue is still there
//            store.stop();
//            store = createStore(false);
//            store.start();
//
//            //Test that the queue was persisted
//            checkQueue(expected, 0, 0);
//        }
//    }
//
//    public void testQueueMessageAdd() throws Exception {
//        final QueueRecord queue = new QueueRecord();
//        queue.name = new AsciiBuffer("testQueue");
//        queue.queueType = new AsciiBuffer("testType");
//
//        final MessageRecord message = new MessageRecord();
//        message.value = new AsciiBuffer("buffer").buffer();
//        message.protocol = new AsciiBuffer("encoding");
//        message.key = store.allocateStoreTracking();
//        message.size = message.value.getLength();
//
//        final QueueEntryRecord qEntryRecord = new QueueEntryRecord();
//        qEntryRecord.messageKey = message.key;
//        qEntryRecord.queueKey = 1L;
//        qEntryRecord.size = message.size;
//
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                session.queueAdd(queue);
//                session.messageAdd(message);
//                session.queueAddMessage(queue, qEntryRecord);
//            }
//        }, null);
//
//        checkQueue(queue, message.size, 1);
//        checkMessageRestore(queue, qEntryRecord, message);
//
//        //Restart the store and make sure the queue is still there
//        if (isStorePersistent()) {
//            store.stop();
//            store = createStore(false);
//            store.start();
//
//            //Test that the queue was persisted
//            checkQueue(queue, message.size, 1);
//            checkMessageRestore(queue, qEntryRecord, message);
//        }
//    }
//
//    public void testSubscriptions() throws Exception {
//        HashMap<AsciiBuffer, SubscriptionRecord> expected = new HashMap<AsciiBuffer, SubscriptionRecord>();
//
//        final SubscriptionRecord record1 = new SubscriptionRecord();
//        record1.name = new AsciiBuffer("sub1");
//        record1.isDurable = true;
//        record1.destination = new AsciiBuffer("topic1");
//        expected.put(record1.name, record1);
//
//        final SubscriptionRecord record2 = new SubscriptionRecord();
//        record2.name = new AsciiBuffer("sub2");
//        record2.isDurable = false;
//        record2.destination = new AsciiBuffer("topic2");
//        record2.expiration = System.currentTimeMillis() + 40000;
//        record2.selector = new AsciiBuffer("foo");
//        byte[] attachment2 = new byte[1024];
//        for (int i = 0; i < attachment2.length; i++) {
//            attachment2[i] = (byte) i;
//        }
//        record2.attachment = new Buffer(attachment2);
//        expected.put(record2.name, record2);
//
//        //They make it?
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                session.addSubscription(record1);
//                session.addSubscription(record2);
//            }
//        }, null);
//
//        checkSubscriptions(expected);
//
//        //Let's remove one:
//        expected.remove(record1.name);
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                session.removeSubscription(record1.name);
//            }
//        }, null);
//
//        checkSubscriptions(expected);
//
//        //Restart the store and make sure the queue is still there
//        if (isStorePersistent()) {
//            store.stop();
//            store = createStore(false);
//            store.start();
//
//            //Test that the queue was persisted
//            checkSubscriptions(expected);
//        }
//    }
//
//
//    public void testMap() throws Exception {
//        final TreeMap<AsciiBuffer, Buffer> expected = new TreeMap<AsciiBuffer, Buffer>();
//        final AsciiBuffer map = new AsciiBuffer("testMap");
//
//        for(int i=0; i < 100000; i++)
//        {
//            expected.put(new AsciiBuffer("Key" + i), new AsciiBuffer("Value" + i));
//        }
//
//        //Test no values present:
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                Iterator<AsciiBuffer> r = session.mapList(null, 10);
//                Assert.assertEquals("Not expecting any maps", false, r.hasNext());
//            }
//        }, null);
//
//        //Test auto add:
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                session.mapEntryPut(new AsciiBuffer("testMap"), expected.firstKey(), expected.get(expected.firstKey()));
//                assertEquals("Value should be in map", session.mapEntryGet(map, expected.firstKey()), expected.get(expected.firstKey()));
//            }
//        }, null);
//
//        //Test re-add non empty map
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                session.mapAdd(map);
//                assertEquals("Value should be in map", session.mapEntryGet(map, expected.firstKey()), expected.get(expected.firstKey()));
//            }
//        }, null);
//
//        //Test overwrite
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                AsciiBuffer overwrite = new AsciiBuffer("overwrite");
//                session.mapEntryPut(map, expected.firstKey(), overwrite);
//                assertEquals("Value should be in map", session.mapEntryGet(map, expected.firstKey()), overwrite);
//            }
//        }, null);
//
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                assertEquals("Value should be in map", session.mapEntryGet(map, expected.firstKey()), new AsciiBuffer("overwrite"));
//            }
//        }, null);
//
//        //Test map remove:
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
////                AsciiBuffer overwrite = new AsciiBuffer("overwrite");
//                session.mapRemove(map);
//                Iterator<AsciiBuffer> r = session.mapList(null, 10);
//                Assert.assertEquals("Not expecting any maps", false, r.hasNext());
//            }
//        }, null);
//
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                Iterator<AsciiBuffer> r = session.mapList(null, 10);
//                Assert.assertEquals("Not expecting any maps", false, r.hasNext());
//            }
//        }, null);
//
//
//        //Test multiple adds:
//        System.out.println(new Date() + " Adding entries");
//
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                for(AsciiBuffer k : expected.keySet())
//                {
//                    session.mapEntryPut(map, k, expected.get(k));
//                }
//            }
//        }, null);
//
//        System.out.println(new Date() + " Checking entries");
//
//        checkMap(map, expected);
//
//        //Restart the store and make sure the entries are still there
//        if (isStorePersistent()) {
//            store.stop();
//
//            System.out.println(new Date() + " Restarting store");
//            store = createStore(false);
//            store.start();
//            System.out.println(new Date() + " Started store");
//
//
//            //Test that the queue was persisted
//            checkMap(map, expected);
//        }
//    }
//
//    private void checkMap(final AsciiBuffer mapName, final TreeMap<AsciiBuffer, Buffer> expected) throws Exception
//    {
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                Iterator<AsciiBuffer> r = session.mapEntryListKeys(mapName, expected.firstKey(), expected.size());
//                TreeMap<AsciiBuffer, Buffer> comp = new TreeMap<AsciiBuffer, Buffer>();
//                while(r.hasNext())
//                {
//                    AsciiBuffer key = r.next();
//                    Buffer value = session.mapEntryGet(mapName, key);
//                    comp.put(key, new AsciiBuffer(value.data));
//                }
//
//                Assert.assertEquals("Map in store doesn't match", expected, comp);
//
//            }
//        }, null);
//    }
//
//    @SuppressWarnings("unchecked")
//    private void checkSubscriptions(HashMap<AsciiBuffer, SubscriptionRecord> expected) throws Exception
//    {
//        final HashMap<AsciiBuffer, SubscriptionRecord> checkMap = (HashMap<AsciiBuffer, SubscriptionRecord>) expected.clone();
//
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                Iterator<SubscriptionRecord> results = session.listSubscriptions();
//                while(results.hasNext())
//                {
//                    SubscriptionRecord r = results.next();
//                    SubscriptionRecord e = checkMap.remove(r.name);
//                    Assert.assertEquals(r, e);
//                }
//
//                //Shouldn't be any expected results left:
//                Assert.assertEquals(0, checkMap.size());
//            }
//        }, null);
//    }
//
//    private void checkQueue(final QueueRecord queue, final long expectedSize, final long expectedCount) throws FatalStoreException, Exception {
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                Iterator<QueueStatus> list = session.queueList(null, 100);
//                Assert.assertTrue(list.hasNext());
//                QueueStatus actual = list.next();
//                assertEquals(queue, actual.record);
//                assertEquals(expectedSize, actual.size);
//                assertEquals(expectedCount, actual.count);
//            }
//        }, null);
//    }
//
//    private void checkMessageRestore(final QueueRecord queue, final QueueEntryRecord qEntryRecord, final MessageRecord message) throws FatalStoreException, Exception {
//        store.execute(new VoidCallback<Exception>() {
//            @Override
//            public void run(HawtDBSession session) throws Exception {
//                Iterator<QueueEntryRecord> qRecords = session.queueListMessagesQueue(queue, 0L, -1L, -1);
//                Assert.assertTrue(qRecords.hasNext());
//                QueueEntryRecord qr = qRecords.next();
//                Assert.assertEquals(qEntryRecord.queueKey, qr.queueKey);
//                Assert.assertEquals(qEntryRecord.messageKey, message.key);
//                MessageRecord record = session.messageGetRecord(qr.messageKey);
//                assertEquals(record, message);
//            }
//        }, null);
//    }
//
//    public void testStoreExecuteExceptionPassthrough() throws Exception {
//        try {
//            store.execute(new VoidCallback<Exception>() {
//                @Override
//                public void run(HawtDBSession session) throws Exception {
//                    QueueRecord qd = new QueueRecord();
//                    qd.name = new AsciiBuffer("test");
//                    session.queueAdd(qd);
//                    throw new IOException("Expected");
//                }
//            }, null);
//            Assert.fail("Expected IOException");
//        } catch (IOException e) {
//        }
//
//        // If the store implementation is transactional, then the work done
//        // should
//        // have been rolled back.
//        if (isStoreTransactional()) {
//            store.execute(new VoidCallback<Exception>() {
//                @Override
//                public void run(HawtDBSession session) throws Exception {
//                    Iterator<QueueStatus> list = session.queueList(null, 100);
//                    Assert.assertFalse(list.hasNext());
//                }
//            }, null);
//        }
//    }
//
//    static void assertEquals(MessageRecord expected, MessageRecord actual) {
//        Assert.assertEquals(expected.value, actual.value);
//        Assert.assertEquals(expected.protocol, actual.protocol);
//        Assert.assertEquals(expected.stream, actual.stream);
//        Assert.assertEquals(expected.size, actual.size);
//    }
//
//    static void assertEquals(QueueRecord expected, QueueRecord actual) {
//        assertEquals(expected.queueType, actual.queueType);
//        assertEquals(expected.name, actual.name);
//        //TODO test partitions?
//
//    }

}
