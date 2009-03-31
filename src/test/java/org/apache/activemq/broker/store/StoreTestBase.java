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

import org.apache.activemq.broker.store.Store.Callback;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.broker.store.Store.Session;
import org.apache.activemq.broker.store.Store.VoidCallback;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;

public abstract class StoreTestBase extends TestCase {

    private Store store;

    abstract protected Store createStore();

    abstract protected boolean isStoreTransactional();

    abstract protected boolean isStorePersistent();

    @Override
    protected void setUp() throws Exception {
        store = createStore();
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
        final AsciiBuffer expected = new AsciiBuffer("test");
        store.execute(new VoidCallback<Exception>() {
            @Override
            public void run(Session session) throws Exception {
                session.queueAdd(expected);
            }
        }, null);

        store.execute(new VoidCallback<Exception>() {
            @Override
            public void run(Session session) throws Exception {
                Iterator<AsciiBuffer> list = session.queueList(null, 100);
                assertTrue(list.hasNext());
                AsciiBuffer actual = list.next();
                assertEquals(expected, actual);
            }
        }, null);
    }

    public void testStoreExecuteExceptionPassthrough() throws Exception {
        try {
            store.execute(new VoidCallback<Exception>() {
                @Override
                public void run(Session session) throws Exception {
                    session.queueAdd(new AsciiBuffer("test"));
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
                    Iterator<AsciiBuffer> list = session.queueList(null, 100);
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
    }

}
