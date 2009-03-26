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
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.activemq.broker.store.Store.QueueRecord;
import org.apache.activemq.broker.store.kahadb.Data.QueueAddMessage;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.index.BTreeVisitor;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.LongMarshaller;
import org.apache.kahadb.util.Marshaller;

public class DestinationEntity {
    
    public final static  Marshaller<DestinationEntity> MARSHALLER = new Marshaller<DestinationEntity>() {
        
        public Class<DestinationEntity> getType() {
            return DestinationEntity.class;
        }

        public DestinationEntity readPayload(DataInput dataIn) throws IOException {
            DestinationEntity value = new DestinationEntity();
            value.queueIndex = new BTreeIndex<Long, QueueRecord>(dataIn.readLong());
            return value;
        }

        public void writePayload(DestinationEntity value, DataOutput dataOut) throws IOException {
            dataOut.writeLong(value.queueIndex.getPageId());
        }
    };

    private long nextQueueKey;
    private BTreeIndex<Long, QueueRecord> queueIndex;

    ///////////////////////////////////////////////////////////////////
    // Lifecycle Methods.
    ///////////////////////////////////////////////////////////////////
    public void allocate(Transaction tx) throws IOException {
        queueIndex = new BTreeIndex<Long, QueueRecord>(tx.allocate());
    }
    
    public void deallocate(Transaction tx) throws IOException {
        queueIndex.clear(tx);
        tx.free(queueIndex.getPageId());
        queueIndex=null;
    }
    
    public void load(Transaction tx) throws IOException {
        if( queueIndex.getPageFile()==null ) {
            
            queueIndex.setPageFile(tx.getPageFile());
            queueIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
            queueIndex.setValueMarshaller(Marshallers.QUEUE_RECORD_MARSHALLER);
            queueIndex.load(tx);
    
            // Figure out the next key using the last entry in the destination.
            Entry<Long, QueueRecord> lastEntry = queueIndex.getLast(tx);
            if( lastEntry!=null ) {
                nextQueueKey = lastEntry.getKey()+1;
            }
        }
    }
    
    ///////////////////////////////////////////////////////////////////
    // Message Methods.
    ///////////////////////////////////////////////////////////////////
    public Long nextQueueKey() {
        return nextQueueKey++;
    }
    
    public void add(Transaction tx, QueueAddMessage command) throws IOException {
        QueueRecord value = new QueueRecord();
        value.setAttachment(command.getAttachment());
        value.setMessageKey(command.getMessageKey());
        value.setQueueKey(command.getQueueKey());
        queueIndex.put(tx, value.getQueueKey(), value);
    }

    public void remove(Transaction tx, long queueKey) throws IOException {
        queueIndex.remove(tx, queueKey);
    }

    public Iterator<QueueRecord> listMessages(Transaction tx, Long firstQueueKey, final int max) throws IOException {
        final ArrayList<QueueRecord> rc = new ArrayList<QueueRecord>(max);
        queueIndex.visit(tx, new BTreeVisitor.GTEVisitor<Long, QueueRecord>(firstQueueKey) {
            @Override
            public boolean isInterestedInKeysBetween(Long first, Long second) {
                if (rc.size() >= max)
                    return false;
                return super.isInterestedInKeysBetween(first, second);
            }

            @Override
            protected void matched(Long key, QueueRecord value) {
                if (rc.size() >= max)
                    return;
                rc.add(value);
            }
        });
        return rc.iterator();
    }


}