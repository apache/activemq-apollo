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
import java.util.Map.Entry;

import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.LongMarshaller;
import org.apache.kahadb.util.Marshaller;

public class StoredDestinationState {
    
    long nextMessageId;
    BTreeIndex<Long, Long> orderIndex;

    public void allocate(Transaction tx) throws IOException {
        orderIndex = new BTreeIndex<Long, Long>(tx.allocate());
    }
    
    public void load(Transaction tx) throws IOException {
        orderIndex.setPageFile(tx.getPageFile());
        orderIndex.setKeyMarshaller(LongMarshaller.INSTANCE);
        orderIndex.setValueMarshaller(LongMarshaller.INSTANCE);
        orderIndex.load(tx);

        // Figure out the next key using the last entry in the destination.
        Entry<Long, Long> lastEntry = orderIndex.getLast(tx);
        if( lastEntry!=null ) {
            nextMessageId = lastEntry.getKey()+1;
        }
    }

    public final static StoredDestinationMarshaller MARSHALLER = new StoredDestinationMarshaller();
    public static class StoredDestinationMarshaller implements Marshaller<StoredDestinationState> {
        
        public Class<StoredDestinationState> getType() {
            return StoredDestinationState.class;
        }

        public StoredDestinationState readPayload(DataInput dataIn) throws IOException {
            StoredDestinationState value = new StoredDestinationState();
            value.orderIndex = new BTreeIndex<Long, Long>(dataIn.readLong());
            return value;
        }

        public void writePayload(StoredDestinationState value, DataOutput dataOut) throws IOException {
            dataOut.writeLong(value.orderIndex.getPageId());
        }
    }

}