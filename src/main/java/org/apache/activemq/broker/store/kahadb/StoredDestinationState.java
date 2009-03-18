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
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.activemq.broker.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.util.Marshaller;

public class StoredDestinationState {
    long nextMessageId;
    BTreeIndex<Long, MessageKeys> orderIndex;
    BTreeIndex<Location, Long> locationIndex;
    BTreeIndex<String, Long> messageIdIndex;

    // These bits are only set for Topics
    BTreeIndex<String, KahaSubscriptionCommand> subscriptions;
    BTreeIndex<String, Long> subscriptionAcks;
    HashMap<String, Long> subscriptionCursors;
    TreeMap<Long, HashSet<String>> ackPositions;
    
    public static class StoredDestinationMarshaller implements Marshaller<StoredDestinationState> {
        private final KahaDBStore store;

        public StoredDestinationMarshaller(KahaDBStore store) {
            this.store = store;
        }

        public Class<StoredDestinationState> getType() {
            return StoredDestinationState.class;
        }

        public StoredDestinationState readPayload(DataInput dataIn) throws IOException {
            StoredDestinationState value = new StoredDestinationState();
            value.orderIndex = new BTreeIndex<Long, MessageKeys>(store.pageFile, dataIn.readLong());
            value.locationIndex = new BTreeIndex<Location, Long>(store.pageFile, dataIn.readLong());
            value.messageIdIndex = new BTreeIndex<String, Long>(store.pageFile, dataIn.readLong());

            if (dataIn.readBoolean()) {
                value.subscriptions = new BTreeIndex<String, KahaSubscriptionCommand>(store.pageFile, dataIn.readLong());
                value.subscriptionAcks = new BTreeIndex<String, Long>(store.pageFile, dataIn.readLong());
            }
            return value;
        }

        public void writePayload(StoredDestinationState value, DataOutput dataOut) throws IOException {
            dataOut.writeLong(value.orderIndex.getPageId());
            dataOut.writeLong(value.locationIndex.getPageId());
            dataOut.writeLong(value.messageIdIndex.getPageId());
            if (value.subscriptions != null) {
                dataOut.writeBoolean(true);
                dataOut.writeLong(value.subscriptions.getPageId());
                dataOut.writeLong(value.subscriptionAcks.getPageId());
            } else {
                dataOut.writeBoolean(false);
            }
        }
    }
}