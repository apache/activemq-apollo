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

import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.Marshaller;

public class StoredDBState {
    
    protected Page<StoredDBState> page;
    protected int state;
    protected BTreeIndex<AsciiBuffer, StoredDestinationState> destinations;
    protected Location lastUpdate;

    // We index the messages 3 ways: by sequence id, by journal location, and by message id.
    long nextMessageId;
    protected BTreeIndex<Long, MessageKeys> orderIndex;
    protected BTreeIndex<Location, Long> locationIndex;
    protected BTreeIndex<AsciiBuffer, Long> messageIdIndex;


    public void write(DataOutput os) throws IOException {

    }
    
    public final static DBStateMarshaller MARSHALLER = new DBStateMarshaller();
    static public class DBStateMarshaller implements Marshaller<StoredDBState> {
        public Class<StoredDBState> getType() {
            return StoredDBState.class;
        }

        public StoredDBState readPayload(DataInput is) throws IOException {
            StoredDBState rc = new StoredDBState();
            rc.state = is.readInt();
            rc.destinations = new BTreeIndex<AsciiBuffer, StoredDestinationState>(is.readLong());
            if (is.readBoolean()) {
                rc.lastUpdate = Marshallers.LOCATION_MARSHALLER.readPayload(is);
            } else {
                rc.lastUpdate = null;
            }
            return rc;
        }

        public void writePayload(StoredDBState object, DataOutput os) throws IOException {
            os.writeInt(object.state);
            os.writeLong(object.destinations.getPageId());
            if (object.lastUpdate != null) {
                os.writeBoolean(true);
                Marshallers.LOCATION_MARSHALLER.writePayload(object.lastUpdate, os);
            } else {
                os.writeBoolean(false);
            }
        }
    }

    public void allocate(Transaction tx) throws IOException {
        // First time this is created.. Initialize a new pagefile.
        page = tx.allocate();
        assert page.getPageId() == 0;
        page.set(this);
        
        state = KahaDBStore.CLOSED_STATE;
        destinations = new BTreeIndex<AsciiBuffer, StoredDestinationState>(tx.getPageFile(), tx.allocate().getPageId());
        tx.store(page, MARSHALLER, true);
    }
    
    public void load(Transaction tx) throws IOException {
        destinations.setPageFile(tx.getPageFile());
        destinations.setKeyMarshaller(Marshallers.ASCII_BUFFER_MARSHALLER);
        destinations.setValueMarshaller(StoredDestinationState.MARSHALLER);
        destinations.load(tx);
    }
}