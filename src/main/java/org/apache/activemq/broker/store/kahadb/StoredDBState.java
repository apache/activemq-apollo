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

import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.util.Marshaller;

public class StoredDBState {
    
    protected final KahaDBStore store;
    protected Page<StoredDBState> page;
    protected int state;
    protected BTreeIndex<String, StoredDestinationState> destinations;
    protected Location lastUpdate;
    protected Location firstInProgressTransactionLocation;

    public StoredDBState(KahaDBStore store) {
        this.store = store;
    }


    public void read(DataInput is) throws IOException {
        state = is.readInt();
        destinations = new BTreeIndex<String, StoredDestinationState>(store.pageFile, is.readLong());
        if (is.readBoolean()) {
            lastUpdate = LocationMarshaller.INSTANCE.readPayload(is);
        } else {
            lastUpdate = null;
        }
        if (is.readBoolean()) {
            firstInProgressTransactionLocation = LocationMarshaller.INSTANCE.readPayload(is);
        } else {
            firstInProgressTransactionLocation = null;
        }
    }

    public void write(DataOutput os) throws IOException {
        os.writeInt(state);
        os.writeLong(destinations.getPageId());

        if (lastUpdate != null) {
            os.writeBoolean(true);
            LocationMarshaller.INSTANCE.writePayload(lastUpdate, os);
        } else {
            os.writeBoolean(false);
        }

        if (firstInProgressTransactionLocation != null) {
            os.writeBoolean(true);
            LocationMarshaller.INSTANCE.writePayload(firstInProgressTransactionLocation, os);
        } else {
            os.writeBoolean(false);
        }
    }
    
    static public class DBStateMarshaller implements Marshaller<StoredDBState> {
        private final KahaDBStore store;

        public DBStateMarshaller(KahaDBStore store) {
            this.store = store;
        }

        public Class<StoredDBState> getType() {
            return StoredDBState.class;
        }

        public StoredDBState readPayload(DataInput dataIn) throws IOException {
            StoredDBState rc = new StoredDBState(this.store);
            rc.read(dataIn);
            return rc;
        }

        public void writePayload(StoredDBState object, DataOutput dataOut) throws IOException {
            object.write(dataOut);
        }
    }
}