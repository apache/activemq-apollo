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
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.kahadb.Data.MessageAdd;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.kahadb.index.BTreeIndex;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Page;
import org.apache.kahadb.page.Transaction;
import org.apache.kahadb.util.Marshaller;

public class RootEntity {
    
    public final static Marshaller<RootEntity> MARSHALLER = new Marshaller<RootEntity>() {
        public Class<RootEntity> getType() {
            return RootEntity.class;
        }

        public RootEntity readPayload(DataInput is) throws IOException {
            RootEntity rc = new RootEntity();
            rc.state = is.readInt();
            rc.destinationIndex = new BTreeIndex<AsciiBuffer, DestinationEntity>(is.readLong());
            if (is.readBoolean()) {
                rc.lastUpdate = Marshallers.LOCATION_MARSHALLER.readPayload(is);
            } else {
                rc.lastUpdate = null;
            }
            return rc;
        }

        public void writePayload(RootEntity object, DataOutput os) throws IOException {
            os.writeInt(object.state);
            os.writeLong(object.destinationIndex.getPageId());
            if (object.lastUpdate != null) {
                os.writeBoolean(true);
                Marshallers.LOCATION_MARSHALLER.writePayload(object.lastUpdate, os);
            } else {
                os.writeBoolean(false);
            }
        }
    };

    // The root page the this object's state is stored on.
    // private Page<StoredDBState> page;

    // State information about the index
    private long pageId;
    private int state;
    private Location lastUpdate;

    // Message Indexes
    private long nextMessageKey;
    private BTreeIndex<Long, MessageKeys> messageKeyIndex;
    private BTreeIndex<Location, Long> locationIndex;
    private BTreeIndex<AsciiBuffer, Long> messageIdIndex;

    // The destinations
    private BTreeIndex<AsciiBuffer, DestinationEntity> destinationIndex;
    private final TreeMap<AsciiBuffer, DestinationEntity> destinations = new TreeMap<AsciiBuffer, DestinationEntity>();

    ///////////////////////////////////////////////////////////////////
    // Lifecycle Methods.
    ///////////////////////////////////////////////////////////////////
    
    public void allocate(Transaction tx) throws IOException {
        // First time this is created.. Initialize a new pagefile.
        Page<RootEntity> page = tx.allocate();
        pageId = page.getPageId();
        assert pageId == 0;
        
        state = KahaDBStore.CLOSED_STATE;
        destinationIndex = new BTreeIndex<AsciiBuffer, DestinationEntity>(tx.getPageFile(), tx.allocate().getPageId());

        page.set(this);
        tx.store(page, MARSHALLER, true);
    }
    
    public void load(Transaction tx) throws IOException {
        destinationIndex.setPageFile(tx.getPageFile());
        destinationIndex.setKeyMarshaller(Marshallers.ASCII_BUFFER_MARSHALLER);
        destinationIndex.setValueMarshaller(DestinationEntity.MARSHALLER);
        destinationIndex.load(tx);
        
        // Keep the StoredDestinations loaded
        destinations.clear();
        for (Iterator<Entry<AsciiBuffer, DestinationEntity>> iterator = destinationIndex.iterator(tx); iterator.hasNext();) {
            Entry<AsciiBuffer, DestinationEntity> entry = iterator.next();
            entry.getValue().load(tx);
            destinations.put(entry.getKey(), entry.getValue());
        }        
    }
    
    public void store(Transaction tx) throws IOException {
        Page<RootEntity> page = tx.load(pageId, null);
        page.set(this);
        tx.store(page, RootEntity.MARSHALLER, true);
    }

    ///////////////////////////////////////////////////////////////////
    // Message Methods.
    ///////////////////////////////////////////////////////////////////
    public Long nextMessageKey() {
        return nextMessageKey++;
    }

    public void messageAdd(Transaction tx, MessageAdd command, Location location) throws IOException {
        long id = nextMessageKey++;
        Long previous = locationIndex.put(tx, location, id);
        if( previous == null ) {
            messageIdIndex.put(tx, command.getMessageId(), id);
            messageKeyIndex.put(tx, id, new MessageKeys(command.getMessageId(), location));
        } else {
            // Message existed.. undo the index update we just did.  Chances
            // are it's a transaction replay.
            locationIndex.put(tx, location, previous);
        }
    }

    public Long messageGetKey(Transaction tx, AsciiBuffer messageId) {
        try {
            return messageIdIndex.get(tx, messageId);
        } catch (IOException e) {
            throw new Store.FatalStoreException(e);
        }
    }
    
    public Location messageGetLocation(Transaction tx, Long messageKey) {
        try {
            MessageKeys t = messageKeyIndex.get(tx, messageKey);
            if( t==null ) {
                return null;
            }
            return t.location;
        } catch (IOException e) {
            throw new Store.FatalStoreException(e);
        }
    }

    ///////////////////////////////////////////////////////////////////
    // Queue Methods.
    ///////////////////////////////////////////////////////////////////
    public void queueAdd(Transaction tx, AsciiBuffer queueName) throws IOException {
        if( destinationIndex.get(tx, queueName)==null ) {
            DestinationEntity rc = new DestinationEntity();
            rc.allocate(tx);
            destinationIndex.put(tx, queueName, rc);
            rc.load(tx);
            destinations.put(queueName, rc);
        }
    }

    public void queueRemove(Transaction tx, AsciiBuffer queueName) throws IOException {
        DestinationEntity destination = destinations.get(queueName);
        if( destination!=null ) {
            destinationIndex.remove(tx, queueName);
            destinations.remove(queueName);
            destination.deallocate(tx);
        }
    }

    public DestinationEntity getDestination(AsciiBuffer queueName) {
        return destinations.get(queueName);
    }
    
    public Iterator<AsciiBuffer> queueList(Transaction tx, AsciiBuffer firstQueueName, int max) {
        return list(destinations, firstQueueName, max);
    }
    
    static private <Key,Value> Iterator<Key> list(TreeMap<Key, Value> map, Key first, int max) {
        ArrayList<Key> rc = new ArrayList<Key>(max);
        Set<Key> keys = (first==null ? map : map.tailMap(first)).keySet();
        for (Key buffer : keys) {
            if( rc.size() >= max ) {
                break;
            }
            rc.add(buffer);
        }
        return rc.iterator();
    }

    public long getPageId() {
        return pageId;
    }

    public void setPageId(long pageId) {
        this.pageId = pageId;
    }

    public int getState() {
        return state;
    }

    public void setState(int state) {
        this.state = state;
    }

    public Location getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(Location lastUpdate) {
        this.lastUpdate = lastUpdate;
    }


}