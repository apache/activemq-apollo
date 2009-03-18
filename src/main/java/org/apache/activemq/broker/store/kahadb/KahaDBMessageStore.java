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

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.store.kahadb.data.KahaDestination;
import org.apache.activemq.broker.store.kahadb.data.KahaAddMessageCommand.KahaAddMessageCommandBean;
import org.apache.activemq.broker.store.kahadb.data.KahaRemoveDestinationCommand.KahaRemoveDestinationCommandBean;
import org.apache.activemq.broker.store.kahadb.data.KahaRemoveMessageCommand.KahaRemoveMessageCommandBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.AbstractMessageStore;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.kahadb.journal.Location;
import org.apache.kahadb.page.Transaction;

public class KahaDBMessageStore extends AbstractMessageStore {
    protected KahaDestination dest;
    protected final KahaDBStore store;

    public KahaDBMessageStore(KahaDBStore store, ActiveMQDestination destination) {
        super(destination);
        this.store = store;
        this.dest = store.convert( destination );
    }

    public ActiveMQDestination getDestination() {
        return destination;
    }

    public void addMessage(ConnectionContext context, Message message) throws IOException {
        KahaAddMessageCommandBean command = new KahaAddMessageCommandBean();
        command.setDestination(dest);
        command.setMessageId(message.getMessageId().toString());
        command.setTransactionInfo( store.createTransactionInfo(message.getTransactionId()) );

        org.apache.activemq.util.ByteSequence packet = store.wireFormat.marshal(message);
        command.setMessage(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));

        store.store(command, store.isEnableJournalDiskSyncs() && message.isResponseRequired());
        
    }
    
    public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
        KahaRemoveMessageCommandBean command = new KahaRemoveMessageCommandBean();
        command.setDestination(dest);
        command.setMessageId(ack.getLastMessageId().toString());
        command.setTransactionInfo(store.createTransactionInfo(ack.getTransactionId()) );
        store.store(command, store.isEnableJournalDiskSyncs() && ack.isResponseRequired());
    }

    public void removeAllMessages(ConnectionContext context) throws IOException {
        KahaRemoveDestinationCommandBean command = new KahaRemoveDestinationCommandBean();
        command.setDestination(dest);
        store.store(command, true);
    }

    public Message getMessage(MessageId identity) throws IOException {
        final String key = identity.toString();
        
        // Hopefully one day the page file supports concurrent read operations... but for now we must
        // externally synchronize...
        Location location;
        synchronized(store.indexMutex) {
            location = store.pageFile.tx().execute(new Transaction.CallableClosure<Location, IOException>(){
                public Location execute(Transaction tx) throws IOException {
                    StoredDestinationState sd = store.getStoredDestination(dest, tx);
                    Long sequence = sd.messageIdIndex.get(tx, key);
                    if( sequence ==null ) {
                        return null;
                    }
                    return sd.orderIndex.get(tx, sequence).location;
                }
            });
        }
        if( location == null ) {
            return null;
        }
        
        return store.loadMessage(location);
    }
    
    public int getMessageCount() throws IOException {
        synchronized(store.indexMutex) {
            return store.pageFile.tx().execute(new Transaction.CallableClosure<Integer, IOException>(){
                public Integer execute(Transaction tx) throws IOException {
                    // Iterate through all index entries to get a count of messages in the destination.
                    StoredDestinationState sd = store.getStoredDestination(dest, tx);
                    int rc=0;
                    for (Iterator<Entry<Location, Long>> iterator = sd.locationIndex.iterator(tx); iterator.hasNext();) {
                        iterator.next();
                        rc++;
                    }
                    return rc;
                }
            });
        }
    }

    public void recover(final MessageRecoveryListener listener) throws Exception {
        synchronized(store.indexMutex) {
            store.pageFile.tx().execute(new Transaction.Closure<Exception>(){
                public void execute(Transaction tx) throws Exception {
                    StoredDestinationState sd = store.getStoredDestination(dest, tx);
                    for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx); iterator.hasNext();) {
                        Entry<Long, MessageKeys> entry = iterator.next();
                        listener.recoverMessage( store.loadMessage(entry.getValue().location) );
                    }
                }
            });
        }
    }

    long cursorPos=0;
    
    public void recoverNextMessages(final int maxReturned, final MessageRecoveryListener listener) throws Exception {
        synchronized(store.indexMutex) {
            store.pageFile.tx().execute(new Transaction.Closure<Exception>(){
                public void execute(Transaction tx) throws Exception {
                    StoredDestinationState sd = store.getStoredDestination(dest, tx);
                    Entry<Long, MessageKeys> entry=null;
                    int counter = 0;
                    for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx, cursorPos); iterator.hasNext();) {
                        entry = iterator.next();
                        listener.recoverMessage( store.loadMessage(entry.getValue().location ) );
                        counter++;
                        if( counter >= maxReturned ) {
                            break;
                        }
                    }
                    if( entry!=null ) {
                        cursorPos = entry.getKey()+1;
                    }
                }
            });
        }
    }

    public void resetBatching() {
        cursorPos=0;
    }

    
    @Override
    public void setBatch(MessageId identity) throws IOException {
        final String key = identity.toString();
        
        // Hopefully one day the page file supports concurrent read operations... but for now we must
        // externally synchronize...
        Long location;
        synchronized(store.indexMutex) {
            location = store.pageFile.tx().execute(new Transaction.CallableClosure<Long, IOException>(){
                public Long execute(Transaction tx) throws IOException {
                    StoredDestinationState sd = store.getStoredDestination(dest, tx);
                    return sd.messageIdIndex.get(tx, key);
                }
            });
        }
        if( location!=null ) {
            cursorPos=location+1;
        }
        
    }

    public void setMemoryUsage(MemoryUsage memoeyUSage) {
    }
    public void start() throws Exception {
    }
    public void stop() throws Exception {
    }
    
}
    