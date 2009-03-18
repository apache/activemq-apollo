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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.store.kahadb.data.KahaSubscriptionCommand;
import org.apache.activemq.broker.store.kahadb.data.KahaRemoveMessageCommand.KahaRemoveMessageCommandBean;
import org.apache.activemq.broker.store.kahadb.data.KahaSubscriptionCommand.KahaSubscriptionCommandBean;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.kahadb.page.Transaction;

public class KahaDBTopicMessageStore extends KahaDBMessageStore implements TopicMessageStore {
    public KahaDBTopicMessageStore(KahaDBStore store, ActiveMQTopic destination) {
        super(store, destination);
    }
    
    public void acknowledge(ConnectionContext context, String clientId, String subscriptionName, MessageId messageId) throws IOException {
        KahaRemoveMessageCommandBean command = new KahaRemoveMessageCommandBean();
        command.setDestination(dest);
        command.setSubscriptionKey(store.subscriptionKey(clientId, subscriptionName));
        command.setMessageId(messageId.toString());
        // We are not passed a transaction info.. so we can't participate in a transaction.
        // Looks like a design issue with the TopicMessageStore interface.  Also we can't recover the original ack
        // to pass back to the XA recover method.
        // command.setTransactionInfo();
        store.store(command, false);
    }

    public void addSubsciption(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
        String subscriptionKey = store.subscriptionKey(subscriptionInfo.getClientId(), subscriptionInfo.getSubscriptionName());
        KahaSubscriptionCommandBean command = new KahaSubscriptionCommandBean();
        command.setDestination(dest);
        command.setSubscriptionKey(subscriptionKey);
        command.setRetroactive(retroactive);
        org.apache.activemq.util.ByteSequence packet = store.wireFormat.marshal(subscriptionInfo);
        command.setSubscriptionInfo(new Buffer(packet.getData(), packet.getOffset(), packet.getLength()));
        store.store(command, store.isEnableJournalDiskSyncs() && true);
    }

    public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
        KahaSubscriptionCommandBean command = new KahaSubscriptionCommandBean();
        command.setDestination(dest);
        command.setSubscriptionKey(store.subscriptionKey(clientId, subscriptionName));
        store.store(command, store.isEnableJournalDiskSyncs() && true);
    }

    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        
        final ArrayList<SubscriptionInfo> subscriptions = new ArrayList<SubscriptionInfo>();
        synchronized(store.indexMutex) {
            store.pageFile.tx().execute(new Transaction.Closure<IOException>(){
                public void execute(Transaction tx) throws IOException {
                    StoredDestinationState sd = store.getStoredDestination(dest, tx);
                    for (Iterator<Entry<String, KahaSubscriptionCommand>> iterator = sd.subscriptions.iterator(tx); iterator.hasNext();) {
                        Entry<String, KahaSubscriptionCommand> entry = iterator.next();
                        SubscriptionInfo info = (SubscriptionInfo)store.wireFormat.unmarshal( new DataInputStream(entry.getValue().getSubscriptionInfo().newInput()) );
                        subscriptions.add(info);

                    }
                }
            });
        }
        
        SubscriptionInfo[]rc=new SubscriptionInfo[subscriptions.size()];
        subscriptions.toArray(rc);
        return rc;
    }

    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        final String subscriptionKey = store.subscriptionKey(clientId, subscriptionName);
        synchronized(store.indexMutex) {
            return store.pageFile.tx().execute(new Transaction.CallableClosure<SubscriptionInfo, IOException>(){
                public SubscriptionInfo execute(Transaction tx) throws IOException {
                    StoredDestinationState sd = store.getStoredDestination(dest, tx);
                    KahaSubscriptionCommand command = sd.subscriptions.get(tx, subscriptionKey);
                    if( command ==null ) {
                        return null;
                    }
                    return (SubscriptionInfo)store.wireFormat.unmarshal( new DataInputStream(command.getSubscriptionInfo().newInput()) );
                }
            });
        }
    }
   
    public int getMessageCount(String clientId, String subscriptionName) throws IOException {
        final String subscriptionKey = store.subscriptionKey(clientId, subscriptionName);
        synchronized(store.indexMutex) {
            return store.pageFile.tx().execute(new Transaction.CallableClosure<Integer, IOException>(){
                public Integer execute(Transaction tx) throws IOException {
                    StoredDestinationState sd = store.getStoredDestination(dest, tx);
                    Long cursorPos = sd.subscriptionAcks.get(tx, subscriptionKey);
                    if ( cursorPos==null ) {
                        // The subscription might not exist.
                        return 0;
                    }
                    cursorPos += 1;
                    
                    int counter = 0;
                    for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx, cursorPos); iterator.hasNext();) {
                        iterator.next();
                        counter++;
                    }
                    return counter;
                }
            });
        }        
    }

    public void recoverSubscription(String clientId, String subscriptionName, final MessageRecoveryListener listener) throws Exception {
        final String subscriptionKey = store.subscriptionKey(clientId, subscriptionName);
        synchronized(store.indexMutex) {
            store.pageFile.tx().execute(new Transaction.Closure<Exception>(){
                public void execute(Transaction tx) throws Exception {
                    StoredDestinationState sd = store.getStoredDestination(dest, tx);
                    Long cursorPos = sd.subscriptionAcks.get(tx, subscriptionKey);
                    cursorPos += 1;
                    
                    for (Iterator<Entry<Long, MessageKeys>> iterator = sd.orderIndex.iterator(tx, cursorPos); iterator.hasNext();) {
                        Entry<Long, MessageKeys> entry = iterator.next();
                        listener.recoverMessage( store.loadMessage(entry.getValue().location ) );
                    }
                }
            });
        }
    }

    public void recoverNextMessages(String clientId, String subscriptionName, final int maxReturned, final MessageRecoveryListener listener) throws Exception {
        final String subscriptionKey = store.subscriptionKey(clientId, subscriptionName);
        synchronized(store.indexMutex) {
            store.pageFile.tx().execute(new Transaction.Closure<Exception>(){
                public void execute(Transaction tx) throws Exception {
                    StoredDestinationState sd = store.getStoredDestination(dest, tx);
                    Long cursorPos = sd.subscriptionCursors.get(subscriptionKey);
                    if( cursorPos == null ) {
                        cursorPos = sd.subscriptionAcks.get(tx, subscriptionKey);
                        cursorPos += 1;
                    }
                    
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
                        sd.subscriptionCursors.put(subscriptionKey, cursorPos+1);
                    }
                }
            });
        }
    }

    public void resetBatching(String clientId, String subscriptionName) {
        try {
            final String subscriptionKey = store.subscriptionKey(clientId, subscriptionName);
            synchronized(store.indexMutex) {
                store.pageFile.tx().execute(new Transaction.Closure<IOException>(){
                    public void execute(Transaction tx) throws IOException {
                        StoredDestinationState sd = store.getStoredDestination(dest, tx);
                        sd.subscriptionCursors.remove(subscriptionKey);
                    }
                });
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
