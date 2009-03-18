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
import java.util.Map;

import org.apache.activemq.broker.store.kahadb.Operation.AddOpperation;
import org.apache.activemq.broker.store.kahadb.Operation.RemoveOpperation;
import org.apache.activemq.broker.store.kahadb.data.KahaCommitCommand.KahaCommitCommandBean;
import org.apache.activemq.broker.store.kahadb.data.KahaPrepareCommand.KahaPrepareCommandBean;
import org.apache.activemq.broker.store.kahadb.data.KahaRollbackCommand.KahaRollbackCommandBean;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.TransactionRecoveryListener;
import org.apache.activemq.store.TransactionStore;

public class KahaDBTransactionStore implements TransactionStore {
    /**
     * 
     */
    private final KahaDBStore kahaDBStore;

    /**
     * @param kahaDBStore
     */
    KahaDBTransactionStore(KahaDBStore kahaDBStore) {
        this.kahaDBStore = kahaDBStore;
    }

    public void commit(TransactionId txid, boolean wasPrepared) throws IOException {
        this.kahaDBStore.store(new KahaCommitCommandBean().setTransactionInfo(this.kahaDBStore.createTransactionInfo(txid)), true);
    }

    public void prepare(TransactionId txid) throws IOException {
        this.kahaDBStore.store(new KahaPrepareCommandBean().setTransactionInfo(this.kahaDBStore.createTransactionInfo(txid)), true);
    }

    public void rollback(TransactionId txid) throws IOException {
        this.kahaDBStore.store(new KahaRollbackCommandBean().setTransactionInfo(this.kahaDBStore.createTransactionInfo(txid)), false);
    }

    public void recover(TransactionRecoveryListener listener) throws IOException {
        for (Map.Entry<TransactionId, ArrayList<Operation>> entry : this.kahaDBStore.preparedTransactions.entrySet()) {
            XATransactionId xid = (XATransactionId)entry.getKey();
            ArrayList<Message> messageList = new ArrayList<Message>();
            ArrayList<MessageAck> ackList = new ArrayList<MessageAck>();
            
            for (Operation op : entry.getValue()) {
                if( op.getClass() == AddOpperation.class ) {
                    AddOpperation addOp = (AddOpperation)op;
                    Message msg = (Message)this.kahaDBStore.wireFormat.unmarshal( new DataInputStream(addOp.getCommand().getMessage().newInput()) );
                    messageList.add(msg);
                } else {
                    RemoveOpperation rmOp = (RemoveOpperation)op;
                    MessageAck ack = (MessageAck)this.kahaDBStore.wireFormat.unmarshal( new DataInputStream(rmOp.getCommand().getAck().newInput()) );
                    ackList.add(ack);
                }
            }
            
            Message[] addedMessages = new Message[messageList.size()];
            MessageAck[] acks = new MessageAck[ackList.size()];
            messageList.toArray(addedMessages);
            ackList.toArray(acks);
            listener.recover(xid, addedMessages, acks);
        }
    }

    public void start() throws Exception {
    }

    public void stop() throws Exception {
    }
}