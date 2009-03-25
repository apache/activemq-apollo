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
package org.apache.activemq.broker.openwire;

import org.apache.activemq.broker.BrokerMessageDelivery;
import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.store.Store.Session.MessageRecord;
import org.apache.activemq.command.Message;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;

public class OpenWireMessageDelivery extends BrokerMessageDelivery {

    static final private AsciiBuffer ENCODING = new AsciiBuffer("openwire");

    private final Message message;
    private Destination destination;
    private AsciiBuffer producerId;
    private PersistListener persistListener = null;

    public interface PersistListener {
        public void onMessagePersisted(OpenWireMessageDelivery delivery);
    }

    public OpenWireMessageDelivery(Message message) {
        this.message = message;
    }

    public void setPersistListener(PersistListener listener) {
        persistListener = listener;
    }

    public Destination getDestination() {
        if (destination == null) {
            destination = OpenwireProtocolHandler.convert(message.getDestination());
        }
        return destination;
    }

    public int getFlowLimiterSize() {
        return 1;
    }

    public int getPriority() {
        return message.getPriority();
    }

    public AsciiBuffer getMsgId() {
        return null;
    }

    public AsciiBuffer getProducerId() {
        if (producerId == null) {
            producerId = new AsciiBuffer(message.getProducerId().toString());
        }
        return producerId;
    }

    public Message getMessage() {
        return message;
    }

    public <T> T asType(Class<T> type) {
        if (type == Message.class) {
            return type.cast(message);
        }
        // TODO: is this right?
        if (message.getClass().isAssignableFrom(type)) {
            return type.cast(message);
        }
        return null;
    }

    public boolean isPersistent() {
        return message.isPersistent();
    }

    public final void onMessagePersisted() {
        if (persistListener != null) {
            persistListener.onMessagePersisted(this);
            persistListener = null;
        }
    }

    public final boolean isResponseRequired() {
        return message.isResponseRequired();
    }


    public MessageRecord createMessageRecord() {
        MessageRecord record = new MessageRecord();
        record.setEncoding(ENCODING);
        // TODO: Serialize it..
        // record.setBuffer()
        // record.setStreamKey(stream);
        record.setMessageId(getMsgId());
        return record;
    }

    public Buffer getTransactionId() {
        // TODO Auto-generated method stub
        return null;
    }

}
