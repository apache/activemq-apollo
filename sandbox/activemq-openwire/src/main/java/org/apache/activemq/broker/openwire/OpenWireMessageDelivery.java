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

import java.io.IOException;

import org.apache.activemq.apollo.broker.BrokerMessageDelivery;
import org.apache.activemq.apollo.broker.Destination;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.openwire.OpenWireFormat;
import org.fusesource.hawtbuf.AsciiBuffer;
import org.fusesource.hawtbuf.Buffer;

public class OpenWireMessageDelivery extends BrokerMessageDelivery {

    static final private AsciiBuffer ENCODING = new AsciiBuffer("openwire");

    private final Message message;
    private AsciiBuffer producerId;
    private OpenWireFormat storeWireFormat;
    private PersistListener persistListener = null;
    private final int size;

    public interface PersistListener {
        public void onMessagePersisted(OpenWireMessageDelivery delivery);
    }

    public OpenWireMessageDelivery(Message message) {
        this.message = message;
        this.size = message.getSize();
    }

    public void setPersistListener(PersistListener listener) {
        persistListener = listener;
    }

    public Destination getDestination() {
        return message.getDestination();
    }

    public int getMemorySize() {
        return size;
    }

    public int getPriority() {
        return message.getPriority();
    }

    public AsciiBuffer getMsgId() {
        return new AsciiBuffer(message.getMessageId().toString());
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

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#getTTE()
     */
    public long getExpiration() {
        return message.getExpiration();
    }

    public MessageEvaluationContext createMessageEvaluationContext() {
        return new OpenwireMessageEvaluationContext(message);
    }

    public String toString() {
        return message.getMessageId().toString();
    }

    public AsciiBuffer getStoreEncoding() {
        return ENCODING;
    }
    
    public Buffer getStoreEncoded() {
        Buffer bytes;
        try {
            bytes = storeWireFormat.marshal(message);
        } catch (IOException e) {
            return null;
        }
        return bytes;
    }
    

    public void setStoreWireFormat(OpenWireFormat wireFormat) {
        this.storeWireFormat = wireFormat;
    }
    
}
