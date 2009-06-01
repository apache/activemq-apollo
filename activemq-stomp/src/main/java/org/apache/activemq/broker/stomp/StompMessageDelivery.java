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
package org.apache.activemq.broker.stomp;

import org.apache.activemq.broker.BrokerMessageDelivery;
import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompFrame;

public class StompMessageDelivery extends BrokerMessageDelivery {

    static final private AsciiBuffer ENCODING = new AsciiBuffer("stomp");

    private final StompFrame frame;
    private Destination destination;
    private Runnable completionCallback;
    private String receiptId;
    private int priority = Integer.MIN_VALUE;
    private AsciiBuffer msgId;
    private PersistListener persistListener = null;
    private long tte = Long.MIN_VALUE;

    public interface PersistListener {
        public void onMessagePersisted(StompMessageDelivery delivery);
    }

    public StompMessageDelivery(StompFrame frame, Destination destiantion) {
        this.frame = frame;
        this.destination = destiantion;
        this.receiptId = frame.getHeaders().remove(Stomp.Headers.RECEIPT_REQUESTED);
    }

    public void setPersistListener(PersistListener listener) {
        persistListener = listener;
    }

    public Destination getDestination() {
        return destination;
    }

    public int getMemorySize() {
        return frame.getContent().length;
    }

    public int getPriority() {
        if (priority == Integer.MIN_VALUE) {
            String p = frame.getHeaders().get(Stomp.Headers.Message.PRORITY);
            try {
                priority = (p == null) ? 4 : Integer.parseInt(p);
            } catch (NumberFormatException e) {
                priority = 4;
            }
        }
        return priority;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#getTTE()
     */
    public long getExpiration() {
        if (tte == Long.MIN_VALUE) {
            String t = frame.getHeaders().get(Stomp.Headers.Message.EXPIRATION_TIME);
            try {
                tte = (t == null) ? -1 : Long.parseLong(t);
            } catch (NumberFormatException e) {
                tte = 1;
            }
        }
        return tte;
    }
    

    public AsciiBuffer getMsgId() {
        if (msgId == null) {
            String p = frame.getHeaders().get(Stomp.Headers.Message.MESSAGE_ID);
            if (p != null) {
                msgId = new AsciiBuffer(p);
            }
        }
        return msgId;
    }

    public AsciiBuffer getProducerId() {
        return null;
    }

    public Runnable getCompletionCallback() {
        return completionCallback;
    }

    public void setCompletionCallback(Runnable completionCallback) {
        this.completionCallback = completionCallback;
    }

    public <T> T asType(Class<T> type) {
        if (type == StompFrame.class) {
            return type.cast(frame);
        }
        return null;
    }

    public StompFrame getStomeFame() {
        return frame;
    }

    public String getReceiptId() {
        return receiptId;
    }

    public boolean isPersistent() {
        String p = frame.getHeaders().get(Stomp.Headers.Send.PERSISTENT);
        return "true".equals(p);
    }

    public boolean isResponseRequired() {
        return receiptId != null;
    }

    public void onMessagePersisted() {
        if (persistListener != null) {
            persistListener.onMessagePersisted(this);
            persistListener = null;
        }
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

    public MessageEvaluationContext createMessageEvaluationContext() {
        return new StompMessageEvaluationContext();
    }

    
}
