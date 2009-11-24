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
package org.apache.activemq.apollo.stomp;

import java.io.IOException;

import org.apache.activemq.apollo.broker.BrokerMessageDelivery;
import org.apache.activemq.apollo.broker.Destination;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;

public class StompMessageDelivery extends BrokerMessageDelivery {
    
    static final private AsciiBuffer ENCODING = new AsciiBuffer("stomp");

    private final StompFrame frame;
    private Destination destination;
    private Runnable completionCallback;
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
            AsciiBuffer p = frame.get(Stomp.Headers.Message.PRORITY);
            try {
                priority = (p == null) ? 4 : Integer.parseInt(p.toString());
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
            AsciiBuffer t = frame.get(Stomp.Headers.Message.EXPIRATION_TIME);
            try {
                tte = (t == null) ? -1 : Long.parseLong(t.toString());
            } catch (NumberFormatException e) {
                tte = 1;
            }
        }
        return tte;
    }

    public AsciiBuffer getMsgId() {
        if (msgId == null) {
            AsciiBuffer p = frame.get(Stomp.Headers.Message.MESSAGE_ID);
            if (p != null) {
                msgId = new AsciiBuffer(p.toString());
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

    public StompFrame getStompFrame() {
        return frame;
    }

    public boolean isPersistent() {
        AsciiBuffer persistent = frame.get(Stomp.Headers.Send.PERSISTENT);
        if( persistent==null ) {
            return false;
        }
        return Stomp.TRUE.equals(persistent);
    }

    public boolean isResponseRequired() {
        return frame.getHeaders().containsKey(Stomp.Headers.RECEIPT_REQUESTED);
    }

    public void onMessagePersisted() {
        if (persistListener != null) {
            persistListener.onMessagePersisted(this);
            persistListener = null;
        }
    }

    public AsciiBuffer getStoreEncoding() {
        return ENCODING;
    }
    
    public Buffer getStoreEncoded() {
        try {
            return StompWireFormat.INSTANCE.marshal(frame);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    

    public MessageEvaluationContext createMessageEvaluationContext() {
        return new StompMessageEvaluationContext();
    }

}
