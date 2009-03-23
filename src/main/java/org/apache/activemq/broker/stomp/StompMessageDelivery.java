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

import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompFrame;

public class StompMessageDelivery implements MessageDelivery {

    static final private AsciiBuffer ENCODING = new AsciiBuffer("stomp");

    private final StompFrame frame;
    private Destination destination;
    private Runnable completionCallback;
    private String receiptId;
    private int priority = Integer.MIN_VALUE;
    private AsciiBuffer msgId;
    private long tracking = -1;
    private PersistListener persistListener = null;

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

    public int getFlowLimiterSize() {
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

    public long getTrackingNumber() {
        return tracking;
    }

    public void setTrackingNumber(long tracking) {
        this.tracking = tracking;
    }

    /**
     * Returns the message's buffer representation.
     * 
     * @return
     */
    public Buffer getMessageBuffer() {
        // Todo use asType() instead?
        throw new UnsupportedOperationException("not yet implemented");
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

    public AsciiBuffer getEncoding() {
        return ENCODING;
    }

    public long getStreamId() {
        return 0;
    }
}
