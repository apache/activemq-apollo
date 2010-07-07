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
package org.apache.activemq.apollo.broker;

import java.io.IOException;

import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.util.buffer.AsciiBuffer;

/**
 * @author cmacnaug
 * 
 */
public class MessageDeliveryWrapper implements MessageDelivery {

    private final MessageDelivery delegate;
//
//    public void acknowledge(SaveableQueueElement<MessageDelivery> sqe) {
//        delegate.acknowledge(sqe);
//    }

    public <T> T asType(Class<T> type) {
        return delegate.asType(type);
    }

    public MessageRecord createMessageRecord() {
        return delegate.createMessageRecord();
    }

    public Destination getDestination() {
        return delegate.getDestination();
    }

    public int getFlowLimiterSize() {
        return delegate.getFlowLimiterSize();
    }

    public AsciiBuffer getMsgId() {
        return delegate.getMsgId();
    }

    public int getPriority() {
        return delegate.getPriority();
    }

    public AsciiBuffer getProducerId() {
        return delegate.getProducerId();
    }

    public long getStoreTracking() {
        return delegate.getStoreTracking();
    }

    public long getTransactionId() {
        return delegate.getTransactionId();
    }

    public boolean isFromStore() {
        return delegate.isFromStore();
    }

    public boolean isPersistent() {
        return delegate.isPersistent();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#getExpiration()
     */
    public long getExpiration() {
        return delegate.getExpiration();
    }

    public boolean isResponseRequired() {
        return delegate.isResponseRequired();
    }

    public void onMessagePersisted() {
        delegate.onMessagePersisted();
    }

//    public void persist(SaveableQueueElement<MessageDelivery> elem, ISourceController<?> controller, boolean delayable) {
//        delegate.persist(elem, controller, delayable);
//    }
//
    public MessageEvaluationContext createMessageEvaluationContext() {
        return delegate.createMessageEvaluationContext();
    }

    MessageDeliveryWrapper(MessageDelivery delivery) {
        delegate = delivery;
    }

    public void beginDispatch(BrokerDatabase database) {
        delegate.beginDispatch(database);
    }

//    public void finishDispatch(ISourceController<?> controller) throws IOException {
//        delegate.finishDispatch(controller);
//    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.apollo.broker.MessageDelivery#setStoreTracking(long)
     */
    public void setStoreTracking(long tracking) {
        delegate.setStoreTracking(tracking);
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.apollo.broker.MessageDelivery#clearTransactionId()
     */
    public void clearTransactionId() {
        delegate.clearTransactionId();
    }
}
