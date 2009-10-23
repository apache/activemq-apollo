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
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.queue.SaveableQueueElement;
import org.apache.activemq.util.buffer.AsciiBuffer;

/**
 * @author cmacnaug
 * 
 */
public class MessageDeliveryWrapper implements MessageDelivery {

    private final MessageDelivery delegate;

    public void acknowledge(SaveableQueueElement<MessageDelivery> sqe) {
        delegate.acknowledge(sqe);
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public <T> T asType(Class<T> type) {
        return delegate.asType(type);
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public MessageRecord createMessageRecord() {
        return delegate.createMessageRecord();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public Destination getDestination() {
        return delegate.getDestination();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public int getFlowLimiterSize() {
        return delegate.getFlowLimiterSize();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public AsciiBuffer getMsgId() {
        return delegate.getMsgId();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public int getPriority() {
        return delegate.getPriority();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public AsciiBuffer getProducerId() {
        return delegate.getProducerId();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public long getStoreTracking() {
        return delegate.getStoreTracking();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public long getTransactionId() {
        return delegate.getTransactionId();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public boolean isFromStore() {
        return delegate.isFromStore();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
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

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public boolean isResponseRequired() {
        return delegate.isResponseRequired();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public void onMessagePersisted() {
        delegate.onMessagePersisted();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public void persist(SaveableQueueElement<MessageDelivery> elem, ISourceController<?> controller, boolean delayable) {
        delegate.persist(elem, controller, delayable);
    }

    public MessageEvaluationContext createMessageEvaluationContext() {
        return delegate.createMessageEvaluationContext();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.apollo.broker.MessageDelivery#persist(org.apache.activemq.apollo.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    MessageDeliveryWrapper(MessageDelivery delivery) {
        delegate = delivery;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.apollo.broker.MessageDelivery#beginDispatch(org.apache
     * .activemq.apollo.broker.BrokerDatabase)
     */
    public void beginDispatch(BrokerDatabase database) {
        delegate.beginDispatch(database);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.apollo.broker.MessageDelivery#finishDispatch(org.
     * apache.activemq.flow.ISourceController)
     */
    public void finishDispatch(ISourceController<?> controller) throws IOException {
        delegate.finishDispatch(controller);
    }

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
