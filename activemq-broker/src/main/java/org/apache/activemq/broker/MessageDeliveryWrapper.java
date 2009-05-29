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
package org.apache.activemq.broker;

import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.broker.store.SaveableQueueElement;
import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;

/**
 * @author cmacnaug
 *
 */
public class MessageDeliveryWrapper implements MessageDelivery {

    private final MessageDelivery delegate;

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public void acknowledge(QueueDescriptor queue) {
        delegate.acknowledge(queue);
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public <T> T asType(Class<T> type) {
        return delegate.asType(type);
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public MessageRecord createMessageRecord() {
        return delegate.createMessageRecord();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public Destination getDestination() {
        return delegate.getDestination();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public int getFlowLimiterSize() {
        return delegate.getFlowLimiterSize();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public AsciiBuffer getMsgId() {
        return delegate.getMsgId();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public int getPriority() {
        return delegate.getPriority();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public AsciiBuffer getProducerId() {
        return delegate.getProducerId();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public long getStoreTracking() {
        return delegate.getStoreTracking();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public Buffer getTransactionId() {
        return delegate.getTransactionId();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public boolean isFromStore() {
        return delegate.isFromStore();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public boolean isPersistent() {
        return delegate.isPersistent();
    }
    
    
    /** (non-Javadoc)
     * @see org.apache.activemq.broker.MessageDelivery#getExpiration()
     */
    public long getExpiration() {
        return delegate.getExpiration();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public boolean isResponseRequired() {
        return delegate.isResponseRequired();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public void onMessagePersisted() {
        delegate.onMessagePersisted();
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    public void persist(SaveableQueueElement<MessageDelivery> elem, ISourceController<?> controller, boolean delayable) {
        delegate.persist(elem, controller, delayable);
    }

    /**
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.MessageDelivery#persist(org.apache.activemq.queue.QueueStore.SaveableQueueElement,
     *      org.apache.activemq.flow.ISourceController, boolean)
     */
    MessageDeliveryWrapper(MessageDelivery delivery) {
        delegate = delivery;
    }
}
