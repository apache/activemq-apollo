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

import java.io.IOException;

import org.apache.activemq.broker.store.Store.MessageRecord;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.queue.QueueStore;

public class MessageDeliveryWrapper implements MessageDelivery {

    private final MessageDelivery delegate;
    
    public void acknowledge(QueueStore.QueueDescriptor queue) {
        delegate.acknowledge(queue);
    }

    public <T> T asType(Class<T> type) {
        return delegate.asType(type);
    }

    public MessageRecord createMessageRecord() throws IOException {
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

    public Buffer getTransactionId() {
        return delegate.getTransactionId();
    }

    public boolean isFromStore() {
        return delegate.isFromStore();
    }

    public boolean isPersistent() {
        return delegate.isPersistent();
    }

    public boolean isResponseRequired() {
        return delegate.isResponseRequired();
    }

    public void onMessagePersisted() {
        delegate.onMessagePersisted();
    }

    public void persist(QueueStore.QueueDescriptor queue, ISourceController<?> controller, long sequenceNumber, boolean delayable) throws IOException {
        delegate.persist(queue, controller, sequenceNumber, delayable);
    }

    MessageDeliveryWrapper(MessageDelivery delivery) {
        delegate = delivery;
    }
}
