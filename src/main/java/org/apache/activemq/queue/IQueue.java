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
package org.apache.activemq.queue;

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.flow.IFlowSink;

public interface IQueue<K, V> extends IFlowSink<V>{

    /**
     * Called to initialize the queue with values from the queue store.
     * 
     * @param sequenceMin
     *            The lowest sequence number in the store.
     * @param sequenceMax
     *            The max sequence number in the store.
     * @param count
     *            The number of messages in the queue
     * @param size
     *            The size of the messages in the queue
     */
    public void initialize(long sequenceMin, long sequenceMax, int count, long size);
    
    public QueueStore.QueueDescriptor getDescriptor();
    
    public int getEnqueuedCount();
    
    public long getEnqueuedSize();
    
    public void addSubscription(Subscription<V> sub);

    public boolean removeSubscription(Subscription<V> sub);
    
    public void setStore(QueueStore<K, V> store);
    
    public void setDispatcher(IDispatcher dispatcher);
    
    public void start();
    
    public void stop();
    
    
 
}
