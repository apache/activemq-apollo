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
import org.apache.activemq.queue.QueueStore.PersistentQueue;
import org.apache.activemq.util.Mapper;

public interface IQueue<K, V> extends IFlowSink<V>, PersistentQueue<K,V>{

    /**
     * @return the number of elements currently held by the queue.
     */
    public int getEnqueuedCount();

    /**
     * @return the size of the elements currently held in the queue.
     */
    public long getEnqueuedSize();

    /**
     * Adds a subscription to the queue. When the queue is started and elements
     * are available, they will be given to the subscription.
     * 
     * @param sub
     *            The subscription to add to the queue.
     */
    public void addSubscription(Subscription<V> sub);

    /**
     * Removes a subscription from the queue.
     * 
     * @param sub
     *            The subscription to remove.
     */
    public boolean removeSubscription(Subscription<V> sub);

    /**
     * Sets a mapper returning the expiration time for elements in this 
     * queue. A positive value indicates that the message has an expiration
     * time. 
     * 
     * @param expirationMapper The expiration mapper.
     */
    public void setExpirationMapper(Mapper<Long, V> expirationMapper);
    
    /**
     * Sets the dispatcher for the queue.
     * 
     * @param dispatcher
     *            The dispatcher to be used by the queue.
     */
    public void setDispatcher(IDispatcher dispatcher);

    /**
     * Starts the queue.
     */
    public void start();

    /**
     * Stops the queue. Elements can still be added to the queue, but they will
     * not be dispatched to subscriptions until the queue is again restarted.
     * 
     * @param dispatcher
     *            The dispatcher to be used by the queue.
     */
    public void stop();

}
