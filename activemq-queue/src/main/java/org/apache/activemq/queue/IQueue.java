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

import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.queue.QueueStore.PersistentQueue;
import org.apache.activemq.util.Mapper;

public interface IQueue<K, V> extends IFlowQueue<V>, PersistentQueue<K, V> {

    /**
     * @return the number of elements currently held by the queue.
     */
    public int getEnqueuedCount();

    /**
     * @return the size of the elements currently held in the queue.
     */
    public long getEnqueuedSize();

    /**
     * Sets a mapper returning the expiration time for elements in this queue. A
     * positive value indicates that the message has an expiration time.
     * 
     * @param expirationMapper
     *            The expiration mapper.
     */
    public void setExpirationMapper(Mapper<Long, V> expirationMapper);

    /**
     * Sets the dispatcher for the queue.
     * 
     * @param dispatcher
     *            The dispatcher to be used by the queue.
     */
    public void setDispatcher(Dispatcher dispatcher);

    /**
     * Sets the base dispatch priority for the queue. Setting to higher value
     * will increase the preference with which the dispatcher dispatches the
     * queue. If the queue itself is priority based, the queue may further
     * increase it's dispatch priority based on the priority of elements that it
     * holds.
     * 
     * @param priority
     *            The base priority for the queue
     */
    public void setDispatchPriority(int priority);
    
    /**
     * Removes the element specified by the given key from the queue:
     * @param key The key.
    public void acknowledge(K key);
    */
    
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

    /**
     * The queue is stopped via {@link #stop()} then shutdown. Once shutdown an
     * {@link IQueue} cannot be restarted. Attempts to manipulate the queue once
     * the queue is shutdown will thrown an {@link IllegalStateException} unless
     * otherwise documented.
     * 
     */
    public void shutdown(Runnable onShutdown);
    
    /**
     * Removes the element with the given sequence from this queue
     * @param key The sequence key. 
     */
    public void remove(long sequence);

}
