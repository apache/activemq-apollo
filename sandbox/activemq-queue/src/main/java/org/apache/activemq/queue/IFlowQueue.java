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
import org.apache.activemq.flow.IFlowRelay;

public interface IFlowQueue<E> extends IBlockingFlowSource<E>, IPollableFlowSource<E>, IFlowRelay<E> {

    public interface FlowQueueListener {
        
        /**
         * Called when there is a queue error
         * 
         * @param queue The queue triggering the exception
         * @param thrown The exception. 
         */
        public void onQueueException(IFlowQueue<?> queue, Throwable thrown);
    }

    public void setFlowQueueListener(FlowQueueListener listener);
    
    /**
     * Adds a subscription to the queue. When the queue is started and elements
     * are available, they will be given to the subscription.
     * 
     * @param sub
     *            The subscription to add to the queue.
     */
    public void addSubscription(Subscription<E> sub);

    /**
     * Removes a subscription from the queue.
     * 
     * @param sub
     *            The subscription to remove.
     */
    public boolean removeSubscription(Subscription<E> sub);
    
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
    
    public void start();

    public void stop();    
}
