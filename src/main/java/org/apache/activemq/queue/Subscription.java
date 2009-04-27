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

import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.ISourceController;

public interface Subscription<E> {

    public interface SubscriptionDeliveryCallback {
        
        /**
         * If {@link Subscription#isBrowser()} returns false this method
         * indicates that the Subscription is finished with the element
         * and that it can be removed from the queue. 
         */
        public void acknowledge();

        /**
         * Indicates that the subscription no longer has interest in
         * the element and that it should be placed back on the queue. 
         * 
         * The provided source controller will be blocked if there 
         * is not enough space available on the sub queue to
         * reenqueue the element.
         * 
         * It is illegal to call this method after a prior call to 
         * {@link #acknowledge()}. 
         * 
         * @param source The source controller.
         */
        public void unacquire(ISourceController<?> sourceController);
    }

    /**
     * True if the message should be removed from the queue when it is
     * dispatched to this subscription.
     * 
     * @return true if the element should be removed on dispatch
     */
    public boolean isRemoveOnDispatch();
    
    /**
     * @return True if this is a subscription browser. 
     */
    public boolean isBrowser();

    public boolean isPreAcquired();

    /**
     * Returns true if the Subscription has a selector. If true
     * is returned the {@link #matches(Object)} will be called
     * prior to an attempt to offer the message to {@link Subscription}
     * 
     * @return true if this {@link Subscription} has a selector.
     */
    public boolean hasSelector();

    /**
     * Called is {@link #hasSelector()} returns true.
     * 
     * @param elem
     *            The element to match.
     * @return false if the message doesn't match
     */
    public boolean matches(E elem);

    /**
     * Offers an item to the subscription. If the subscription is not remove on
     * dispatch, then it must call acknowledge method on the callback when it
     * has acknowledged the message.
     * 
     * @param element
     *            The delivery container the offered element.
     * @param controller
     *            The queue's controller, which must be used if the offered
     *            element exceeds the subscription's buffer limits.
     * @param callback
     *            The {@link SubscriptionDeliveryCallback} associated with the element
     * 
     * @return true if the element was accepted false otherwise, if false is
     *         returned the caller must have called
     *         {@link ISourceController#onFlowBlock(ISinkController)} prior to
     *         returning false.
     */
    public boolean offer(E element, ISourceController<E> controller, SubscriptionDeliveryCallback callback);

    @Deprecated
    public IFlowSink<E> getSink();

}
