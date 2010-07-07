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

import org.apache.activemq.broker.store.QueueDescriptor;
import org.apache.activemq.flow.ISourceController;

public interface Subscription<E> {

    public interface SubscriptionDelivery<E> {
        
        /**
         * @return The descriptor of the queue from which this element came.
         */
        public QueueDescriptor getQueueDescriptor();
        
        /**
         * @return a key that can be used to remove the message from the queue. 
         */
        public long getSourceQueueRemovalKey();
        
        /**
         * Acknowledges the delivery.
         */
        public void acknowledge();

        /**
         * Indicates that the subscription no longer has interest in the element
         * and that it should be placed back on the queue.
         * 
         * The provided source controller will be blocked if there is not enough
         * space available on the queue to reenqueue the element.
         * 
         * It is illegal to call this method after a prior call to
         * {@link #acknowledge()}.
         * 
         * @param source
         *            The source controller.
         */
        public void unacquire(ISourceController<?> sourceController);

        /**
         * @return Returns true if the delivery is a redelivery
         */
        public boolean isRedelivery();
    }

    /**
     * True if the element should be removed on dispatch to the subscriptions.
     * 
     * @return true if the element should be removed on dispatch
     */
    public boolean isRemoveOnDispatch(E elem);

    /**
     * @return True if this is a subscription browser.
     */
    public boolean isBrowser();

    /**
     * Indicates that the subscription is exclusive. When there at least one
     * exclusive subscription on a shared queue, the queue will dispatch to only
     * one such consumer while there is at least one connected.
     * 
     * @return True if the Subscription is exclusive.
     */
    public boolean isExclusive();

    /**
     * Returns true if the Subscription has a selector. If true is returned the
     * {@link #matches(Object)} will be called prior to an attempt to offer the
     * message to {@link Subscription}
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
     *            The {@link SubscriptionDelivery<E>} associated with the
     *            element
     * 
     * @return true if the element was accepted false otherwise, if false is
     *         returned the caller must have called
     *         {@link ISourceController#onFlowBlock(ISinkController)} prior to
     *         returning false.
     */
    public boolean offer(E element, ISourceController<?> controller, SubscriptionDelivery<E> callback);

    /**
     * Pushes an item to the subscription. If the subscription is not remove on
     * dispatch, then it must call acknowledge method on the callback when it
     * has acknowledged the message.
     * 
     * @param element
     *            The delivery container the offered element.
     * @param controller
     *            The queue's controller, which must be used if the added
     *            element exceeds the subscription's buffer limits.
     * @param callback
     *            The {@link SubscriptionDelivery<E>} associated with the
     *            element
     * @return true if the element was accepted false otherwise, if false is
     *         returned the caller must have called
     *         {@link ISourceController#onFlowBlock(ISinkController)} prior to
     *         returning false.
     */
    public void add(E element, ISourceController<?> controller, SubscriptionDelivery<E> callback);
}
