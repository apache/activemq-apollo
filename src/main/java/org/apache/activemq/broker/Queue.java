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
import java.util.HashMap;

import org.apache.activemq.broker.DeliveryTarget;
import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.queue.IQueue;
import org.apache.activemq.queue.QueueStore;
import org.apache.activemq.queue.Subscription;
import org.apache.activemq.queue.QueueStore.SaveableQueueElement;
import org.apache.activemq.queue.Subscription.SubscriptionDeliveryCallback;

public class Queue implements DeliveryTarget {

    HashMap<DeliveryTarget, Subscription<MessageDelivery>> subs = new HashMap<DeliveryTarget, Subscription<MessageDelivery>>();
    private Destination destination;
    private IQueue<Long, MessageDelivery> queue;
    private VirtualHost virtualHost;

    Queue(IQueue<Long, MessageDelivery> queue) {
        this.queue = queue;
    }

    public final void deliver(MessageDelivery delivery, ISourceController<?> source) {
        queue.add(delivery, source);
    }

    public final void addConsumer(final DeliveryTarget dt) {
        Subscription<MessageDelivery> sub = new QueueSubscription(dt);

        Subscription<MessageDelivery> old = subs.put(dt, sub);
        if (old == null) {
            queue.addSubscription(sub);
        } else {
            subs.put(dt, old);
        }
    }

    public boolean removeSubscirption(final DeliveryTarget dt) {
        Subscription<MessageDelivery> sub = subs.remove(dt);
        if (sub != null) {
            return queue.removeSubscription(sub);
        }
        return false;
    }

    public void start() throws Exception {
        queue.start();
    }

    public void stop() throws Exception {
        if (queue != null) {
            queue.stop();
        }
    }

    public IFlowSink<MessageDelivery> getSink() {
        return queue;
    }

    public boolean hasSelector() {
        return false;
    }

    public boolean match(MessageDelivery message) {
        return true;
    }

    public VirtualHost getBroker() {
        return virtualHost;
    }

    public void setVirtualHost(VirtualHost virtualHost) {
        this.virtualHost = virtualHost;
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    public final Destination getDestination() {
        return destination;
    }

    public boolean isDurable() {
        return true;
    }

    public static class QueueSubscription implements Subscription<MessageDelivery> {
        final DeliveryTarget target;

        public QueueSubscription(DeliveryTarget dt) {
            this.target = dt;
        }

        public boolean matches(MessageDelivery message) {
            return target.match(message);
        }

        public boolean hasSelector() {
            return target.hasSelector();
        }

        public boolean isRemoveOnDispatch(MessageDelivery delivery) {
            return !delivery.isPersistent();
        }

        public IFlowSink<MessageDelivery> getSink() {
            return target.getSink();
        }

        @Override
        public String toString() {
            return target.getSink().toString();
        }

        public boolean offer(MessageDelivery elem, ISourceController<MessageDelivery> controller, SubscriptionDeliveryCallback callback) {
            return target.getSink().offer(new QueueDelivery(elem, callback), controller);
        }

        public boolean isBrowser() {
            return false;
        }
    }

    private static class QueueDelivery extends MessageDeliveryWrapper {
        private final SubscriptionDeliveryCallback callback;

        QueueDelivery(MessageDelivery delivery, SubscriptionDeliveryCallback callback) {
            super(delivery);
            this.callback = callback;
        }

        @Override
        public void persist(SaveableQueueElement<MessageDelivery> elem, ISourceController<?> controller, boolean delayable) {
            // We override this for queue deliveries as the sub needn't
            // persist the message
        }

        public void acknowledge(QueueStore.QueueDescriptor queue) {
            if (callback != null) {
                callback.acknowledge();
            }
        }

    }
}