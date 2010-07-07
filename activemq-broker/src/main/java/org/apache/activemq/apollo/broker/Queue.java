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

import org.apache.activemq.apollo.broker.ProtocolHandler.ConsumerContext;
import org.fusesource.hawtdispatch.internal.util.RunnableCountDownLatch;

public class Queue implements DeliveryTarget {
    private Destination destination;

// TODO:
//    private Destination destination;
//    private VirtualHost virtualHost;
//
//    Queue() {
//        this.queue = queue;
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see
//     * org.apache.activemq.broker.DeliveryTarget#deliver(org.apache.activemq
//     * .broker.MessageDelivery, org.apache.activemq.flow.ISourceController)
//     */
//    public void deliver(MessageDelivery message, ISourceController<?> source) {
//        queue.add(message, source);
//    }
//
//    public final void addSubscription(final Subscription<MessageDelivery> sub) {
//        queue.addSubscription(sub);
//    }
//
//    public boolean removeSubscription(final Subscription<MessageDelivery> sub) {
//        return queue.removeSubscription(sub);
//    }
//
//    public void start() throws Exception {
//        queue.start();
//    }
//
//    public void stop() throws Exception {
//        if (queue != null) {
//            queue.stop();
//        }
//    }
//
//    public void shutdown(Runnable onShutdown) throws Exception {
//        if (queue != null) {
//            queue.shutdown(onShutdown);
//        }
//    }
//
//    public boolean hasSelector() {
//        return false;
//    }
//
//    public boolean matches(MessageDelivery message) {
//        return true;
//    }
//
//    public VirtualHost getBroker() {
//        return virtualHost;
//    }
//
//    public void setVirtualHost(VirtualHost virtualHost) {
//        this.virtualHost = virtualHost;
//    }
//
//    public void setDestination(Destination destination) {
//        this.destination = destination;
//    }
//
//    public final Destination getDestination() {
//        return destination;
//    }
//
//    public boolean isDurable() {
//        return true;
//    }
//
//    public static class QueueSubscription implements BrokerSubscription {
//        Subscription<MessageDelivery> subscription;
//        final Queue queue;
//
//        public QueueSubscription(Queue queue) {
//            this.queue = queue;
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.broker.BrokerSubscription#connect(org.apache.
//         * activemq.broker.protocol.ProtocolHandler.ConsumerContext)
//         */
//        public void connect(ConsumerContext subscription) throws UserAlreadyConnectedException {
//            this.subscription = subscription;
//            queue.addSubscription(subscription);
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.broker.BrokerSubscription#disconnect(org.apache
//         * .activemq.broker.protocol.ProtocolHandler.ConsumerContext)
//         */
//        public void disconnect(ConsumerContext context) {
//            queue.removeSubscription(subscription);
//        }
//
//        /* (non-Javadoc)
//         * @see org.apache.activemq.broker.BrokerSubscription#getDestination()
//         */
//        public Destination getDestination() {
//            return queue.getDestination();
//        }
//    }

    public void deliver(MessageDelivery message) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean hasSelector() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public boolean matches(MessageDelivery message) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    public void shutdown(RunnableCountDownLatch done) {
    }

    public Destination getDestination() {
        return destination;
    }
}