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
import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.FilterException;
import org.apache.activemq.filter.MessageEvaluationContext;

public class DurableSubscription implements BrokerSubscription, DeliveryTarget {

//    private final IQueue<Long, MessageDelivery> queue;
    private final VirtualHost host;
    private final Destination destination;
//    private Subscription<MessageDelivery> connectedSub;
    boolean started = false;
    BooleanExpression selector;

    DurableSubscription(VirtualHost host, Destination destination, BooleanExpression selector) {
        this.host = host;
//        this.queue = queue;
        this.destination = destination;
        this.selector = selector;
        //TODO If a durable subscribes to a queue 
        this.host.getRouter().bind(destination, this);
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.broker.BrokerSubscription#getDestination()
     */
    public Destination getDestination() {
        return destination;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.broker.DeliveryTarget#deliver(org.apache.activemq.broker.MessageDelivery, org.apache.activemq.flow.ISourceController)
     */
    public void deliver(MessageDelivery message) {
//        TODO:
//        queue.add(message, source);
    }

    public synchronized void connect(final ConsumerContext subscription) throws UserAlreadyConnectedException {
//        TODO:
//        if (this.connectedSub == null) {
//            this.connectedSub = subscription;
//            queue.addSubscription(connectedSub);
//        } else if (connectedSub != subscription) {
//            throw new UserAlreadyConnectedException();
//        }
    }

    public synchronized void disconnect(final ConsumerContext subscription) {
//        TODO:
//        if (connectedSub != null && connectedSub == subscription) {
//            queue.removeSubscription(connectedSub);
//            connectedSub = null;
//        }
    }

    public boolean matches(MessageDelivery message) {
        if (selector == null) {
            return true;
        }

        MessageEvaluationContext selectorContext = message.createMessageEvaluationContext();
        selectorContext.setDestination(destination);
        try {
            return (selector.matches(selectorContext));
        } catch (FilterException e) {
            e.printStackTrace();
            return false;
        }
    }

    public boolean hasSelector() {
        return selector != null;
    }
}
