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

import org.apache.activemq.filter.BooleanExpression;
import org.apache.activemq.filter.FilterException;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.queue.Subscription;

public class TopicSubscription implements BrokerSubscription, DeliveryTarget {

    protected final BooleanExpression selector;
    protected final Destination destination;
    protected Subscription<MessageDelivery> connectedSub;
    private final VirtualHost host;

    TopicSubscription(VirtualHost host, Destination destination, BooleanExpression selector) {
       this.host = host;
       this.selector = selector;
       this.destination = destination;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.broker.DeliveryTarget#deliver(org.apache.activemq.broker.MessageDelivery, org.apache.activemq.flow.ISourceController)
     */
    public final void deliver(MessageDelivery message, ISourceController<?> source) {
        connectedSub.add(message, source, null);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.DeliveryTarget#hasSelector()
     */
    public boolean hasSelector() {
        return selector != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.broker.BrokerSubscription#connect(org.apache.activemq
     * .broker.protocol.ProtocolHandler.ConsumerContext)
     */
    public synchronized void connect(Subscription<MessageDelivery> subsription) throws UserAlreadyConnectedException {
        connectedSub = subsription;
        host.getRouter().bind(destination, this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.broker.BrokerSubscription#disconnect(org.apache.activemq
     * .broker.protocol.ProtocolHandler.ConsumerContext)
     */
    public synchronized void disconnect(Subscription<MessageDelivery> context) {
        host.getRouter().unbind(destination, this);
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
}
