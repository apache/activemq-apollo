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
import org.apache.activemq.flow.Flow;
import org.apache.activemq.flow.IFlowLimiter;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.flow.SizeLimiter;
import org.apache.activemq.queue.ExclusivePersistentQueue;
import org.apache.activemq.queue.ExclusiveQueue;
import org.apache.activemq.queue.IFlowQueue;
import org.apache.activemq.queue.QueueDispatchTarget;
import org.apache.activemq.queue.Subscription;
import org.apache.activemq.util.IntrospectionSupport;

class TopicSubscription implements BrokerSubscription, DeliveryTarget {

	static final boolean USE_PERSISTENT_QUEUES = true; 
	
    protected final BooleanExpression selector;
    protected final Destination destination;
    protected Subscription<MessageDelivery> connectedSub;
    private final VirtualHost host;
    
    //TODO: replace this with a base interface for queue which also support non persistent use case.
	private IFlowQueue<MessageDelivery> queue;

    TopicSubscription(VirtualHost host, Destination destination, BooleanExpression selector) {
        this.host = host;
        this.selector = selector;
        this.destination = destination;
    }

    @Override
    public String toString() {
        return IntrospectionSupport.toString(this);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.activemq.broker.DeliveryTarget#deliver(org.apache.activemq
     * .broker.MessageDelivery, org.apache.activemq.flow.ISourceController)
     */
    public final void deliver(MessageDelivery message, ISourceController<?> source) {
        if (matches(message)) {
            queue.add(message, source);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.DeliveryTarget#hasSelector()
     */
    public boolean hasSelector() {
        return selector != null;
    }

    public synchronized void connect(final ConsumerContext subscription) throws UserAlreadyConnectedException {
        if (this.connectedSub == null) {
        	if( subscription.isPersistent() ) {
        		queue = createPersistentQueue(subscription);
        	} else {
        		queue = createNonPersistentQueue(subscription);
        	}
    		queue.start();
        	
        	this.connectedSub = subscription;
        	this.queue.addSubscription(connectedSub);
    		this.host.getRouter().bind(destination, this);
        } else if (connectedSub != subscription) {
            throw new UserAlreadyConnectedException();
        }
    }

    private IFlowQueue<MessageDelivery> createNonPersistentQueue(final ConsumerContext subscription) {
		Flow flow = new Flow(subscription.getResourceName(), false);
		String name = subscription.getResourceName();
		IFlowLimiter<MessageDelivery> limiter = new SizeLimiter<MessageDelivery>(100, 50);
		ExclusiveQueue<MessageDelivery> queue = new ExclusiveQueue<MessageDelivery>(flow, name, limiter);
		queue.setDrain( new QueueDispatchTarget<MessageDelivery>() {
            public void drain(MessageDelivery elem, ISourceController<MessageDelivery> controller) {
                subscription.add(elem, controller);
            }
        });
		return queue;
	}

	private IFlowQueue<MessageDelivery> createPersistentQueue(ConsumerContext subscription) {
        ExclusivePersistentQueue<Long, MessageDelivery> queue = host.getQueueStore().createExclusivePersistentQueue();
        return queue;
	}

    @SuppressWarnings("unchecked")
	private void destroyPersistentQueue(IFlowQueue<MessageDelivery> queue) {
    	ExclusivePersistentQueue<Long, MessageDelivery> pq = (ExclusivePersistentQueue<Long, MessageDelivery>) queue;
		host.getQueueStore().deleteQueue(pq.getDescriptor());
	}

	public synchronized void disconnect(final ConsumerContext subscription) {
        if (connectedSub != null && connectedSub == subscription) {
    		this.host.getRouter().unbind(destination, this);
    		this.queue.removeSubscription(connectedSub);
    		this.connectedSub = null;
    		
    		queue.stop();
        	if( USE_PERSISTENT_QUEUES ) {
        		destroyPersistentQueue(queue);
        	}
    		queue=null;
        }
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

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.broker.BrokerSubscription#getDestination()
     */
    public Destination getDestination() {
        return destination;
    }
}
