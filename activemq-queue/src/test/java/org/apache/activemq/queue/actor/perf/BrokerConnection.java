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
package org.apache.activemq.queue.actor.perf;

import java.util.LinkedList;

import org.apache.activemq.actor.ActorProxy;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.queue.actor.transport.Transport;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class BrokerConnection extends BaseConnection implements DeliveryTarget {
    
    interface BrokerProtocol extends Protocol {
        public void onBrokerDispatch(Message msg, Runnable r);
    }
    
    public static class DispatchRequest {

        private final Message message;
        private final Runnable onComplete;

        public DispatchRequest(Message message, Runnable onComplete) {
            this.message = message;
            this.onComplete = onComplete;
        }
        
    }
    
    private MockBroker broker;
    private BrokerProtocol brokerActor;
    private Transport transport;
    private int priorityLevels;

    protected void createActor() {
        actor = brokerActor = ActorProxy.create(BrokerProtocol.class, new BrokerProtocolImpl(), dispatchQueue);
    }

    protected class BrokerProtocolImpl extends ProtocolImpl implements BrokerProtocol {

        String name;
        
        @Override
        public void start() {
            this.transport = BrokerConnection.this.transport;
            super.start();
        }
        
        // TODO: to increase fairness: we might want to have a pendingQueue per sender
        final LinkedList<DispatchRequest> pendingQueue = new LinkedList<DispatchRequest>(); 
        
        @Override
        protected void onReceiveString(String remoteName) {
            name = "broker->"+remoteName;
        }
        
        @Override
        protected void onReceiveMessage(final Message msg) {
            // We don't dish out flow control credit until the broker
            // lets us know that the message routing completed.
            // In the slow consumer case, it could take a while for him
            // to complete the routing and we don't want to have th producer
            // send us more messages than the max session protocol window
            // is configured with.
            broker.router.route(msg, dispatchQueue, new Runnable() {
                public void run() {
                    BrokerProtocolImpl.super.onReceiveMessage(msg);
                }
            });
        }
        
        @Override
        protected void onReceiveDestination(Destination destination) {
            broker.subscribe(destination, BrokerConnection.this);
        }

        public void onBrokerDispatch(Message message, Runnable onComplete) {
            if( !isSessionSendBlocked() ) {
                sessionSend(message);
                onComplete.run();
            } else {
                pendingQueue.add(new DispatchRequest(message, onComplete));
            }
        }
        
        @Override
        protected void onSessionResume() {
            while( !isSessionSendBlocked() ) {
                DispatchRequest request = pendingQueue.poll();
                if( request==null ) {
                    return;
                }
                sessionSend(request.message);
                request.onComplete.run();
            }
        }
        
    }

    public void add(Message msg, Runnable r) {
        brokerActor.onBrokerDispatch(msg, r);
    }

    public boolean hasSelector() {
        return false;
    }

    public boolean match(Message message) {
        return true;
    }

    public MockBroker getBroker() {
        return broker;
    }

    public void setBroker(MockBroker broker) {
        this.broker = broker;
    }

    public Transport getTransport() {
        return transport;
    }

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public int getPriorityLevels() {
        return priorityLevels;
    }

    public void setPriorityLevels(int priorityLevels) {
        this.priorityLevels = priorityLevels;
    }
    
}
