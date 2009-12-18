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
import org.apache.activemq.flow.Commands.FlowControl;
import org.apache.activemq.queue.actor.transport.Transport;
import org.apache.activemq.util.IntrospectionSupport;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class BrokerConnection extends BaseConnection implements DeliveryTarget {
    
    interface BrokerConnectionStateActor extends ConnectionStateActor {
        public void onBrokerDispatch(Message msg, Runnable r);
    }
    
    public static class DispatchRequest {

        private final Message message;
        private final Runnable onComplete;

        public DispatchRequest(Message message, Runnable onComplete) {
            this.message = message;
            this.onComplete = onComplete;
        }
        
        @Override
        public String toString() {
            return IntrospectionSupport.toString(this);
        }
    }
    
    private MockBroker broker;
    private BrokerConnectionStateActor brokerActor;
    private Transport transport;
    private int priorityLevels;

    protected void createActor() {
        actor = brokerActor = ActorProxy.create(BrokerConnectionStateActor.class, new BrokerConnectionState(), dispatchQueue);
    }

    protected class BrokerConnectionState extends ConnectionState implements BrokerConnectionStateActor {

        String name;
        
        @Override
        public void onStart() {
            this.transport = BrokerConnection.this.transport;
            super.onStart();
        }
        
        // TODO: to increase fairness: we might want to have a pendingQueue per sender
        final LinkedList<DispatchRequest> pendingQueue = new LinkedList<DispatchRequest>(); 
        final LinkedList<Runnable> dispatchedQueue = new LinkedList<Runnable>(); 
        
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
            broker.router.route(msg, 
                    dispatchQueue, new Runnable() {
                public void run() {
                    BrokerConnectionState.super.onReceiveMessage(msg);
                }
                
                @Override
                public String toString() {
                    return IntrospectionSupport.toString(BrokerConnectionState.this, "name", "inboundSessionWindow");
                }
            });
        }
        
        @Override
        protected void onReceiveDestination(Destination destination) {
            broker.subscribe(destination, BrokerConnection.this);
        }

        public void onBrokerDispatch(Message message, Runnable onComplete) {
            pendingQueue.add(new DispatchRequest(message, onComplete));
            dispatchPendingQueue();
        }
        
        @Override
        protected void onSessionResume() {
            dispatchPendingQueue();
        }

        private void dispatchPendingQueue() {
            while( !isSessionSendBlocked() ) {
                DispatchRequest request = pendingQueue.poll();
                if( request==null ) {
                    return;
                }
                dispatchedQueue.add(request.onComplete);
                sessionSend(request.message);
            }
        }
     
        @Override
        protected void onReceiveFlowControl(FlowControl command) {
            super.onReceiveFlowControl(command);
            int credit = command.getCredit();
            for( int i=0; i < credit; i++) {
                Runnable onComplete = dispatchedQueue.poll();
                if( onComplete!=null ) {
                    onComplete.run();
                }
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
