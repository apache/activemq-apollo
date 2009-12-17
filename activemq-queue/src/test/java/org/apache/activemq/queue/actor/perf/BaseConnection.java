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

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.dispatch.internal.BaseRetained;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.flow.Commands.FlowControl;
import org.apache.activemq.flow.Commands.Destination.DestinationBean;
import org.apache.activemq.flow.Commands.Destination.DestinationBuffer;
import org.apache.activemq.flow.Commands.FlowControl.FlowControlBean;
import org.apache.activemq.flow.Commands.FlowControl.FlowControlBuffer;
import org.apache.activemq.queue.actor.transport.Transport;
import org.apache.activemq.queue.actor.transport.TransportHandler;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract public class BaseConnection extends BaseRetained {

    protected interface Protocol extends TransportHandler {
        void start();
        void shutdown(Runnable onShutdown);
    }
    
    protected String name;
    protected Dispatcher dispatcher;

    protected DispatchQueue dispatchQueue;
    protected Protocol actor;

    
    @Override
    protected void startup() {
        super.startup();
        dispatchQueue = dispatcher.createSerialQueue(name);
        createActor();
        actor.start();
    }

    @Override
    protected void shutdown() {
        actor.shutdown(new Runnable() {
            public void run() {
                // notifies registered shutdown handlers 
                BaseConnection.super.shutdown();
            }
        });
    }
    
    public static class WindowController extends WindowLimiter {

        private int maxSize;
        private int processed;
        private int creditsAt;
        
        public int processed(int count) {
            int rc = 0;
            processed += count;
            if( processed >= creditsAt ) {
                change(processed);
                rc = processed;
                processed = 0;
            }
            return rc;
        }
        
        int maxSize(int newMaxSize) {
            int change = newMaxSize-maxSize;
            this.maxSize=newMaxSize;
            this.creditsAt = maxSize/2;
            change(change);
            return change;
        }
        
        int maxSize() {
            return maxSize;
        }

    }
    
    public static class WindowLimiter {

        private int opensAt = 1;
        private int size;
        private boolean closed;
        
        public WindowLimiter() {
            this.closed = true;
        }

        int size() {
            return size;
        }
        
        WindowLimiter size(int size) {
            this.size = size;
            return this;
        }
        
        public boolean isOpen() {
            return !closed;
        }
        
        public boolean isClosed() {
            return closed;
        }
        
        public void change(int change) {
            size += change;
            if( change > 0 && closed && size >= opensAt) {
                closed = false;
            } else if( change < 0 && !closed && size <= 0) {
                closed = true;
            }
        }

    }
    
    abstract protected void createActor();
    
    // The actor pattern ensures that this object is only accessed in
    // serial execution context.  So synchronization is required.
    // It also places a restriction that all operations should 
    // avoid mutex contention and avoid blocking IO calls.
    protected class ProtocolImpl implements Protocol {
        
        final protected WindowController inboundSessionWindow = new WindowController();
        final protected WindowLimiter outboundSessionWindow = new WindowLimiter();
        final protected WindowLimiter outboundTransportWindow = new WindowLimiter();

        protected Transport transport;
        protected Runnable onShutdown;
        protected boolean disconnected;
        protected Exception failure;

        ProtocolImpl() {
            outboundTransportWindow.size(100);
        }
        
        public void start() {
            
            transport.setTargetQueue(dispatchQueue);
            transport.setHandler(this);
            transport.resume();
        }
        
        public void shutdown(Runnable onShutdown) {
            if( disconnected ) {
                onShutdown.run();
            } else {
                this.onShutdown = onShutdown;
                transport.release();
            }
        }

        public void onConnect() {
            sendFlowControl(inboundSessionWindow.maxSize(1000));
        }
        
        public void onDisconnect() {
            disconnected = true;
            if( onShutdown!=null ) {
                shutdown(onShutdown);
                onShutdown=null;
            }
        }

        public void onFailure(Exception failure) {
            failure.printStackTrace();
            this.failure = failure;
        }

        public void onRecevie(Object command) {
            if (command.getClass() == Message.class) {
                // We should not be getting messages
                // when the window is closed..
                if( inboundSessionWindow.isClosed() ) {
                    onFailure(new Exception("Session overrun: " + command));
                }
                outboundSessionWindow.change(-1);
                onReceiveMessage((Message) command);
            } else if (command.getClass() == FlowControlBean.class || command.getClass() == FlowControlBuffer.class) {
                onReceiveFlowControl((FlowControl) command);
            } else if (command.getClass() == String.class) {
                onReceiveString((String)command);
            } else if (command.getClass() == DestinationBuffer.class || command.getClass() == DestinationBean.class) {
                onReceiveDestination((Destination)command);
            } else {
                onFailure(new Exception("Unrecognized command: " + command));
            }
        }

        public void sessionSend(Message message) {
            transportSend(message);
        }
        
        protected void onReceiveDestination(Destination command) {
        }

        protected void onReceiveString(String command) {
        }

        protected void onReceiveMessage(Message msg) {
            sendFlowControl(inboundSessionWindow.processed(1));
        }

        private void sendFlowControl(int credits) {
            if( credits!=0 ) {
                FlowControlBean fc = new FlowControlBean();
                fc.setCredit(credits);
                transportSend(fc);
            }
        }
        
        public void transportSend(Object message) {
            outboundTransportWindow.change(-1);
            transport.send(message, onSendCompleted, dispatchQueue);
        }
        
        private final Runnable onSendCompleted = new Runnable() {
            public void run() {
                boolean wasClosed = outboundTransportWindow.isClosed();
                outboundTransportWindow.change(1);
                if( !wasClosed && !isSessionSendBlocked() ) {
                    onSessionResume();
                }
            }
        };
        
        protected void onReceiveFlowControl(FlowControl command) {
            boolean wasClosed = outboundSessionWindow.isClosed();
            outboundSessionWindow.change(command.getCredit());
            if( wasClosed && !isSessionSendBlocked() ) {
                onSessionResume();
            }

        }

        protected boolean isSessionSendBlocked() {
            return outboundTransportWindow.isClosed() || outboundSessionWindow.isClosed(); 
        }

        protected void onSessionResume() {
        }
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    public void setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }
    
}
