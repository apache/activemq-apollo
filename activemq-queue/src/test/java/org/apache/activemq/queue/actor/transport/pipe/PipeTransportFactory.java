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
package org.apache.activemq.queue.actor.transport.pipe;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.actor.ActorProxy;
import org.apache.activemq.dispatch.DispatchObject;
import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.queue.actor.transport.Transport;
import org.apache.activemq.queue.actor.transport.TransportFactory;
import org.apache.activemq.queue.actor.transport.TransportHandler;
import org.apache.activemq.queue.actor.transport.TransportServer;
import org.apache.activemq.queue.actor.transport.TransportServerHandler;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PipeTransportFactory implements TransportFactory {

    static protected final HashMap<String, PipeTransportServer> servers = new HashMap<String, PipeTransportServer>();
    
    static void perform_unbind(PipeTransportServer server) {
        synchronized(servers) {
            servers.remove(server.name);
        }
    }
    
    static void perform_bind(PipeTransportServer server) throws IOException {
        synchronized(servers) {
            if (servers.containsKey(server.name)) {
                throw new IOException("Server already bound: " + server.name);
            }
            servers.put(server.name, server);
        }
    }

    static void perform_connect(PipeTransport clientTransport) throws IOException {
        PipeTransportServer server;
        synchronized(servers) {
            server = servers.get(clientTransport.name);
            if( server == null ) {
                throw new IOException("Server not bound: " + clientTransport.name);
            }
        }
        PipeTransport serverTransport = new PipeTransport(server.dispatcher);
        clientTransport.peer = serverTransport.actor;
        serverTransport.peer = clientTransport.actor;
        server.actor.onConnect(serverTransport);
    }
    
    
    public TransportServer bind(Dispatcher dispatcher, String bindUri) {
        String name;
        Map<String, String> options;
        try {
            URI uri = new URI(bindUri);
            name = uri.getHost();
            options = URISupport.parseParamters(uri);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid bind uri: "+e, e);
        }

        PipeTransportServer rc = new PipeTransportServer(dispatcher);
        rc.connectURI = bindUri;
        IntrospectionSupport.setProperties(rc, options);
        if (!options.isEmpty()) {
            throw new IllegalArgumentException("Invalid bind uri parameters: " + options);
        }
        rc.name = name;
        return rc;
    }

    public Transport connect(Dispatcher dispatcher, String connectUri) {
        
        String name;
        Map<String, String> options;
        try {
            URI uri = new URI(connectUri);
            name = uri.getHost();
            options = URISupport.parseParamters(uri);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid connect uri: "+e, e);
        }
        
        PipeTransport rc = new PipeTransport(dispatcher);
        IntrospectionSupport.setProperties(rc, options);
        if (!options.isEmpty()) {
            throw new IllegalArgumentException("Invalid connect uri parameters: " + options);
        }
        rc.connnectAddress = connectUri;
        rc.name = name;
        return rc;

    }
    
    public static class DispatchObjectFilter implements DispatchObject {
        protected DispatchObject next;
        
        public DispatchObjectFilter() {
        }

        public DispatchObjectFilter(DispatchObject next) {
            this.next = next;
        }
        
        public void addShutdownWatcher(Runnable shutdownWatcher) {
            next.addShutdownWatcher(shutdownWatcher);
        }
        public <Context> Context getContext() {
            return next.getContext();
        }
        public DispatchQueue getTargetQueue() {
            return next.getTargetQueue();
        }
        public void release() {
            next.release();
        }
        public void resume() {
            next.resume();
        }
        public void retain() {
            next.retain();
        }
        public <Context> void setContext(Context context) {
            next.setContext(context);
        }
        public void setTargetQueue(DispatchQueue queue) {
            next.setTargetQueue(queue);
        }
        public void suspend() {
            next.suspend();
        }
    }
    
    interface PipeTransportServerActor {
        public void onBind();
        public void onConnect(PipeTransport serverSide);
        public void onUnbind();
    }

    protected class PipeTransportServer extends DispatchObjectFilter implements TransportServer, PipeTransportServerActor {
        
        private final Dispatcher dispatcher;
        private final DispatchQueue dispatchQueue;
        private final PipeTransportServerActor actor;

        protected String connectURI;
        protected TransportServerHandler handler;
        protected String name;
        protected String wireFormat;
        protected WireFormatFactory wireFormatFactory;
        protected boolean marshal;
        
        protected final AtomicInteger suspendCounter = new AtomicInteger();
        protected long connectionCounter;

        public PipeTransportServer(Dispatcher dispatcher) {
            super( dispatcher.createSerialQueue(null) );
            this.dispatcher = dispatcher;
            dispatchQueue = (DispatchQueue) next;
            dispatchQueue.suspend();
            dispatchQueue.addShutdownWatcher(new Runnable() {
                public void run() {
                    perform_unbind(PipeTransportServer.this);
                }
            });
            this.actor = ActorProxy.create(PipeTransportServerActor.class, this, dispatchQueue);
            this.actor.onBind();
        }
        
        public void onBind() {
            try {
                perform_bind(this);
                handler.onBind();
            } catch (IOException e) {
                handler.onFailure(e);
            }
        }

        public void onUnbind() {
            perform_unbind(this);
            handler.onUnbind();
        }
        
        public void setHandler(TransportServerHandler handler) {
            this.handler = handler;
        }

        public void onConnect(PipeTransport serverSide) {
            long connectionId = connectionCounter++;
            serverSide.remoteAddress = connectURI.toString() + "#" + connectionId;
            handler.onAccept(serverSide);
        }

        public String getConnectURI() {
            return connectURI;
        }

        public void setWireFormatFactory(WireFormatFactory wireFormatFactory) {
            this.wireFormatFactory = wireFormatFactory;
        }

        public boolean isMarshal() {
            return marshal;
        }

        public void setMarshal(boolean marshal) {
            this.marshal = marshal;
        }

        public String getWireFormat() {
            return wireFormat;
        }

        public void setWireFormat(String wireFormat) {
            this.wireFormat = wireFormat;
        }

    }

    interface PipeTransportActor {
        public void onConnect();
        public void onDispatch(Object message, Runnable onCompleted, DispatchQueue queue);
        public void onDisconnect();
        public void onFailure(Exception e);
    }

    protected static class PipeTransport extends DispatchObjectFilter implements PipeTransportActor, Transport {

        public String connnectAddress;
        public String remoteAddress;
        private PipeTransportActor actor;
        private PipeTransportActor peer;
        
        private DispatchQueue dispatchQueue;
        private TransportHandler handler;
        private String name;
        private WireFormat wf;
        private String wireFormat;
        private boolean marshal;
        
        protected final AtomicInteger suspendCounter = new AtomicInteger();
        
        public PipeTransport(Dispatcher dispatcher) {
            super( dispatcher.createSerialQueue(null) );
            this.dispatchQueue = (DispatchQueue) next;
            this.dispatchQueue.suspend();
            this.dispatchQueue.addShutdownWatcher(new Runnable() {
                public void run() {
                    PipeTransportActor peer = PipeTransport.this.peer;
                    if( peer!=null ) {
                        peer.onDisconnect();
                        peer = null;
                    }
                }
            });
            
            // Queue up the connect event so it's the first thing that gets executed when
            // this object gets resumed..
            this.actor = ActorProxy.create(PipeTransportActor.class, this, dispatchQueue);
            this.actor.onConnect();
        }
        
        public void setHandler(TransportHandler hanlder) {
            this.handler = hanlder;
        }
        
        public void onConnect() {
            try {
                if (connnectAddress != null) {
                    // Client side connect case...
                    perform_connect(this);
                    remoteAddress = connnectAddress;
                    handler.onConnect();
                } else {
                    // Server side connect case...
                    if( peer==null || remoteAddress==null ) {
                        throw new IOException("Server transport not properly initialized.");
                    }
                    handler.onConnect();
                }
            } catch (IOException e) {
                handler.onFailure(e);
            }
        }

        public void send(Object message) {
            send(message, null, null);
        }

        public void send(Object message, Runnable onCompleted, DispatchQueue queue) {
            try {
                if( peer==null ) {
                    throw new IOException("not connected");
                }
                if (wf != null && marshal) {
                    message = wf.marshal(message);
                }
            } catch (IOException e) {
                actor.onFailure(e);
                complete(onCompleted, queue);
                return;
            }
            peer.onDispatch(message, onCompleted, queue);
        }

        public void onDispatch(Object message, Runnable onCompleted, DispatchQueue queue) {
            try {
                Object m = message;
                if (wf != null && marshal) {
                    try {
                        m = wf.unmarshal((Buffer) m);
                    } catch (IOException e) {
                        handler.onFailure(e);
                        return;
                    }
                }
                handler.onRecevie(m);
            } finally {
                complete(onCompleted, queue);
            }
        }

        private void complete(Runnable onCompleted, DispatchQueue queue) {
            if( onCompleted!=null ) {
                if(queue!=null) {
                    queue.dispatchAsync(onCompleted);
                } else {
                    onCompleted.run();
                }
            }
        }
        
        public void onDisconnect() {
            handler.onDisconnect();
        }
        public void onFailure(Exception e) {
            handler.onFailure(e);
        }

        public String getRemoteAddress() {
            return remoteAddress;
        }

        public void setWireFormat(String wireFormat) {
            this.wireFormat = wireFormat;
        }

        public String getWireFormat() {
            return wireFormat;
        }


    }
}
