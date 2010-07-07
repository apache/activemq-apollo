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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.actor.ActorProxy;
import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.queue.actor.perf.DispatchObjectFilter;
import org.apache.activemq.queue.actor.transport.Transport;
import org.apache.activemq.queue.actor.transport.TransportFactory;
import org.apache.activemq.queue.actor.transport.TransportHandler;
import org.apache.activemq.queue.actor.transport.TransportServer;
import org.apache.activemq.queue.actor.transport.TransportServerHandler;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.fusesource.hawtbuf.buffer.Buffer;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PipeTransportFactory implements TransportFactory {

    static protected final HashMap<String, PipeTransportServer> servers = new HashMap<String, PipeTransportServer>();
    static final AtomicLong CONNECTION_IDS = new AtomicLong();

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
        
        long connectionId = clientTransport.connectionId;
        String remoteAddress = clientTransport.connnectAddress+":server:"+connectionId;
        PipeTransport serverTransport = new PipeTransport(server.dispatcher, connectionId, remoteAddress);
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

        PipeTransportServer rc = new PipeTransportServer(dispatcher, name);
        rc.connectURI = bindUri;
        IntrospectionSupport.setProperties(rc, options);
        if (!options.isEmpty()) {
            throw new IllegalArgumentException("Invalid bind uri parameters: " + options);
        }
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
        
        long connectionId = CONNECTION_IDS.incrementAndGet();
        String remoteAddress = connectUri+":client:"+connectionId;
        PipeTransport rc = new PipeTransport(dispatcher, connectionId, remoteAddress);
        IntrospectionSupport.setProperties(rc, options);
        if (!options.isEmpty()) {
            throw new IllegalArgumentException("Invalid connect uri parameters: " + options);
        }
        rc.connnectAddress = connectUri;
        rc.name = name;
        return rc;

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

        public PipeTransportServer(Dispatcher dispatcher, String name) {
            super( dispatcher.createSerialQueue(name) );
            this.dispatcher = dispatcher;
            this.name=name;
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
        
        protected final AtomicBoolean connected = new AtomicBoolean();
        protected final AtomicInteger suspendCounter = new AtomicInteger();
        private final long connectionId;

        final protected AtomicInteger retained = new AtomicInteger(1);

        public PipeTransport(Dispatcher dispatcher, long connectionId, String remoteAddress) {
            super( dispatcher.createSerialQueue(remoteAddress) );
            this.connectionId = connectionId;
            this.dispatchQueue = (DispatchQueue) next;
            this.dispatchQueue.suspend();
            this.remoteAddress = remoteAddress;
            
            // Queue up the connect event so it's the first thing that gets executed when
            // this object gets resumed..
            this.actor = ActorProxy.create(PipeTransportActor.class, this, dispatchQueue);
            this.actor.onConnect();
        }
        
        public void retain() {
            retained.getAndIncrement();
        }

        public void release() {
            if (retained.decrementAndGet() == 0) {
                PipeTransportActor peer = PipeTransport.this.peer;
                if( peer!=null ) {
                    peer.onDisconnect();
                }
                dispatchQueue.dispatchAsync(new Runnable(){
                    public void run() {
                        handler = null;
                    }
                });
                dispatchQueue.release();                
            }
        }
        
        public void setHandler(TransportHandler hanlder) {
            this.handler = hanlder;
        }
        
        public void onConnect() {
            try {
                if( !connected.compareAndSet(false, true) ) {
                    throw new IOException("allready connected.");
                }
                
                if (connnectAddress != null) {
                    // Client side connect case...
                    perform_connect(this);
                    if( handler!=null ) {
                        handler.onConnect();
                    }
                } else {
                    // Server side connect case...
                    if( peer==null ) {
                        throw new IOException("Server transport not properly initialized.");
                    }
                    if( handler!=null ) {
                        handler.onConnect();
                    }
                }
                dispatchQueue.retain();
            } catch (IOException e) {
                onFailure(e);
            }
        }
        
        public void onDisconnect() {
            if( !connected.compareAndSet(true, false) ) {
                throw new AssertionError("Was not connected.");
            }
            if( handler!=null ) {
                handler.onDisconnect();
            }
            dispatchQueue.release();
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
            if( queue!=null ) {
                queue.retain();
            }
            peer.onDispatch(message, onCompleted, queue);
        }

        public void onDispatch(Object message, Runnable onCompleted, DispatchQueue queue) {
            try {
                if( handler!=null ) {
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
                }
            } finally {
                complete(onCompleted, queue);
            }
        }

        private void complete(Runnable onCompleted, DispatchQueue queue) {
            if( onCompleted!=null ) {
                if(queue!=null) {
                    queue.dispatchAsync(onCompleted);
                    if( queue!=null ) {
                        queue.release();
                    }
                } else {
                    onCompleted.run();
                }
            }
        }
        
        public void onFailure(Exception e) {
            if( handler!=null ) {
                handler.onFailure(e);
            }
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
