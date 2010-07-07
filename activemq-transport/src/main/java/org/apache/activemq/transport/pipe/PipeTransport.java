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
package org.apache.activemq.transport.pipe;

import org.apache.activemq.transport.CompletionCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.wireformat.WireFormat;
import org.fusesource.hawtdispatch.CustomDispatchSource;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.EventAggregators;

import java.io.EOFException;
import java.io.IOException;
import java.net.URI;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PipeTransport implements Transport {
    static private final Object EOF_TOKEN = new Object();

    final private PipeTransportServer server;
    PipeTransport peer;
    private TransportListener listener;
    private String remoteAddress;
    private AtomicBoolean stopping = new AtomicBoolean();
    private String name;
    private WireFormat wireformat;
    private boolean marshal;
    private boolean trace;

    private DispatchQueue dispatchQueue;
    private CustomDispatchSource<Object,LinkedList<Object>> dispatchSource;
    private boolean connected;

    public PipeTransport(PipeTransportServer server) {
        this.server = server;
    }

    public DispatchQueue getDispatchQueue() {
        return dispatchQueue;
    }
    public void setDispatchQueue(DispatchQueue queue) {
        if( dispatchQueue!=null ) {
            dispatchQueue.release();
        }
        this.dispatchQueue = queue;
        if( dispatchQueue!=null ) {
            dispatchQueue.retain();
        }
    }

    public void start() throws Exception {
        if (dispatchQueue == null) {
            throw new IllegalArgumentException("dispatchQueue is not set");
        }
        server.dispatchQueue.dispatchAsync(new Runnable(){
            public void run() {
                dispatchSource = Dispatch.createSource(EventAggregators.linkedList(), dispatchQueue);
                dispatchSource.setEventHandler(new Runnable(){
                    public void run() {
                        try {
                            final LinkedList<Object> commands = dispatchSource.getData();
                            for (Object o : commands) {

                                if(o == EOF_TOKEN) {
                                    throw new EOFException();
                                }

                                if (wireformat != null && marshal) {
                                    listener.onTransportCommand(wireformat.unmarshal((Buffer) o));
                                } else {
                                    listener.onTransportCommand(o);
                                }
                            }

                            // let the peer know that they have been processed.
                            peer.dispatchQueue.dispatchAsync(new Runnable() {
                                public void run() {
                                    outbound -= commands.size();
                                    drainInbound();
                                }
                            });
                        } catch (IOException e) {
                            listener.onTransportFailure(e);
                        }

                    }
                });
                if( peer.dispatchSource != null ) {
                    fireConnected();
                    peer.fireConnected();
                }
            }
        });
    }

    private void fireConnected() {
        dispatchQueue.dispatchAsync(new Runnable() {
            public void run() {
                connected = true;
                dispatchSource.resume();
                listener.onTransportConnected();
                drainInbound();
            }
        });
    }

    public void stop() throws Exception {
        if( connected ) {
            peer.dispatchSource.merge(EOF_TOKEN);
        }
        if( dispatchSource!=null ) {
            dispatchSource.release();
            dispatchSource = null;
        }
        setDispatchQueue(null);
    }

    static final class OneWay {
        final Object command;
        final CompletionCallback callback;

        public OneWay(Object command, CompletionCallback callback) {
            this.callback = callback;
            this.command = command;
        }
    }

    final LinkedList<OneWay> inbound = new LinkedList<OneWay>();
    int outbound = 0;
    int maxOutbound = 100;

    @Deprecated
    public void oneway(Object command) {
        oneway(command, null);
    }

    public void oneway(Object command, CompletionCallback callback) {
        if( !connected ) {
            throw new IllegalStateException("Not connected.");
        }
        if( outbound < maxOutbound ) {
            transmit(command, callback);
        } else {
            inbound.add(new OneWay(command, callback));
        }
    }

    private void drainInbound() {
        while( outbound < maxOutbound && !inbound.isEmpty() ) {
            OneWay oneWay = inbound.poll();
            transmit(oneWay.command, oneWay.callback);
        }
    }

    private void transmit(Object command, CompletionCallback callback) {
        outbound++;
        peer.dispatchSource.merge(command);
        if( callback!=null ) {
            callback.onCompletion();
        }
    }

    public String getRemoteAddress() {
        return remoteAddress;
    }

    public <T> T narrow(Class<T> target) {
        if (target.isAssignableFrom(getClass())) {
            return target.cast(this);
        }
        return null;
    }

    public void suspendRead() {
        dispatchSource.suspend();
    }

    public void resumeRead() {
        dispatchSource.resume();
    }
    public void reconnect(URI uri, CompletionCallback callback) {
        throw new UnsupportedOperationException();
    }

    public void setRemoteAddress(String remoteAddress) {
        this.remoteAddress = remoteAddress;
        if (name == null) {
            name = remoteAddress;
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    public TransportListener getTransportListener() {
        return listener;
    }
    public void setTransportListener(TransportListener listener) {
        this.listener = listener;
    }

    public WireFormat getWireformat() {
        return wireformat;
    }
    public void setWireformat(WireFormat wireformat) {
        this.wireformat = wireformat;
    }


    public boolean isTrace() {
        return trace;
    }

    public void setTrace(boolean trace) {
        this.trace = trace;
    }

    public boolean isMarshal() {
        return marshal;
    }
    public void setMarshal(boolean marshall) {
        this.marshal = marshall;
    }

    public boolean isConnected() {
        return !stopping.get();
    }
    public boolean isDisposed() {
        return false;
    }
    public boolean isFaultTolerant() {
        return false;
    }
}
