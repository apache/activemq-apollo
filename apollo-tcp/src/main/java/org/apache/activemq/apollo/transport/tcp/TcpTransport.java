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
package org.apache.activemq.apollo.transport.tcp;

import org.apache.activemq.apollo.transport.ProtocolCodec;
import org.apache.activemq.apollo.transport.TransportListener;
import org.apache.activemq.apollo.util.JavaBaseService;
import org.apache.activemq.apollo.transport.Transport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.DispatchSource;
import org.fusesource.hawtdispatch.Retained;

import java.io.IOException;
import java.net.*;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Map;

/**
 * An implementation of the {@link org.apache.activemq.apollo.transport.Transport} interface using raw tcp/ip
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TcpTransport extends JavaBaseService implements Transport {

    private static final Logger LOG = LoggerFactory.getLogger(TcpTransport.class);

    private Map<String, Object> socketOptions;

    abstract static class SocketState {
        void onStop(Runnable onCompleted) {
        }
        void onCanceled() {
        }
        boolean is(Class<? extends SocketState> clazz) {
            return getClass()==clazz;
        }
    }

    static class DISCONNECTED extends SocketState{}

    class CONNECTING extends SocketState{
        void onStop(Runnable onCompleted) {
            trace("CONNECTING.onStop");
            CANCELING state = new CANCELING();
            socketState = state;
            state.onStop(onCompleted);
        }
        void onCanceled() {
            trace("CONNECTING.onCanceled");
            CANCELING state = new CANCELING();
            socketState = state;
            state.onCanceled();
        }
    }

    class CONNECTED extends SocketState {
        void onStop(Runnable onCompleted) {
            trace("CONNECTED.onStop");
            CANCELING state = new CANCELING();
            socketState = state;
            state.add(createDisconnectTask());
            state.onStop(onCompleted);
        }
        void onCanceled() {
            trace("CONNECTED.onCanceled");
            CANCELING state = new CANCELING();
            socketState = state;
            state.add(createDisconnectTask());
            state.onCanceled();
        }
        Runnable createDisconnectTask() {
            return new Runnable(){
                public void run() {
                    listener.onTransportDisconnected();
                }
            };
        }
    }

    class CANCELING extends SocketState {
        private LinkedList<Runnable> runnables =  new LinkedList<Runnable>();
        private int remaining;
        private boolean dispose;

        public CANCELING() {
            if( readSource!=null ) {
                remaining++;
                readSource.cancel();
            }
            if( writeSource!=null ) {
                remaining++;
                writeSource.cancel();
            }
        }
        void onStop(Runnable onCompleted) {
            trace("CANCELING.onCompleted");
            add(onCompleted);
            dispose = true;
        }
        void add(Runnable onCompleted) {
            if( onCompleted!=null ) {
                runnables.add(onCompleted);
            }
        }
        void onCanceled() {
            trace("CANCELING.onCanceled");
            remaining--;
            if( remaining!=0 ) {
                return;
            }
            try {
                channel.close();
            } catch (IOException ignore) {
            }
            socketState = new CANCELED(dispose);
            for (Runnable runnable : runnables) {
                runnable.run();
            }
            if (dispose) {
                dispose();
            }
        }
    }

    class CANCELED extends SocketState {
        private boolean disposed;

        public CANCELED(boolean disposed) {
            this.disposed=disposed;
        }

        void onStop(Runnable onCompleted) {
            trace("CANCELED.onStop");
            if( !disposed ) {
                disposed = true;
                dispose();
            }
            onCompleted.run();
        }
    }

    protected URI remoteLocation;
    protected URI localLocation;
    private TransportListener listener;
    private String remoteAddress;
    private ProtocolCodec wireformat;

    private SocketChannel channel;

    private SocketState socketState = new DISCONNECTED();

    private DispatchQueue dispatchQueue;
    private DispatchSource readSource;
    private DispatchSource writeSource;

    protected boolean useLocalHost = true;
    boolean full = false;

    private final Runnable CANCEL_HANDLER = new Runnable() {
        public void run() {
            socketState.onCanceled();
        }
    };

    static final class OneWay {
        final Object command;
        final Retained retained;

        public OneWay(Object command, Retained retained) {
            this.command = command;
            this.retained = retained;
        }
    }

    public void connected(SocketChannel channel) throws IOException {
        this.channel = channel;

        if( wireformat!=null ) {
            wireformat.setReadableByteChannel(this.channel);
            wireformat.setWritableByteChannel(this.channel);
        }

        this.channel.configureBlocking(false);
        this.remoteAddress = channel.socket().getRemoteSocketAddress().toString();
        channel.socket().setSoLinger(true, 0);

        this.socketState = new CONNECTED();
    }

    public void connecting(URI remoteLocation, URI localLocation) throws IOException {
        this.channel = SocketChannel.open();
        this.channel.configureBlocking(false);
        this.remoteLocation = remoteLocation;
        this.localLocation = localLocation;

        if( wireformat!=null ) {
            wireformat.setReadableByteChannel(this.channel);
            wireformat.setWritableByteChannel(this.channel);
        }

        if (localLocation != null) {
            InetSocketAddress localAddress = new InetSocketAddress(InetAddress.getByName(localLocation.getHost()), localLocation.getPort());
            channel.socket().bind(localAddress);
        }

        String host = resolveHostName(remoteLocation.getHost());
        InetSocketAddress remoteAddress = new InetSocketAddress(host, remoteLocation.getPort());
        channel.connect(remoteAddress);
        this.socketState = new CONNECTING();
    }


    public DispatchQueue getDispatchQueue() {
        return dispatchQueue;
    }

    public void setDispatchQueue(DispatchQueue queue) {
        if (dispatchQueue != null) {
            dispatchQueue.release();
        }
        this.dispatchQueue = queue;
        if (dispatchQueue != null) {
            dispatchQueue.retain();
        }
    }

    public void _start(Runnable onCompleted) {
        try {
            if (socketState.is(CONNECTING.class) ) {
                trace("connecting...");
                // this allows the connect to complete..
                readSource = Dispatch.createSource(channel, SelectionKey.OP_CONNECT, dispatchQueue);
                readSource.setEventHandler(new Runnable() {
                    public void run() {
                        if (getServiceState() != STARTED) {
                            return;
                        }
                        try {
                            trace("connected.");
                            channel.finishConnect();
                            readSource.setCancelHandler(null);
                            readSource.release();
                            readSource=null;
                            socketState = new CONNECTED();
                            onConnected();
                        } catch (IOException e) {
                            onTransportFailure(e);
                        }
                    }
                });
                readSource.setCancelHandler(CANCEL_HANDLER);
                readSource.resume();
            } else if (socketState.is(CONNECTED.class) ) {
                trace("was connected.");
                onConnected();
            } else {
                System.err.println("cannot be started.  socket state is: "+socketState); 
            }
        } catch (IOException e) {
            onTransportFailure(e);
        } finally {
            if( onCompleted!=null ) {
                onCompleted.run();
            }
        }
    }

    public void _stop(final Runnable onCompleted) {
        trace("stopping.. at state: "+socketState);
        socketState.onStop(onCompleted);
    }

    protected String resolveHostName(String host) throws UnknownHostException {
        String localName = InetAddress.getLocalHost().getHostName();
        if (localName != null && isUseLocalHost()) {
            if (localName.equals(host)) {
                return "localhost";
            }
        }
        return host;
    }

    private void onConnected() throws SocketException {

        readSource = Dispatch.createSource(channel, SelectionKey.OP_READ, dispatchQueue);
        writeSource = Dispatch.createSource(channel, SelectionKey.OP_WRITE, dispatchQueue);

        readSource.setCancelHandler(CANCEL_HANDLER);
        writeSource.setCancelHandler(CANCEL_HANDLER);

        readSource.setEventHandler(new Runnable() {
            public void run() {
                drainInbound();
            }
        });
        writeSource.setEventHandler(new Runnable() {
            public void run() {
                drainOutbound();
            }
        });

        remoteAddress = channel.socket().getRemoteSocketAddress().toString();
        listener.onTransportConnected();
    }



    private void dispose() {

        if( readSource!=null ) {
            readSource.release();
            readSource=null;
        }

        if( writeSource!=null ) {
            writeSource.release();
            writeSource=null;
        }
        
        dispatchQueue.release();
        this.wireformat = null;
    }

    public void onTransportFailure(IOException error) {
        listener.onTransportFailure(error);
        socketState.onCanceled();
    }


    public boolean full() {
        return full;
    }

    public boolean offer(Object command) {
        assert Dispatch.getCurrentQueue() == dispatchQueue;
        try {
            if (!socketState.is(CONNECTED.class)) {
                throw new IOException("Not connected.");
            }
            if (getServiceState() != STARTED) {
                throw new IOException("Not running.");
            }

            ProtocolCodec.BufferState rc = wireformat.write(command);
            switch (rc ) {
                case FULL:
                    return false;
                case WAS_EMPTY:
                    writeSource.resume();
                default:
                    return true;
            }
        } catch (IOException e) {
            onTransportFailure(e);
            return false;
        }

    }

    /**
     * @retruns true if there are no in progress writes.
     */
    private void drainOutbound() {
        assert Dispatch.getCurrentQueue() == dispatchQueue;
        if (getServiceState() != STARTED || !socketState.is(CONNECTED.class)) {
            return;
        }
        try {
            if( wireformat.flush() == ProtocolCodec.BufferState.EMPTY ) {
                writeSource.suspend();
                listener.onRefill();
            }
        } catch (IOException e) {
            onTransportFailure(e);
        }
    }

    private void drainInbound() {
        if (!getServiceState().isStarted() || readSource.isSuspended()) {
            return;
        }
        try {
            Object command = wireformat.read();
            while ( command!=null ) {
                try {
                    listener.onTransportCommand(command);
                } catch (Throwable e) {
                    e.printStackTrace();
                    onTransportFailure(new IOException("Transport listener failure."));
                }

                // the transport may be suspended after processing a command.
                if (getServiceState() == STOPPED || readSource.isSuspended()) {
                    return;
                }

                command = wireformat.read();
            }
        } catch (IOException e) {
            onTransportFailure(e);
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

    private boolean assertConnected() {
        try {
            if ( !isConnected() ) {
                throw new IOException("Not connected.");
            }
            return true;
        } catch (IOException e) {
            onTransportFailure(e);
        }
        return false;
    }

    public void suspendRead() {
        if( isConnected() && readSource!=null ) {
            readSource.suspend();
        }
    }


    public void resumeRead() {
        if( isConnected() && readSource!=null ) {
            readSource.resume();
            dispatchQueue.execute(new Runnable(){
                public void run() {
                    drainInbound();
                }
            });
        }
    }

    public String getTypeId() {
        return "tcp";
    }

    public void reconnect(URI uri) {
        throw new UnsupportedOperationException();
    }

    public TransportListener getTransportListener() {
        return listener;
    }

    public void setTransportListener(TransportListener listener) {
        this.listener = listener;
    }

    public ProtocolCodec getProtocolCodec() {
        return wireformat;
    }

    public void setProtocolCodec(ProtocolCodec protocolCodec) {
        this.wireformat = protocolCodec;
        if( channel!=null ) {
            protocolCodec.setReadableByteChannel(this.channel);
            protocolCodec.setWritableByteChannel(this.channel);
        }
    }

    public boolean isConnected() {
        return socketState.is(CONNECTED.class);
    }

    public boolean isDisposed() {
        return getServiceState() == STOPPED;
    }

    public boolean isFaultTolerant() {
        return false;
    }

    public void setSocketOptions(Map<String, Object> socketOptions) {
        this.socketOptions = socketOptions;
    }

    public boolean isUseLocalHost() {
        return useLocalHost;
    }

    /**
     * Sets whether 'localhost' or the actual local host name should be used to
     * make local connections. On some operating systems such as Macs its not
     * possible to connect as the local host name so localhost is better.
     */
    public void setUseLocalHost(boolean useLocalHost) {
        this.useLocalHost = useLocalHost;
    }


    private void trace(String message) {
        if( LOG.isTraceEnabled() ) {
            final String label = dispatchQueue.getLabel();
            if( label !=null ) {
                LOG.trace(label +" | "+message);
            } else {
                LOG.trace(message);
            }
        }
    }

}
