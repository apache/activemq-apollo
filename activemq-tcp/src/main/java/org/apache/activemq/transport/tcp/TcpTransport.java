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
package org.apache.activemq.transport.tcp;

import org.apache.activemq.apollo.util.BaseService;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.fusesource.hawtbuf.DataByteArrayOutputStream;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.DispatchSource;
import org.fusesource.hawtdispatch.Retained;

import java.io.EOFException;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.Map;

/**
 * An implementation of the {@link Transport} interface using raw tcp/ip
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TcpTransport extends BaseService implements Transport {

    private static final Log LOG = LogFactory.getLog(TcpTransport.class);

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
    private WireFormat wireformat;

    private SocketChannel channel;

    private SocketState socketState = new DISCONNECTED();

    private DispatchQueue dispatchQueue;
    private DispatchSource readSource;
    private DispatchSource writeSource;

    int bufferSize = 1024*64;

    DataByteArrayOutputStream next_outbound_buffer;
    ByteBuffer outbound_buffer;
    protected boolean useLocalHost = true;
    ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);
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
        this.channel.configureBlocking(false);
        this.remoteAddress = channel.socket().getRemoteSocketAddress().toString();
        this.socketState = new CONNECTED();
    }

    public void connecting(URI remoteLocation, URI localLocation) throws IOException {
        this.channel = SocketChannel.open();
        this.channel.configureBlocking(false);
        this.remoteLocation = remoteLocation;
        this.localLocation = localLocation;

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

        channel.socket().setSendBufferSize(bufferSize);
        channel.socket().setReceiveBufferSize(bufferSize);

        next_outbound_buffer = new DataByteArrayOutputStream(bufferSize);
        outbound_buffer = ByteBuffer.allocate(0);

        readSource = Dispatch.createSource(channel, SelectionKey.OP_READ, dispatchQueue);
        writeSource = Dispatch.createSource(channel, SelectionKey.OP_WRITE, dispatchQueue);

        readSource.setCancelHandler(CANCEL_HANDLER);
        writeSource.setCancelHandler(CANCEL_HANDLER);

        readSource.setEventHandler(new Runnable() {
            public void run() {
                try {
                    drainInbound();
                } catch (IOException e) {
                    onTransportFailure(e);
                }
            }
        });
        writeSource.setEventHandler(new Runnable() {
            public void run() {
                if (getServiceState() == STARTED) {
                    // once the outbound is drained.. we can suspend getting
                    // write events.
                    if (drainOutbound()) {
                        writeSource.suspend();
                    }
                }
            }
        });

        remoteAddress = channel.socket().getRemoteSocketAddress().toString();
        listener.onTransportConnected();
    }



    private void dispose() {

//        OneWay oneWay = outbound.poll();
//        while (oneWay != null) {
//            if (oneWay.retained != null) {
//                oneWay.retained.release();
//            }
//            oneWay = outbound.poll();
//        }
        readSource.release();
        writeSource.release();
        dispatchQueue.release();
        next_outbound_buffer = null;
        outbound_buffer = null;
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
        } catch (IOException e) {
            onTransportFailure(e);
            return false;
        }

        if ( full ) {
            return false;
        } else {
            try {
                wireformat.marshal(command, next_outbound_buffer);
                if( next_outbound_buffer.size() >= bufferSize>>2 ) {
                    full  = true;
                }
            } catch (IOException e) {
                onTransportFailure(e);
                return false;
            }
            if ( outbound_buffer.remaining()==0 ) {
                writeSource.resume();
            }
            return true;
        }

    }

    /**
     * @retruns true if there are no in progress writes.
     */
    private boolean drainOutbound() {
        assert Dispatch.getCurrentQueue() == dispatchQueue;
        try {

            while (socketState.is(CONNECTED.class) ) {

                // if we have a pending write that is being sent over the socket...
                if (outbound_buffer.remaining()!=0) {
                    channel.write(outbound_buffer);
                    if (outbound_buffer.remaining() != 0) {
                        return false;
                    }
                } else {
                    if( next_outbound_buffer.size()!=0) {
                        // size of next buffer is based on how much was used in the previous buffer.
                        int prev_size = Math.min(Math.max(outbound_buffer.position()+512, 512), bufferSize);
                        outbound_buffer = next_outbound_buffer.toBuffer().toByteBuffer();
                        next_outbound_buffer = new DataByteArrayOutputStream(prev_size);
                    } else {
                        if( full ) {
                            full = false;
                            listener.onRefill();
                            // If the listener did not have anything for us...
                            if (next_outbound_buffer.size() == 0) {
                                // the source is now drained...
                                return true;
                            }
                        } else {
                            return true;
                        }
                    }
                }
            }

        } catch (IOException e) {
            onTransportFailure(e);
            return true;
        }
        return outbound_buffer.remaining() == 0;
    }

    private void drainInbound() throws IOException {
        if (!getServiceState().isStarted() || readSource.isSuspended()) {
            return;
        }
        while (true) {

            // do we need to read in more data???
            if (this.wireformat.unmarshalEndPos() == readBuffer.position()) {

                // do we need a new data buffer to read data into??
                if (readBuffer.remaining() == 0) {

                    // How much data is still not consumed by the wireformat
                    int size = this.wireformat.unmarshalEndPos() - this.wireformat.unmarshalStartPos();

                    int new_capacity = this.wireformat.unmarshalStartPos() == 0 ? size+bufferSize : (size > bufferSize ? size+bufferSize : bufferSize);
                    byte[] new_buffer = new byte[new_capacity];

                    if (size > 0) {
                        System.arraycopy(readBuffer.array(), this.wireformat.unmarshalStartPos(), new_buffer, 0, size);
                    }

                    readBuffer = ByteBuffer.wrap(new_buffer);
                    readBuffer.position(size);
                    this.wireformat.unmarshalStartPos(0);
                    this.wireformat.unmarshalEndPos(size);
                }

                // Try to fill the buffer with data from the socket..
                int p = readBuffer.position();
                int count = channel.read(readBuffer);
                if (count == -1) {
                    throw new EOFException("Peer disconnected");
                } else if (count == 0) {
                    return;
                }
            }

            Object command = this.wireformat.unmarshalNB(readBuffer);

            // Sanity checks to make sure the wireformat is behaving as expected.
            assert wireformat.unmarshalStartPos() <= wireformat.unmarshalEndPos();
            assert wireformat.unmarshalEndPos() <= readBuffer.position();

            if (command != null) {
                listener.onTransportCommand(command);

                // the transport may be suspended after processing a command.
                if (getServiceState() == STOPPED || readSource.isSuspended()) {
                    return;
                }
            }

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
        if( isConnected() ) {
            readSource.suspend();
        }
    }


    public void resumeRead() {
        if( isConnected() ) {
            readSource.resume();
        }
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

    public WireFormat getWireformat() {
        return wireformat;
    }

    public void setWireformat(WireFormat wireformat) {
        this.wireformat = wireformat;
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
