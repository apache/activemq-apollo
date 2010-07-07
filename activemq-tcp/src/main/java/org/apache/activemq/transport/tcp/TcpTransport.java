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

import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.buffer.DataByteArrayOutputStream;
import org.apache.activemq.wireformat.WireFormat;
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

import static org.apache.activemq.transport.tcp.TcpTransport.SocketState.*;
import static org.apache.activemq.transport.tcp.TcpTransport.TransportState.*;

/**
 * An implementation of the {@link Transport} interface using raw tcp/ip
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TcpTransport implements Transport {
    private Map<String, Object> socketOptions;
    private WireFormat.UnmarshalSession unmarshalSession;

    enum SocketState {
        CONNECTING,
        CONNECTED,
        DISCONNECTED
    }

    enum TransportState {
        CREATED,
        RUNNING,
        DISPOSED
    }

    protected URI remoteLocation;
    protected URI localLocation;
    private TransportListener listener;
    private String remoteAddress;
    private WireFormat wireformat;

    private SocketChannel channel;

    private SocketState socketState = DISCONNECTED;
    private TransportState transportState = CREATED;

    private DispatchQueue dispatchQueue;
    private DispatchSource readSource;
    private DispatchSource writeSource;

    int bufferSize = 1024*64;

    final LinkedList<OneWay> outbound = new LinkedList<OneWay>();
    DataByteArrayOutputStream next_outbound_buffer;
    ByteBuffer outbound_buffer;
    protected boolean useLocalHost = true;
    ByteBuffer readBuffer = ByteBuffer.allocate(bufferSize);


    static final class OneWay {
        final Object command;
        final Retained retained;

        public OneWay(Object command, Retained retained) {
            this.command = command;
            this.retained = retained;
        }
    }

    public void connected(SocketChannel channel) {
        this.channel = channel;
        this.remoteAddress = channel.socket().getRemoteSocketAddress().toString();
        this.socketState = CONNECTED;
    }

    public void connecting(URI remoteLocation, URI localLocation) throws IOException {
        this.remoteLocation = remoteLocation;
        this.localLocation = localLocation;
        this.socketState = CONNECTING;
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

    public void start() throws Exception {
        if (dispatchQueue == null) {
            throw new IllegalArgumentException("dispatchQueue is not set");
        }
        if (listener == null) {
            throw new IllegalArgumentException("listener is not set");
        }
        if (transportState != CREATED) {
            throw new IllegalStateException("start can only be used from the created state");
        }
        transportState = RUNNING;

        unmarshalSession = wireformat.createUnmarshalSession();

        if (socketState == CONNECTING) {
            channel = SocketChannel.open();
        }
        channel.configureBlocking(false);
        channel.socket().setSendBufferSize(bufferSize);
        channel.socket().setReceiveBufferSize(bufferSize);
        next_outbound_buffer = new DataByteArrayOutputStream(bufferSize);
        outbound_buffer = ByteBuffer.allocate(0);

        if (socketState == CONNECTING) {

            if (localLocation != null) {
                InetSocketAddress localAddress = new InetSocketAddress(InetAddress.getByName(localLocation.getHost()), localLocation.getPort());
                channel.socket().bind(localAddress);
            }

            String host = resolveHostName(remoteLocation.getHost());
            InetSocketAddress remoteAddress = new InetSocketAddress(host, remoteLocation.getPort());
            channel.connect(remoteAddress);

            final DispatchSource connectSource = Dispatch.createSource(channel, SelectionKey.OP_CONNECT, dispatchQueue);
            connectSource.setEventHandler(new Runnable() {
                public void run() {
                    if (transportState == RUNNING) {
                        try {
                            socketState = CONNECTED;
                            channel.finishConnect();
                            connectSource.release();
                            fireConnected();
                        } catch (IOException e) {
                            onTransportFailure(e);
                        }
                    }
                }
            });
            connectSource.resume();
        } else {
            fireConnected();
        }
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

    private void fireConnected() {

        try {
            channel.socket().setSendBufferSize(bufferSize);
            channel.socket().setReceiveBufferSize(bufferSize);
        } catch (SocketException e) {
        }

        readSource = Dispatch.createSource(channel, SelectionKey.OP_READ, dispatchQueue);
        readSource.setEventHandler(new Runnable() {
            public void run() {
                try {
                    drainInbound();
                } catch (IOException e) {
                    onTransportFailure(e);
                }
            }
        });
        readSource.setCancelHandler(new Runnable() {
            public void run() {
                readSource.release();
                releaseResources();
            }
        });

        writeSource = Dispatch.createSource(channel, SelectionKey.OP_WRITE, dispatchQueue);
        writeSource.setEventHandler(new Runnable() {
            public void run() {
                if (transportState == RUNNING) {
                    // once the outbound is drained.. we can suspend getting
                    // write events.
                    if (drainOutbound()) {
                        writeSource.suspend();
                    }
                }
            }
        });
        writeSource.setCancelHandler(new Runnable() {
            public void run() {
                writeSource.release();
                releaseResources();
            }
        });

        remoteAddress = channel.socket().getRemoteSocketAddress().toString();
        listener.onTransportConnected();
    }


    public void stop() throws Exception {
        if (transportState != RUNNING) {
            throw new IllegalStateException("stop can only be used from the started state");
        }
        transportState = DISPOSED;
        readSource.cancel();
        writeSource.cancel();
    }

    private void releaseResources() {
        if( writeSource.isReleased() && writeSource.isReleased() ) {
            try {
                channel.close();
            } catch (IOException ignore) {
            }
            listener.onTransportDisconnected();
            OneWay oneWay = outbound.poll();
            while (oneWay != null) {
                if (oneWay.retained != null) {
                    oneWay.retained.release();
                }
            }
            setDispatchQueue(null);
            next_outbound_buffer = null;
            outbound_buffer = null;
            unmarshalSession = null;
        }
    }

    public void onTransportFailure(IOException error) {
        if( socketState == CONNECTED ) {
            socketState = DISCONNECTED;
            listener.onTransportFailure(error);
            readSource.cancel();
            writeSource.cancel();
        }
    }


    public boolean isFull() {
        return next_outbound_buffer.size() >= bufferSize>>2;
    }

    public void oneway(Object command, Retained retained) {
        assert Dispatch.getCurrentQueue() == dispatchQueue;
        try {
            if (socketState != CONNECTED) {
                throw new IOException("Not connected.");
            }
            if (transportState != RUNNING) {
                throw new IOException("Not running.");
            }
        } catch (IOException e) {
            onTransportFailure(e);
            return;
        }

        boolean wasEmpty = next_outbound_buffer.size()==0;
        if (retained!=null && isFull() ) {
            // retaining blocks the sender it is released.
            retained.retain();
            outbound.add(new OneWay(command, retained));
        } else {
            try {
                wireformat.marshal(command, next_outbound_buffer);
            } catch (IOException e) {
                onTransportFailure(e);
                return;
            }
            if ( outbound_buffer.remaining()==0 ) {
                writeSource.resume();
            }
        }

    }

    /**
     * @retruns true if the outbound has been drained of all objects and there are no in progress writes.
     */
    private boolean drainOutbound() {
        try {

            while (socketState == CONNECTED) {

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
                        // marshall all the available frames..
                        OneWay oneWay = outbound.poll();
                        while (oneWay != null) {
                            wireformat.marshal(oneWay.command, next_outbound_buffer);
                            if (oneWay.retained != null) {
                                oneWay.retained.release();
                            }
                            if ( isFull() ) {
                                oneWay = null;
                            } else {
                                oneWay = outbound.poll();
                            }
                        }

                        if (next_outbound_buffer.size() == 0) {
                            // the source is now drained...
                            return true;
                        }
                    }
                }
            }

        } catch (IOException e) {
            onTransportFailure(e);
            return true;
        }

        return outbound.isEmpty() && outbound_buffer == null;
    }

    private void drainInbound() throws IOException {
        if (transportState == DISPOSED || readSource.isSuspended()) {
            return;
        }
        while (true) {

            // do we need to read in more data???
            if (unmarshalSession.getEndPos() == readBuffer.position()) {

                // do we need a new data buffer to read data into??
                if (readBuffer.remaining() == 0) {

                    // double the capacity size if needed...
                    int new_capacity = unmarshalSession.getStartPos() != 0 ? bufferSize : readBuffer.capacity() << 2;
                    byte[] new_buffer = new byte[new_capacity];

                    // If there was un-consummed data.. move it to the start of the new buffer.
                    int size = unmarshalSession.getEndPos() - unmarshalSession.getStartPos();
                    if (size > 0) {
                        System.arraycopy(readBuffer.array(), unmarshalSession.getStartPos(), new_buffer, 0, size);
                    }

                    readBuffer = ByteBuffer.wrap(new_buffer);
                    readBuffer.position(size);
                    unmarshalSession.setStartPos(0);
                    unmarshalSession.setEndPos(size);
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

            Object command = unmarshalSession.unmarshal(readBuffer);
            if (command != null) {
                listener.onTransportCommand(command);

                // the transport may be suspended after processing a command.
                if (transportState == DISPOSED || readSource.isSuspended()) {
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

    public void suspendRead() {
        readSource.suspend();
    }

    public void resumeRead() {
        readSource.resume();
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
        return socketState == CONNECTED;
    }

    public boolean isDisposed() {
        return transportState == DISPOSED;
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

}
