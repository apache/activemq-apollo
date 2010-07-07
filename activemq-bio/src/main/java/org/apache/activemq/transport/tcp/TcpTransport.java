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

import org.apache.activemq.transport.CompletionCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.util.buffer.ByteArrayOutputStream;
import org.apache.activemq.wireformat.WireFormat;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.DispatchSource;

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

    final LinkedList<OneWay> outbound = new LinkedList<OneWay>();
    int maxOutbound = 1024*32;
    ByteBuffer outbound_frame;
    protected boolean useLocalHost = true;

    static final class OneWay {
        final Buffer buffer;
        final CompletionCallback callback;

        public OneWay(Buffer buffer, CompletionCallback callback) {
            this.callback = callback;
            this.buffer = buffer;
        }
    }

    public void connected(SocketChannel channel) {
        this.channel = channel;
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
        if (listener == null) {
            throw new IllegalArgumentException("listener is not set");
        }
        if( transportState!=CREATED ) {
            throw new IllegalStateException("can only be started from the created stae");
        }
        transportState=RUNNING;

        if( socketState == CONNECTING ) {
            channel = SocketChannel.open();
        }
        channel.configureBlocking(false);
        if( socketState == CONNECTING ) {

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
                    if( transportState==RUNNING ) {
                        try {
                            socketState = CONNECTED;
                            channel.finishConnect();
                            connectSource.release();
                            fireConnected();
                        } catch (IOException e) {
                            listener.onException(e);
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
            channel.socket().setSendBufferSize(maxOutbound);
            channel.socket().setReceiveBufferSize(maxOutbound);
        } catch (SocketException e) {
        }

        readSource = Dispatch.createSource(channel, SelectionKey.OP_READ, dispatchQueue);
        readSource.setEventHandler(new Runnable(){
            public void run() {
                drainInbound();
            }
        });

        writeSource = Dispatch.createSource(channel, SelectionKey.OP_WRITE, dispatchQueue);
        writeSource.setEventHandler(new Runnable(){
            public void run() {
                if( transportState==RUNNING ) {
                    // once the outbound is drained.. we can suspend getting
                    // write events.
                    if( drainOutbound() ) {
                        writeSource.suspend();
                    }
                }
            }
        });

        remoteAddress = channel.socket().getRemoteSocketAddress().toString();
        listener.onConnected();
        readSource.resume();
    }


    public void stop() throws Exception {
        if( readSource!=null ) {
            readSource.release();
            readSource = null;
        }
        if( writeSource!=null ) {
            writeSource.release();
            writeSource = null;
        }
        setDispatchQueue(null);
        transportState=DISPOSED;
    }

    @Deprecated
    public void oneway(Object command) {
        oneway(command, null);
    }

    public void oneway(Object command, CompletionCallback callback) {
        try {
            if( socketState != CONNECTED ) {
                throw new IllegalStateException("Not connected.");
            }
        } catch (IllegalStateException e) {
            if( callback!=null ) {
                callback.onFailure(e);
            }
        }

        // Marshall the command.
        Buffer buffer = null;
        try {
            buffer = wireformat.marshal(command);
        } catch (IOException e) {
            callback.onFailure(e);
            return;
        }

        outbound.add(new OneWay(buffer, callback));

        // wait for write ready events if this write
        // cannot be drained.
        if( outbound.size()==1 && !drainOutbound() ) {
            writeSource.resume();
        }
    }

    /**
    * @retruns true if the outbound has been drained of all objects and there are no in progress writes.
    */
    private boolean drainOutbound() {
        try {
            
            while(socketState == CONNECTED) {
                
              // if we have a pending write that is being sent over the socket...
              if( outbound_frame!=null ) {

                channel.write(outbound_frame);
                if( outbound_frame.remaining() != 0 ) {
                  return false;
                } else {
                  outbound_frame = null;
                }

              } else {

                // marshall all the available frames..
                ByteArrayOutputStream buffer = new ByteArrayOutputStream(maxOutbound << 2);
                OneWay oneWay = outbound.poll();

                while( oneWay!=null) {
                    buffer.write(oneWay.buffer);
                    if( oneWay.callback!=null ) {
                        oneWay.callback.onCompletion();
                    }
                    if( buffer.size() < maxOutbound ) {
                        oneWay = outbound.poll();
                    } else {
                        oneWay = null;
                    }
                }


                if( buffer.size()==0 ) {
                  // the source is now drained...
                  return true;
                } else {
                  outbound_frame = buffer.toBuffer().toByteBuffer();
                }
              }

            }

        } catch (IOException e) {
            listener.onException(e);
        }
        
        return outbound.isEmpty() && outbound_frame==null;
    }

    private void drainInbound() {
        Object command = null;
        // the transport may be suspended after processing a command.
        while( !readSource.isSuspended() && (command=wireformat.unmarshal(channel))!=null ) {
            listener.onCommand(command);
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

    public void suspend() {
        readSource.suspend();
    }

    public void resume() {
        readSource.resume();
    }
    
    public void reconnect(URI uri, CompletionCallback callback) {
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

//    private static final Log LOG = LogFactory.getLog(TcpTransport.class);
//    private static final ThreadPoolExecutor SOCKET_CLOSE;
//    protected final URI remoteLocation;
//    protected final URI localLocation;
//    protected final WireFormat wireFormat;
//
//    protected int connectionTimeout = 30000;
//    protected int soTimeout;
//    protected int socketBufferSize = 64 * 1024;
//    protected int ioBufferSize = 8 * 1024;
//    protected boolean closeAsync = true;
//    protected Socket socket;
//    protected DataOutputStream dataOut;
//    protected DataInputStream dataIn;
//    protected TcpBufferedOutputStream buffOut = null;
//
//    private static final boolean ASYNC_WRITE = false;
//    /**
//     * trace=true -> the Transport stack where this TcpTransport object will be,
//     * will have a TransportLogger layer trace=false -> the Transport stack
//     * where this TcpTransport object will be, will NOT have a TransportLogger
//     * layer, and therefore will never be able to print logging messages. This
//     * parameter is most probably set in Connection or TransportConnector URIs.
//     */
//    protected boolean trace = false;
//    /**
//     * Name of the LogWriter implementation to use. Names are mapped to classes
//     * in the
//     * resources/META-INF/services/org/apache/activemq/transport/logwriters
//     * directory. This parameter is most probably set in Connection or
//     * TransportConnector URIs.
//     */
//    //    protected String logWriterName = TransportLoggerFactory.defaultLogWriterName;
//    /**
//     * Specifies if the TransportLogger will be manageable by JMX or not. Also,
//     * as long as there is at least 1 TransportLogger which is manageable, a
//     * TransportLoggerControl MBean will me created.
//     */
//    protected boolean dynamicManagement = false;
//    /**
//     * startLogging=true -> the TransportLogger object of the Transport stack
//     * will initially write messages to the log. startLogging=false -> the
//     * TransportLogger object of the Transport stack will initially NOT write
//     * messages to the log. This parameter only has an effect if trace == true.
//     * This parameter is most probably set in Connection or TransportConnector
//     * URIs.
//     */
//    protected boolean startLogging = true;
//    /**
//     * Specifies the port that will be used by the JMX server to manage the
//     * TransportLoggers. This should only be set in an URI by a client (producer
//     * or consumer) since a broker will already create a JMX server. It is
//     * useful for people who test a broker and clients in the same machine and
//     * want to control both via JMX; a different port will be needed.
//     */
//    protected int jmxPort = 1099;
//    protected int minmumWireFormatVersion;
//    protected SocketFactory socketFactory;
//    protected final AtomicReference<CountDownLatch> stoppedLatch = new AtomicReference<CountDownLatch>();
//
//    private Map<String, Object> socketOptions;
//    private Boolean keepAlive;
//    private Boolean tcpNoDelay;
//    private Thread runnerThread;
//
//    protected boolean useActivityMonitor;
//
//    /**
//     * Connect to a remote Node - e.g. a Broker
//     *
//     * @param wireFormat
//     * @param socketFactory
//     * @param remoteLocation
//     * @param localLocation
//     *            - e.g. local InetAddress and local port
//     * @throws IOException
//     * @throws UnknownHostException
//     */
//    public TcpTransport(WireFormat wireFormat, SocketFactory socketFactory, URI remoteLocation, URI localLocation) throws UnknownHostException, IOException {
//        this.wireFormat = wireFormat;
//        this.socketFactory = socketFactory;
//        try {
//            this.socket = socketFactory.createSocket();
//        } catch (SocketException e) {
//            this.socket = null;
//        }
//        this.remoteLocation = remoteLocation;
//        this.localLocation = localLocation;
//        setDaemon(false);
//    }
//
//    /**
//     * Initialize from a server Socket
//     *
//     * @param wireFormat
//     * @param socket
//     * @throws IOException
//     */
//    public TcpTransport(WireFormat wireFormat, Socket socket) throws IOException {
//        this.wireFormat = wireFormat;
//        this.socket = socket;
//        this.remoteLocation = null;
//        this.localLocation = null;
//        setDaemon(true);
//    }
//
//    LinkedBlockingQueue<Object> outbound = new LinkedBlockingQueue<Object>();
//    private Thread onewayThread;
//
//    /**
//     * A one way asynchronous send
//     */
//    public void oneway(Object command) throws IOException {
//        checkStarted();
//        try {
//            if (ASYNC_WRITE) {
//                outbound.put(command);
//            } else {
//                wireFormat.marshal(command, dataOut);
//                dataOut.flush();
//            }
//        } catch (InterruptedException e) {
//            throw new InterruptedIOException();
//        }
//    }
//
//    protected void sendOneways() {
//        try {
//            LOG.debug("Started oneway thead");
//            while (!isStopped()) {
//                Object command = outbound.poll(500, TimeUnit.MILLISECONDS);
//                if (command != null) {
//                    try {
//                        // int count=0;
//                        while (command != null) {
//                            wireFormat.marshal(command, dataOut);
//                            // count++;
//                            command = outbound.poll();
//                        }
//                        // System.out.println(count);
//                        dataOut.flush();
//                    } catch (IOException e) {
//                        getTransportListener().onException(e);
//                    }
//                }
//            }
//        } catch (InterruptedException e) {
//        }
//    }
//
//    /**
//     * @return pretty print of 'this'
//     */
//    public String toString() {
//        return "tcp://" + socket.getInetAddress() + ":" + socket.getPort();
//    }
//
//    /**
//     * reads packets from a Socket
//     */
//    public void run() {
//        LOG.trace("TCP consumer thread for " + this + " starting");
//        this.runnerThread = Thread.currentThread();
//        try {
//            while (!isStopped()) {
//                doRun();
//            }
//        } catch (IOException e) {
//            stoppedLatch.get().countDown();
//            onException(e);
//        } catch (Throwable e) {
//            stoppedLatch.get().countDown();
//            IOException ioe = new IOException("Unexpected error occured");
//            ioe.initCause(e);
//            onException(ioe);
//        } finally {
//            stoppedLatch.get().countDown();
//        }
//    }
//
//    protected void doRun() throws IOException {
//        try {
//            Object command = readCommand();
//            doConsume(command);
//        } catch (SocketTimeoutException e) {
//        } catch (InterruptedIOException e) {
//        }
//    }
//
//    protected Object readCommand() throws IOException {
//        return wireFormat.unmarshal(dataIn);
//    }
//
//    // Properties
//    // -------------------------------------------------------------------------
//
//    public boolean isTrace() {
//        return trace;
//    }
//
//    public void setTrace(boolean trace) {
//        this.trace = trace;
//    }
//
//    void setUseInactivityMonitor(boolean val) {
//        useActivityMonitor = val;
//    }
//
//    public boolean isUseInactivityMonitor() {
//        return useActivityMonitor;
//    }
//
//    //    public String getLogWriterName() {
//    //        return logWriterName;
//    //    }
//    //
//    //    public void setLogWriterName(String logFormat) {
//    //        this.logWriterName = logFormat;
//    //    }
//
//    public boolean isDynamicManagement() {
//        return dynamicManagement;
//    }
//
//    public void setDynamicManagement(boolean useJmx) {
//        this.dynamicManagement = useJmx;
//    }
//
//    public boolean isStartLogging() {
//        return startLogging;
//    }
//
//    public void setStartLogging(boolean startLogging) {
//        this.startLogging = startLogging;
//    }
//
//    public int getJmxPort() {
//        return jmxPort;
//    }
//
//    public void setJmxPort(int jmxPort) {
//        this.jmxPort = jmxPort;
//    }
//
//    public int getMinmumWireFormatVersion() {
//        return minmumWireFormatVersion;
//    }
//
//    public void setMinmumWireFormatVersion(int minmumWireFormatVersion) {
//        this.minmumWireFormatVersion = minmumWireFormatVersion;
//    }
//
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

//    public int getSocketBufferSize() {
//        return socketBufferSize;
//    }
//
//    /**
//     * Sets the buffer size to use on the socket
//     */
//    public void setSocketBufferSize(int socketBufferSize) {
//        this.socketBufferSize = socketBufferSize;
//    }
//
//    public int getSoTimeout() {
//        return soTimeout;
//    }
//
//    /**
//     * Sets the socket timeout
//     */
//    public void setSoTimeout(int soTimeout) {
//        this.soTimeout = soTimeout;
//    }
//
//    public int getConnectionTimeout() {
//        return connectionTimeout;
//    }
//
//    /**
//     * Sets the timeout used to connect to the socket
//     */
//    public void setConnectionTimeout(int connectionTimeout) {
//        this.connectionTimeout = connectionTimeout;
//    }
//
//    public Boolean getKeepAlive() {
//        return keepAlive;
//    }
//
//    /**
//     * Enable/disable TCP KEEP_ALIVE mode
//     */
//    public void setKeepAlive(Boolean keepAlive) {
//        this.keepAlive = keepAlive;
//    }
//
//    public Boolean getTcpNoDelay() {
//        return tcpNoDelay;
//    }
//
//    /**
//     * Enable/disable the TCP_NODELAY option on the socket
//     */
//    public void setTcpNoDelay(Boolean tcpNoDelay) {
//        this.tcpNoDelay = tcpNoDelay;
//    }
//
//    /**
//     * @return the ioBufferSize
//     */
//    public int getIoBufferSize() {
//        return this.ioBufferSize;
//    }
//
//    /**
//     * @param ioBufferSize
//     *            the ioBufferSize to set
//     */
//    public void setIoBufferSize(int ioBufferSize) {
//        this.ioBufferSize = ioBufferSize;
//    }
//
//    /**
//     * @return the closeAsync
//     */
//    public boolean isCloseAsync() {
//        return closeAsync;
//    }
//
//    /**
//     * @param closeAsync
//     *            the closeAsync to set
//     */
//    public void setCloseAsync(boolean closeAsync) {
//        this.closeAsync = closeAsync;
//    }
//
//    // Implementation methods
//    // -------------------------------------------------------------------------
//    protected String resolveHostName(String host) throws UnknownHostException {
//        String localName = InetAddress.getLocalHost().getHostName();
//        if (localName != null && isUseLocalHost()) {
//            if (localName.equals(host)) {
//                return "localhost";
//            }
//        }
//        return host;
//    }
//
//    /**
//     * Configures the socket for use
//     *
//     * @param sock
//     * @throws SocketException
//     */
//    protected void initialiseSocket(Socket sock) throws SocketException {
//        if (socketOptions != null) {
//            IntrospectionSupport.setProperties(socket, socketOptions);
//        }
//
//        try {
//            sock.setReceiveBufferSize(socketBufferSize);
//            sock.setSendBufferSize(socketBufferSize);
//        } catch (SocketException se) {
//            LOG.warn("Cannot set socket buffer size = " + socketBufferSize);
//            LOG.debug("Cannot set socket buffer size. Reason: " + se, se);
//        }
//        sock.setSoTimeout(soTimeout);
//
//        if (keepAlive != null) {
//            sock.setKeepAlive(keepAlive.booleanValue());
//        }
//        if (tcpNoDelay != null) {
//            sock.setTcpNoDelay(tcpNoDelay.booleanValue());
//        }
//    }
//
//    protected void doStart() throws Exception {
//        connect();
//        if (ASYNC_WRITE) {
//            onewayThread = new Thread() {
//                @Override
//                public void run() {
//                    sendOneways();
//                }
//            };
//            onewayThread.start();
//        }
//
//        stoppedLatch.set(new CountDownLatch(1));
//        super.doStart();
//    }
//
//    protected void connect() throws Exception {
//
//        if (socket == null && socketFactory == null) {
//            throw new IllegalStateException("Cannot connect if the socket or socketFactory have not been set");
//        }
//
//        InetSocketAddress localAddress = null;
//        InetSocketAddress remoteAddress = null;
//
//        if (localLocation != null) {
//            localAddress = new InetSocketAddress(InetAddress.getByName(localLocation.getHost()), localLocation.getPort());
//        }
//
//        if (remoteLocation != null) {
//            String host = resolveHostName(remoteLocation.getHost());
//            remoteAddress = new InetSocketAddress(host, remoteLocation.getPort());
//        }
//
//        if (socket != null) {
//
//            if (localAddress != null) {
//                socket.bind(localAddress);
//            }
//
//            // If it's a server accepted socket.. we don't need to connect it
//            // to a remote address.
//            if (remoteAddress != null) {
//                if (connectionTimeout >= 0) {
//                    socket.connect(remoteAddress, connectionTimeout);
//                } else {
//                    socket.connect(remoteAddress);
//                }
//            }
//
//        } else {
//            // For SSL sockets.. you can't create an unconnected socket :(
//            // This means the timout option are not supported either.
//            if (localAddress != null) {
//                socket = socketFactory.createSocket(remoteAddress.getAddress(), remoteAddress.getPort(), localAddress.getAddress(), localAddress.getPort());
//            } else {
//                socket = socketFactory.createSocket(remoteAddress.getAddress(), remoteAddress.getPort());
//            }
//        }
//
//        initialiseSocket(socket);
//        initializeStreams();
//    }
//
//    protected void doStop(ServiceStopper stopper) throws Exception {
//        if (LOG.isDebugEnabled()) {
//            LOG.debug("Stopping transport " + this);
//        }
//
//        // Closing the streams flush the sockets before closing.. if the socket
//        // is hung.. then this hangs the close.
//        // closeStreams();
//        if (socket != null) {
//            if (closeAsync) {
//                // closing the socket can hang also
//                final CountDownLatch latch = new CountDownLatch(1);
//
//                SOCKET_CLOSE.execute(new Runnable() {
//
//                    public void run() {
//                        try {
//                            socket.close();
//                        } catch (IOException e) {
//                            LOG.debug("Caught exception closing socket", e);
//                        } finally {
//                            latch.countDown();
//                        }
//                    }
//
//                });
//                latch.await(1, TimeUnit.SECONDS);
//            } else {
//                try {
//                    socket.close();
//                } catch (IOException e) {
//                    LOG.debug("Caught exception closing socket", e);
//                }
//
//            }
//            if (ASYNC_WRITE) {
//                onewayThread.join();
//            }
//        }
//    }
//
//    /**
//     * Override so that stop() blocks until the run thread is no longer running.
//     */
//    @Override
//    public void stop() throws Exception {
//        super.stop();
//        CountDownLatch countDownLatch = stoppedLatch.get();
//        if (countDownLatch != null && Thread.currentThread() != this.runnerThread) {
//            countDownLatch.await(1, TimeUnit.SECONDS);
//        }
//    }
//
//    protected void initializeStreams() throws Exception {
//        TcpBufferedInputStream buffIn = new TcpBufferedInputStream(socket.getInputStream(), ioBufferSize);
//        this.dataIn = new DataInputStream(buffIn);
//        buffOut = new TcpBufferedOutputStream(socket.getOutputStream(), ioBufferSize);
//        this.dataOut = new DataOutputStream(buffOut);
//    }
//
//    protected void closeStreams() throws IOException {
//        if (dataOut != null) {
//            dataOut.close();
//        }
//        if (dataIn != null) {
//            dataIn.close();
//        }
//    }
//
//    public void setSocketOptions(Map<String, Object> socketOptions) {
//        this.socketOptions = new HashMap<String, Object>(socketOptions);
//    }
//
//    public String getRemoteAddress() {
//        if (socket != null) {
//            return "" + socket.getRemoteSocketAddress();
//        }
//        return null;
//    }
//
//    @Override
//    public <T> T narrow(Class<T> target) {
//        if (target == Socket.class) {
//            return target.cast(socket);
//        } else if (target == TcpBufferedOutputStream.class) {
//            return target.cast(buffOut);
//        }
//        return super.narrow(target);
//    }
//
//    static {
//        SOCKET_CLOSE = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new ThreadFactory() {
//            public Thread newThread(Runnable runnable) {
//                Thread thread = new Thread(runnable, "TcpSocketClose: " + runnable);
//                thread.setPriority(Thread.MAX_PRIORITY);
//                thread.setDaemon(true);
//                return thread;
//            }
//        });
//    }
//
//    public WireFormat getWireformat()
//    {
//        return wireFormat;
//    }
}
