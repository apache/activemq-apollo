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

import org.apache.activemq.apollo.transport.TransportServer;
import org.apache.activemq.apollo.transport.Transport;
import org.apache.activemq.apollo.transport.TransportAcceptListener;
import org.apache.activemq.apollo.util.IOExceptionSupport;
import org.apache.activemq.apollo.util.IntrospectionSupport;
import org.fusesource.hawtdispatch.Dispatch;
import org.fusesource.hawtdispatch.DispatchQueue;
import org.fusesource.hawtdispatch.DispatchSource;

import java.io.IOException;
import java.net.*;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

/**
 * A TCP based implementation of {@link TransportServer}
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

public class TcpTransportServer implements TransportServer {

    private ServerSocketChannel channel;
    private TransportAcceptListener listener;
    private URI bindURI;
    private URI connectURI;
    private DispatchQueue dispatchQueue;
    private DispatchSource acceptSource;
    private int backlog = 500;
    private Map<String, Object> transportOptions;

    public TcpTransportServer(URI location) {
        this.bindURI = location;
    }

    public void setAcceptListener(TransportAcceptListener listener) {
        this.listener = listener;
    }

    public URI getConnectURI() {
        return connectURI;
    }

    public InetSocketAddress getSocketAddress() {
        return (InetSocketAddress) channel.socket().getLocalSocketAddress();
    }

    public DispatchQueue getDispatchQueue() {
        return dispatchQueue;
    }

    public void setDispatchQueue(DispatchQueue dispatchQueue) {
        this.dispatchQueue = dispatchQueue;
    }

    public void suspend() {
        acceptSource.resume();
    }

    public void resume() {
        acceptSource.resume();
    }

    public void start() throws Exception {
        start(null);
    }
    public void start(Runnable onCompleted) throws IOException {
        URI bind = bindURI;

        String host = bind.getHost();
        host = (host == null || host.length() == 0) ? "localhost" : host;
        if (host.equals("localhost")) {
            host = "0.0.0.0";
        }

        InetAddress addr = InetAddress.getByName(host);
        try {
            channel = ServerSocketChannel.open();
            channel.configureBlocking(false);
            channel.socket().bind(new InetSocketAddress(addr, bind.getPort()), backlog);
        } catch (IOException e) {
            throw IOExceptionSupport.create("Failed to bind to server socket: " + bind + " due to: " + e, e);
        }

        try {
            connectURI = connectURI(resolveHostName(channel.socket(), addr));
        } catch (URISyntaxException e) {
            // it could be that the host name contains invalid characters such
            // as _ on unix platforms
            // so lets try use the IP address instead
            try {
                connectURI = connectURI(addr.getHostAddress());
            } catch (URISyntaxException e2) {
                throw IOExceptionSupport.create(e2);
            }
        }
        
        acceptSource = Dispatch.createSource(channel, SelectionKey.OP_ACCEPT, dispatchQueue);
        acceptSource.setEventHandler(new Runnable() {
            public void run() {
                try {
                    SocketChannel client = channel.accept();
                    while( client!=null ) {
                        handleSocket(client);
                        client = channel.accept();
                    }
                } catch (IOException e) {
                    listener.onAcceptError(e);
                }
            }
        });
        acceptSource.setCancelHandler(new Runnable() {
            public void run() {
                try {
                    channel.close();
                } catch (IOException e) {
                }
            }
        });
        acceptSource.resume();
        if( onCompleted!=null ) {
            dispatchQueue.execute(onCompleted);
        }
    }

    private URI connectURI(String hostname) throws URISyntaxException {
        return new URI(bindURI.getScheme(), bindURI.getUserInfo(), hostname, channel.socket().getLocalPort(), bindURI.getPath(), bindURI.getQuery(), bindURI.getFragment());
    }

    protected String resolveHostName(ServerSocket socket, InetAddress bindAddress) throws UnknownHostException {
        String result = null;
        if (socket.isBound()) {
            if (socket.getInetAddress().isAnyLocalAddress()) {
                // make it more human readable and useful, an alternative to 0.0.0.0
                result = InetAddress.getLocalHost().getHostName();
            } else {
                result = socket.getInetAddress().getCanonicalHostName();
            }
        } else {
            result = bindAddress.getCanonicalHostName();
        }
        return result;
    }

    public void stop() throws Exception {
        stop(null);
    }
    public void stop(Runnable onCompleted) throws Exception {
        acceptSource.setDisposer(onCompleted);
        acceptSource.release();
    }

    public URI getBindURI() {
        return bindURI;
    }

    public void setBindURI(URI bindURI) {
        this.bindURI = bindURI;
    }

    public int getBacklog() {
        return backlog;
    }

    public void setBacklog(int backlog) {
        this.backlog = backlog;
    }

    protected final void handleSocket(SocketChannel socket) throws IOException {
        HashMap<String, Object> options = new HashMap<String, Object>();
//      options.put("maxInactivityDuration", Long.valueOf(maxInactivityDuration));
//      options.put("maxInactivityDurationInitalDelay", Long.valueOf(maxInactivityDurationInitalDelay));
//      options.put("trace", Boolean.valueOf(trace));
//      options.put("soTimeout", Integer.valueOf(soTimeout));
//      options.put("socketBufferSize", Integer.valueOf(socketBufferSize));
//      options.put("connectionTimeout", Integer.valueOf(connectionTimeout));
//      options.put("dynamicManagement", Boolean.valueOf(dynamicManagement));
//      options.put("startLogging", Boolean.valueOf(startLogging));

        Transport transport = createTransport(socket, options);
        listener.onAccept(transport);
    }

    private Transport createTransport(SocketChannel socketChannel, HashMap<String, Object> options) throws IOException {
        TcpTransport transport = new TcpTransport();
        transport.connected(socketChannel);
        if( options!=null ) {
            IntrospectionSupport.setProperties(transport, options);
        }
        if (transportOptions != null) {
            IntrospectionSupport.setProperties(transport, transportOptions);
        }
        return transport;
    }

    public void setTransportOption(Map<String, Object> transportOptions) {
        this.transportOptions = transportOptions;
    }

    /**
     * @return pretty print of this
     */
    public String toString() {
        return "" + bindURI;
    }

}