package org.apache.activemq.flow;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.command.BrokerInfo;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.TransportServer;

import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;

public class PipeTransportFactory extends TransportFactory {
    
    private final HashMap<String,PipeTransportServer> servers = new HashMap<String, PipeTransportServer>();
    static final AtomicInteger connectionCounter = new AtomicInteger();
    
    private static class PipeTransport implements Transport, Runnable {

        private final Pipe<Object> pipe;
        private TransportListener listener;
        private String remoteAddress;
        private AtomicBoolean stopping = new AtomicBoolean();
        private Thread thread;

        public PipeTransport(Pipe<Object> pipe) {
            this.pipe = pipe;
        }

        public void start() throws Exception {
            thread = new Thread(this, getRemoteAddress());
            thread.start();
        }

        public void stop() throws Exception {
            stopping.set(true);
            thread.join();
        }
        
        public void oneway(Object command) throws IOException {
            try {
                while( !stopping.get() ) {
                    if( pipe.offer(command, 500, TimeUnit.MILLISECONDS) ) {
                        break;
                    }
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }

        public void run() {
            try {
                while( !stopping.get() ) {
                    Object value = pipe.poll(500, TimeUnit.MILLISECONDS);
                    if( value!=null ) {
                        listener.onCommand(value);
                    }
                }
            } catch (InterruptedException e) {
            }
        }
        
        public String getRemoteAddress() {
            return remoteAddress;
        }

        public TransportListener getTransportListener() {
            return listener;
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

        public <T> T narrow(Class<T> target) {
            if (target.isAssignableFrom(getClass())) {
                return target.cast(this);
            }
            return null;
        }

        public void reconnect(URI uri) throws IOException {
            throw new UnsupportedOperationException();
        }
        
        public FutureResponse asyncRequest(Object command, ResponseCallback responseCallback) throws IOException {
            throw new UnsupportedOperationException();
        }


        public Object request(Object command) throws IOException {
            throw new UnsupportedOperationException();
        }

        public Object request(Object command, int timeout) throws IOException {
            throw new UnsupportedOperationException();
        }

        public void setTransportListener(TransportListener listener) {
            this.listener = listener;
        }

        public void setRemoteAddress(String remoteAddress) {
            this.remoteAddress = remoteAddress;
        }

    }
    
    private class PipeTransportServer implements TransportServer {
        private URI connectURI;
        private TransportAcceptListener listener;
        private String name;

        public URI getConnectURI() {
            return connectURI;
        }

        public InetSocketAddress getSocketAddress() {
            return null;
        }

        public void setAcceptListener(TransportAcceptListener listener) {
            this.listener = listener;
        }

        public void setBrokerInfo(BrokerInfo brokerInfo) {
        }

        public void start() throws Exception {
        }

        public void stop() throws Exception {
            unbind(this);
        }

        public void setConnectURI(URI connectURI) {
            this.connectURI = connectURI;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public Transport connect() {
            int connectionId = connectionCounter.incrementAndGet();
            String remoteAddress = connectURI.toString()+"#"+connectionId;
            assert listener!= null: "Server does not have an accept listener";
            Pipe<Object> pipe = new Pipe<Object>(10);
            PipeTransport rc = new PipeTransport(pipe);
            rc.setRemoteAddress(remoteAddress);
            PipeTransport serverSide = new PipeTransport(pipe.connect());
            serverSide.setRemoteAddress(remoteAddress);
            listener.onAccept(serverSide);
            return rc;
        }
    }
    
    @Override
    public synchronized TransportServer doBind(URI uri) throws IOException {
        String node = uri.getHost();
        if( servers.containsKey(node) ) {
            throw new IOException("Server allready bound: "+node);
        }
        PipeTransportServer server = new PipeTransportServer();
        server.setConnectURI(uri);
        server.setName(node);
        servers.put(node, server);
        return server;
    }
    
    private synchronized void unbind(PipeTransportServer server) {
        servers.remove(server.getName());
    }

    @Override
    public synchronized Transport doCompositeConnect(URI location) throws Exception {
        String name = location.getHost();
        PipeTransportServer server = servers.get(name );
        if( server==null ) {
            throw new IOException("Server is not bound: "+name);
        }
        return server.connect();
    }

}
