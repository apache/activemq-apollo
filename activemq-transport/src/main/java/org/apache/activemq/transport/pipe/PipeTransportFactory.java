package org.apache.activemq.transport.pipe;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.dispatch.IDispatcher.DispatchContext;
import org.apache.activemq.dispatch.IDispatcher.Dispatchable;
import org.apache.activemq.transport.DispatchableTransport;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.pipe.Pipe.ReadReadyListener;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class PipeTransportFactory extends TransportFactory {

    protected final HashMap<String, PipeTransportServer> servers = new HashMap<String, PipeTransportServer>();

    protected static class PipeTransport implements DispatchableTransport, Dispatchable, Runnable, ReadReadyListener<Object> {

        private final Pipe<Object> pipe;
        private TransportListener listener;
        private String remoteAddress;
        private AtomicBoolean stopping = new AtomicBoolean();
        private Thread thread;
        private DispatchContext readContext;
        private String name;
        private WireFormat wireFormat;

        public PipeTransport(Pipe<Object> pipe) {
            this.pipe = pipe;
        }

        public void start() throws Exception {
            if (readContext != null) {
                pipe.setMode(Pipe.ASYNC);
                readContext.requestDispatch();
            } else {
                thread = new Thread(this, getRemoteAddress());
                thread.start();
            }
        }

        public void stop() throws Exception {
            if (readContext != null) {
                readContext.close(true);
            } else {
                stopping.set(true);
                thread.join();
            }
        }

        public void setDispatcher(IDispatcher dispatcher) {
            readContext = dispatcher.register(this, name);
        }

        public void onReadReady(Pipe<Object> pipe) {
            if (readContext != null) {
                readContext.requestDispatch();
            }
        }

        public void setName(String name) {
            this.name = name;
        }

        public void oneway(Object command) throws IOException {

            try {
                if (wireFormat != null) {
                    pipe.write(wireFormat.marshal(command));
                } else {
                    pipe.write(command);
                }
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
            /*
             * try { while( !stopping.get() ) { if( pipe.offer(command, 500,
             * TimeUnit.MILLISECONDS) ) { break; } } } catch
             * (InterruptedException e) { throw new InterruptedIOException(); }
             */
        }

        public boolean dispatch() {
            while (true) {
                try {
                    Object o = pipe.poll();
                    if (o == null) {
                        pipe.setReadReadyListener(this);
                        return true;
                    } else {
                        if (wireFormat != null) {
                            listener.onCommand(wireFormat.unmarshal((ByteSequence) o));
                        } else {
                            listener.onCommand(o);
                        }
                        return false;
                    }
                } catch (IOException e) {
                    listener.onException(e);
                }
            }
        }

        public void run() {

            try {
                while (!stopping.get()) {
                    Object value = pipe.poll(500, TimeUnit.MILLISECONDS);
                    if (value != null) {
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

        public boolean isUseInactivityMonitor() {
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
            if (name == null) {
                name = remoteAddress;
            }
        }

        public void setWireFormat(WireFormat wireFormat) {
            this.wireFormat = wireFormat;
        }

        public void setDispatchPriority(int priority) {
            readContext.updatePriority(priority);
        }

        /* (non-Javadoc)
         * @see org.apache.activemq.transport.Transport#getWireformat()
         */
        public WireFormat getWireformat() {
            return wireFormat;
        }
    }

    protected class PipeTransportServer implements TransportServer {
    	protected URI connectURI;
        protected TransportAcceptListener listener;
        protected String name;
        protected WireFormatFactory wireFormatFactory;
        protected final AtomicInteger connectionCounter = new AtomicInteger();

        public URI getConnectURI() {
            return connectURI;
        }

        public InetSocketAddress getSocketAddress() {
            return null;
        }

        public void setAcceptListener(TransportAcceptListener listener) {
            this.listener = listener;
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
            String remoteAddress = connectURI.toString() + "#" + connectionId;
            assert listener != null : "Server does not have an accept listener";
            Pipe<Object> pipe = new Pipe<Object>();
            PipeTransport rc = createClientTransport(pipe);
            rc.setRemoteAddress(remoteAddress);
            PipeTransport serverSide = cerateServerTransport(pipe);
            serverSide.setRemoteAddress(remoteAddress);
            if (wireFormatFactory != null) {
                rc.setWireFormat(wireFormatFactory.createWireFormat());
                serverSide.setWireFormat(wireFormatFactory.createWireFormat());
            }
            listener.onAccept(serverSide);
            return rc;
        }

		protected PipeTransport createClientTransport(Pipe<Object> pipe) {
			return new PipeTransport(pipe);
		}

		protected PipeTransport cerateServerTransport(Pipe<Object> pipe) {
			return new PipeTransport(pipe.connect());
		}

        public void setWireFormatFactory(WireFormatFactory wireFormatFactory) {
            this.wireFormatFactory = wireFormatFactory;
        }
    }

    @Override
    public synchronized TransportServer doBind(URI uri) throws IOException {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParamters(uri));

            String node = uri.getHost();
            if (servers.containsKey(node)) {
                throw new IOException("Server already bound: " + node);
            }
            PipeTransportServer server = createTransportServer();
            server.setConnectURI(uri);
            server.setName(node);
            if (options.containsKey("wireFormat")) {
                server.setWireFormatFactory(createWireFormatFactory(options));
            }
                
            servers.put(node, server);
            return server;
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

	protected PipeTransportServer createTransportServer() {
		return new PipeTransportServer();
	}

    private synchronized void unbind(PipeTransportServer server) {
        servers.remove(server.getName());
    }

    @Override
    public synchronized Transport doCompositeConnect(URI location) throws Exception {
        String name = location.getHost();
        PipeTransportServer server = servers.get(name);
        if (server == null) {
            throw new IOException("Server is not bound: " + name);
        }
        return server.connect();
    }

}
