package org.apache.activemq.transport.pipe;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.dispatch.DispatchPriority;
import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.dispatch.internal.RunnableCountDownLatch;
import org.apache.activemq.transport.DispatchableTransport;
import org.apache.activemq.transport.FutureResponse;
import org.apache.activemq.transport.ResponseCallback;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportListener;
import org.apache.activemq.transport.TransportServer;
import org.apache.activemq.transport.pipe.Pipe.ReadReadyListener;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.util.buffer.Buffer;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

public class PipeTransportFactory extends TransportFactory {
    static private final Object EOF_TOKEN = new Object();

    static protected final HashMap<String, PipeTransportServer> servers = new HashMap<String, PipeTransportServer>();

    protected static class PipeTransport implements DispatchableTransport, Runnable, ReadReadyListener<Object> {

        private final Pipe<Object> pipe;
        private TransportListener listener;
        private String remoteAddress;
        private AtomicBoolean stopping = new AtomicBoolean();
        private Thread thread;
        private String name;
        private WireFormat wireFormat;
        private boolean marshal;
        private boolean trace;
        private DispatchQueue dispatchQueue;
        private Runnable dispatchTask;

        public PipeTransport(Pipe<Object> pipe) {
            this.pipe = pipe;
        }

        public void start() throws Exception {
            if (dispatchQueue != null) {
                pipe.setMode(Pipe.ASYNC);
                dispatchQueue.dispatchAsync(dispatchTask);
            } else {
                thread = new Thread(this, getRemoteAddress());
                thread.start();
            }
        }

        public void stop() throws Exception {
        	pipe.write(EOF_TOKEN);
            if (dispatchQueue != null) {
                RunnableCountDownLatch done = new RunnableCountDownLatch(1);
                dispatchQueue.setShutdownHandler(done);
                dispatchQueue.release();
                done.await();
            } else {
                stopping.set(true);
                if( thread!=null ) {
                	thread.join();
                }
            }
        }

        public void setDispatcher(Dispatcher dispatcher) {
            dispatchQueue = dispatcher.createSerialQueue(name);
            dispatchTask = new Runnable(){
                public void run() {
                    dispatch();
                }
            };
        }

        public void onReadReady(Pipe<Object> pipe) {
            if (dispatchQueue != null) {
                dispatchQueue.dispatchAsync(dispatchTask);
            }
        }

        public void setName(String name) {
            this.name = name;
        }

        public void oneway(Object command) throws IOException {

            try {
                if (wireFormat != null && marshal) {
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

        public void dispatch() {
            while (true) {
                try {
                    Object o = pipe.poll();
                    if (o == null) {
                        pipe.setReadReadyListener(this);
                        return;
                    } else {
                    	if(o == EOF_TOKEN) {
                    		throw new EOFException();
                    	}                    	
                        if (wireFormat != null && marshal) {
                            listener.onCommand(wireFormat.unmarshal((Buffer) o));
                        } else {
                            listener.onCommand(o);
                        }
                        dispatchQueue.dispatchAsync(dispatchTask);
                        return;
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
                    	if(value == EOF_TOKEN) {
                    		throw new EOFException();
                    	} else {
                            if (wireFormat != null && marshal) {
                                listener.onCommand(wireFormat.unmarshal((Buffer)value));
                            } else {
                                listener.onCommand(value);
                            }
                    	}
                    }
                }
            } catch (IOException e) {
                listener.onException(e);
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

        public void setDispatchPriority(DispatchPriority priority) {
//            TODO:
//            readContext.updatePriority(priority);
        }

        public WireFormat getWireformat() {
            return wireFormat;
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
    }

    protected class PipeTransportServer implements TransportServer {
    	protected URI connectURI;
        protected TransportAcceptListener listener;
        protected String name;
        protected WireFormatFactory wireFormatFactory;
        protected boolean marshal;
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

        public PipeTransport connect() {
            int connectionId = connectionCounter.incrementAndGet();
            String remoteAddress = connectURI.toString() + "#" + connectionId;
            assert listener != null : "Server does not have an accept listener";
            Pipe<Object> pipe = new Pipe<Object>();
            PipeTransport rc = createClientTransport(pipe);
            rc.setRemoteAddress(remoteAddress);
            PipeTransport serverSide = cerateServerTransport(pipe);
            serverSide.setMarshal(marshal);
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

		public boolean isMarshal() {
			return marshal;
		}

		public void setMarshal(boolean marshal) {
			this.marshal = marshal;
		}
    }

    @Override
    public TransportServer doBind(URI uri) throws IOException {
        try {
            Map<String, String> options = new HashMap<String, String>(URISupport.parseParamters(uri));
            String node = uri.getHost();
    		synchronized(servers) {
	            if (servers.containsKey(node)) {
	                throw new IOException("Server already bound: " + node);
	            }
	            PipeTransportServer server = createTransportServer();
	            server.setConnectURI(uri);
	            server.setName(node);
                server.setWireFormatFactory(createWireFormatFactory(options));
                IntrospectionSupport.setProperties(server, options);
                
                if (!options.isEmpty()) {
                    throw new IllegalArgumentException("Invalid bind parameters: " + options);
                }
                
                
	            servers.put(node, server);
	            return server;
    		}
        } catch (URISyntaxException e) {
            throw IOExceptionSupport.create(e);
        }
    }

	protected PipeTransportServer createTransportServer() {
		return new PipeTransportServer();
	}
	

	static public PipeTransportServer lookup(String name) {
		synchronized(servers) {
			return servers.get(name);
    	}
	}
    
    static public Map<String, PipeTransportServer> getServers() {
    	synchronized(servers) {
    		return new HashMap<String, PipeTransportServer>(servers);
    	}
    }

	static public void unbind(PipeTransportServer server) {
		synchronized(servers) {
			servers.remove(server.getName());
		}
    }
	
	public Transport compositeConfigure(Transport transport, WireFormat format, Map options) {

        PipeTransport pipeTransport = (PipeTransport)transport.narrow(PipeTransport.class);
        IntrospectionSupport.setProperties(pipeTransport, options);
        
        if (pipeTransport.isTrace()) {
            throw new UnsupportedOperationException("Trace not implemented");
//            try {
//                transport = TransportLoggerFactory.getInstance().createTransportLogger(transport, pipeTransport.getLogWriterName(),
//                        pipeTransport.isDynamicManagement(), pipeTransport.isStartLogging(), pipeTransport.getJmxPort());
//            } catch (Throwable e) {
//                LOG.error("Could not create TransportLogger object for: " + pipeTransport.getLogWriterName() + ", reason: " + e, e);
//            }
        }
        
        transport = format.createTransportFilters(transport, options);
        return transport;
    }

    protected String getOption(Map options, String key, String def) {
        String rc = (String) options.remove(key);
        if( rc == null ) {
            rc = def;
        }
        return rc;
    }

    protected Transport createTransport(URI location, WireFormat wf) throws UnknownHostException, IOException {
        String name = location.getHost();
		synchronized(servers) {
	        PipeTransportServer server = lookup(name);
	        if (server == null) {
	            throw new IOException("Server is not bound: " + name);
	        }
	        PipeTransport transport = server.connect();
	        transport.setWireFormat(wf);
	        return transport;
		}
    }

	
    
}
