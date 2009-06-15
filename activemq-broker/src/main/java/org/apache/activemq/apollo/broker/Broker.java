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
package org.apache.activemq.apollo.broker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.Service;
import org.apache.activemq.apollo.Connection;
import org.apache.activemq.broker.store.Store;
import org.apache.activemq.broker.store.StoreFactory;
import org.apache.activemq.dispatch.DispatcherAware;
import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.dispatch.PriorityDispatcher;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportServer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Broker implements Service {

	static final private Log LOG = LogFactory.getLog(Broker.class);
	
    public static final int MAX_USER_PRIORITY = 10;
    public static final int MAX_PRIORITY = MAX_USER_PRIORITY + 1;

    private final ArrayList<Connection> clientConnections = new ArrayList<Connection>();
    private final ArrayList<TransportServer> transportServers = new ArrayList<TransportServer>();
    private final ArrayList<String> connectUris = new ArrayList<String>();

    private final LinkedHashMap<AsciiBuffer, VirtualHost> virtualHosts = new LinkedHashMap<AsciiBuffer, VirtualHost>();
    private VirtualHost defaultVirtualHost;
	private String name;
    private IDispatcher dispatcher;
    private BrokerDatabase database;
    
    private final class BrokerAcceptListener implements TransportAcceptListener {
		public void onAccept(final Transport transport) {
		    BrokerConnection connection = new BrokerConnection();
		    connection.setBroker(Broker.this);
		    connection.setTransport(transport);
		    connection.setPriorityLevels(MAX_PRIORITY);
		    connection.setDispatcher(dispatcher);
		    clientConnections.add(connection);
		    try {
		        connection.start();
		    } catch (Exception e1) {
		        onAcceptError(e1);
		    }
		}

		public void onAcceptError(Exception error) {
			LOG.warn("Accept error: " + error);
			LOG.debug("Accept error details: ", error);
		}
	}

	enum State { 
    	CONFIGURATION, STARTING, RUNNING, STOPPING {
    		@Override
    		public boolean isStopping() {
    			return true;
    		}
    	}
    	, STOPPED {
    		@Override
    		public boolean isStopping() {
    			return true;
    		}
    	}, 
    	UNKNOWN;
    	
    	public boolean isStopping() {
    		return false;
    	}
    	
    };
    
    private final AtomicReference<State> state = new AtomicReference<State>(State.CONFIGURATION);

    // /////////////////////////////////////////////////////////////////
    // Methods of the Service Interface
    // /////////////////////////////////////////////////////////////////    

    public final void start() throws Exception {

		if ( state.get()!=State.CONFIGURATION ) {
    		throw new IllegalStateException("Can only start a broker that has never been started before");
    	}

		// Don't change the state to STARTING yet as we may need to 
		// apply some default configuration to this broker instance before it's started.
		if( dispatcher == null ) {
			int threads = Runtime.getRuntime().availableProcessors();
			dispatcher = PriorityDispatcher.createPriorityDispatchPool("Broker: "+getName(), Broker.MAX_PRIORITY, threads);
		}
		if ( database == null ) {
			Store store = StoreFactory.createStore("kaha-db");
			database = new BrokerDatabase(store);
		}
	    addVirtualHost(getDefaultVirtualHost());

	    // Ok now we are ready to start the broker up....
		if ( !state.compareAndSet(State.CONFIGURATION, State.STARTING) ) {
    		throw new IllegalStateException("Can only start a broker that has never been started before");
    	}
    	try {
		    dispatcher.start();
		    database.setDispatcher(dispatcher);
		    database.start();

	    	synchronized(virtualHosts) {
			    for (VirtualHost virtualHost : virtualHosts.values()) {
			        virtualHost.start();
			    }
	    	}
	    	
		    // Startup the transports.
	    	synchronized(transportServers) {
			    for (TransportServer server : transportServers) {
			    	startTransportServer(server);
			    }
	    	}
	    	
        	state.set(State.RUNNING);
        	
    	} catch (Exception e) {
    		// We should try to avoid falling here... basically means 
    		// we need to handle failure during the startup to avoid 
    		// a partially started up broker.
        	state.set(State.UNKNOWN);
    	}
        
    }

    public final void stop() throws Exception {
    	if ( !state.compareAndSet(State.RUNNING, State.STOPPING) ) {
    		throw new IllegalStateException("Can only stop a broker that is running");
    	}
    	
    	synchronized(transportServers) {
	        for ( TransportServer server : transportServers) {
				stop(server);
	        }
    	}

        for (Connection connection : clientConnections) {
        	stop(connection);
        }

        for (VirtualHost virtualHost : virtualHosts.values()) {
        	stop(virtualHost);
        }
        stop(database);
        dispatcher.shutdown();
    	state.set(State.STOPPED);

    }
        
    // /////////////////////////////////////////////////////////////////
    // Life cycle support operations.
    // /////////////////////////////////////////////////////////////////    
    
    public boolean isStopping() {
        return state.get().isStopping();
    }
    
    // /////////////////////////////////////////////////////////////////
    // connectUris Related Operations
    // /////////////////////////////////////////////////////////////////    
    public List<String> getConnectUris() {
    	synchronized(connectUris) {
    		return new ArrayList<String>(connectUris);
    	}
	}

	public void addConnectUri(String uri) {
    	synchronized(connectUris) {
    		this.connectUris.add(uri);
    	}
	}
	
	public void removeConnectUri(String uri) {
    	synchronized(connectUris) {
    		this.connectUris.remove(uri);
    	}
	}

	
    // /////////////////////////////////////////////////////////////////
    // transportServers Related Operations
    // /////////////////////////////////////////////////////////////////    
    public List<TransportServer> getTransportServers() {
    	synchronized(transportServers) {
    		return new ArrayList<TransportServer>(transportServers);
    	}
	}

	public void addTransportServer(TransportServer server) {
    	synchronized(transportServers) {
    		switch(state.get()) {
    		case RUNNING:
    			startTransportServerWrapException(server);
    			break;
    		case CONFIGURATION:
        		this.transportServers.add(server);
        		break;
    		default:
    			throw new IllegalStateException("Cannot add a transport server when broker is: " + state.get());
    		}
    	}
	}
	
	public void removeTransportServer(TransportServer server) {
    	synchronized(transportServers) {
    		switch(state.get()) {
    		case RUNNING:
    			stopTransportServerWrapException(server);
    			break;
    		case STOPPED:
    		case CONFIGURATION:
        		this.transportServers.remove(server);
        		break;
    		default:
    			throw new IllegalStateException("Cannot add a transport server when broker is: " + state.get());
    		}
    	}
	}

	// /////////////////////////////////////////////////////////////////
    // Virtual Host Related Operations
    // /////////////////////////////////////////////////////////////////
    public VirtualHost getDefaultVirtualHost() {
        synchronized (virtualHosts) {
            if (defaultVirtualHost == null) {
                defaultVirtualHost = new VirtualHost(this);
                ArrayList<AsciiBuffer> names = new ArrayList<AsciiBuffer>(1);
                names.add(new AsciiBuffer("default"));
                defaultVirtualHost.setHostNames(names);
            }
            return defaultVirtualHost;
        }
    }

    public void setDefaultVirtualHost(VirtualHost defaultVirtualHost) {
    	assertInConfigurationState();
    	synchronized (virtualHosts) {
            this.defaultVirtualHost = defaultVirtualHost;
        }
    }

    public void addVirtualHost(VirtualHost host) throws Exception {
    	assertInConfigurationState();
    	
        synchronized (virtualHosts) {
            // Make sure it's valid.
            ArrayList<AsciiBuffer> hostNames = host.getHostNames();
            if (hostNames.isEmpty()) {
                throw new Exception("Virtual host must be configured with at least one host name.");
            }
            for (AsciiBuffer name : hostNames) {
                if (virtualHosts.containsKey(name)) {
                    throw new Exception("Virtual host with host name " + name + " already exists.");
                }
            }

            // Register it.
            for (AsciiBuffer name : hostNames) {
                virtualHosts.put(name, host);
            }

            // The first virtual host defined is the default virtual host.
            if (virtualHosts.size() == 1) {
                setDefaultVirtualHost(host);
            }
        }
    }

	public synchronized void removeVirtualHost(VirtualHost host) throws Exception {
    	assertInConfigurationState();
        synchronized (virtualHosts) {
            for (AsciiBuffer name : host.getHostNames()) {
                virtualHosts.remove(name);
            }
            // Was the default virtual host removed? Set the default to the next
            // virtual host.
            if (host == defaultVirtualHost) {
                if (virtualHosts.isEmpty()) {
                    defaultVirtualHost = null;
                } else {
                    defaultVirtualHost = virtualHosts.values().iterator().next();
                }
            }
        }
    }

    public VirtualHost getVirtualHost(AsciiBuffer name) {
        synchronized (virtualHosts) {
            return virtualHosts.get(name);
        }
    }

    public synchronized Collection<VirtualHost> getVirtualHosts() {
        synchronized (virtualHosts) {
            return new ArrayList<VirtualHost>(virtualHosts.values());
        }
    }
    
    // /////////////////////////////////////////////////////////////////
    // Property Accessors
    // /////////////////////////////////////////////////////////////////

    public String getName() {
        return name;
    }
    public void setName(String name) {
    	assertInConfigurationState();
        this.name = name;
    }

    public IDispatcher getDispatcher() {
        return dispatcher;
    }
    public void setDispatcher(IDispatcher dispatcher) {
    	assertInConfigurationState();
        this.dispatcher = dispatcher;
    }

    public BrokerDatabase getDatabase() {
        return database;
    }
    public void setDatabase(BrokerDatabase database) {
    	assertInConfigurationState();
        this.database = database;
    }
    public void setStore(Store store) {
    	assertInConfigurationState();
        database = new BrokerDatabase(store);
    }
 
    // /////////////////////////////////////////////////////////////////
    // Helper Methods
    // /////////////////////////////////////////////////////////////////

    private void assertInConfigurationState() {
		if( state.get() != State.CONFIGURATION ) {
			throw new IllegalStateException("Broker is not in the configuration state.");
		}
	}
    
    /**
     * Helper method to help stop broker services and log error if they fail to start.
     * @param server
     */
    private void stop(Service server) {
		try {
			server.stop();
		} catch (Exception e) {
			LOG.warn("Could not stop "+server+": "+e);
			LOG.debug("Could not stop "+server+" due to: ", e);
		}
	}

    private void startTransportServerWrapException(TransportServer server) {
		try {
			startTransportServer(server);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
    
	private void startTransportServer(TransportServer server) throws Exception {
		server.setAcceptListener(new BrokerAcceptListener());
		if (server instanceof DispatcherAware ) {
			((DispatcherAware) server).setDispatcher(dispatcher);
		}
		server.start();
	}

    private void stopTransportServerWrapException(TransportServer server) {
		try {
			server.stop();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
   
}