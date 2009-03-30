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
package org.apache.activemq.broker;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.Connection;
import org.apache.activemq.broker.store.BrokerDatabase;
import org.apache.activemq.broker.store.Store;
import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.transport.DispatchableTransportServer;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;

public class MessageBroker implements TransportAcceptListener {

    public static final int MAX_USER_PRIORITY = 10;
    public static final int MAX_PRIORITY = MAX_USER_PRIORITY + 1;

    final ArrayList<Connection> clientConnections = new ArrayList<Connection>();
    private final LinkedHashMap<AsciiBuffer, VirtualHost> virtualHosts = new LinkedHashMap<AsciiBuffer, VirtualHost>();
    private VirtualHost defaultVirtualHost;

    private TransportServer transportServer;
    private String bindUri;
    private String connectUri;
    private String name;
    private IDispatcher dispatcher;
    private BrokerDatabase database;
    private final AtomicBoolean stopping = new AtomicBoolean();

    public String getName() {
        return name;
    }

    public final void stop() throws Exception {
        stopping.set(true);
        transportServer.stop();

        for (Connection connection : clientConnections) {
            connection.stop();
        }

        for (VirtualHost virtualHost : virtualHosts.values()) {
            virtualHost.stop();
        }
        database.stop();
        dispatcher.shutdown();

    }

    public final void start() throws Exception {
        dispatcher.start();
        if (database != null) {
            database.start();
        } else {
            throw new Exception("Store not initialized");
        }
        addVirtualHost(getDefaultVirtualHost());

        for (VirtualHost virtualHost : virtualHosts.values()) {
            virtualHost.start();
        }

        transportServer = TransportFactory.bind(new URI(bindUri));
        transportServer.setAcceptListener(this);
        if (transportServer instanceof DispatchableTransportServer) {
            ((DispatchableTransportServer) transportServer).setDispatcher(dispatcher);
        }
        transportServer.start();

    }

    public void onAccept(final Transport transport) {
        BrokerConnection connection = new BrokerConnection();
        connection.setBroker(this);
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
        System.out.println("Accept error: " + error);
        error.printStackTrace();
    }

    public IDispatcher getDispatcher() {
        return dispatcher;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDispatcher(IDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public String getBindUri() {
        return bindUri;
    }

    public void setBindUri(String uri) {
        this.bindUri = uri;
    }

    public boolean isStopping() {
        return stopping.get();
    }

    public String getConnectUri() {
        return connectUri;
    }

    public void setConnectUri(String connectUri) {
        this.connectUri = connectUri;
    }

    // /////////////////////////////////////////////////////////////////
    // Virtual Host Related Opperations
    // /////////////////////////////////////////////////////////////////
    public VirtualHost getDefaultVirtualHost() {
        synchronized (virtualHosts) {
            if (defaultVirtualHost == null) {
                defaultVirtualHost = new VirtualHost();
                defaultVirtualHost.setDatabase(database);
                ArrayList<AsciiBuffer> names = new ArrayList<AsciiBuffer>(1);
                names.add(new AsciiBuffer("default"));
                defaultVirtualHost.setHostNames(names);
            }
            return defaultVirtualHost;
        }
    }

    public void setDefaultVirtualHost(VirtualHost defaultVirtualHost) {
        synchronized (virtualHosts) {
            this.defaultVirtualHost = defaultVirtualHost;
        }
    }

    public void addVirtualHost(VirtualHost host) throws Exception {
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
        host.setDatabase(database);
    }

    public synchronized void removeVirtualHost(VirtualHost host) throws Exception {
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

    public void setStore(Store store) {
        database = new BrokerDatabase(store, dispatcher);
    }

}