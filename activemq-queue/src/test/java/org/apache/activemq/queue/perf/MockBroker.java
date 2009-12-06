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
package org.apache.activemq.queue.perf;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.dispatch.DispatcherFactory;
import org.apache.activemq.dispatch.DispatcherAware;
import org.apache.activemq.flow.IFlowSink;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;

public class MockBroker implements TransportAcceptListener {

    public interface DeliveryTarget {
        public IFlowSink<Message> getSink();

        /**
         * @return true if this sub has a selector
         */
        public boolean hasSelector();
        
        public boolean match(Message message);
    }

    final Router router = new Router();

    final ArrayList<BrokerConnection> connections = new ArrayList<BrokerConnection>();
    final ArrayList<BrokerConnection> brokerConnections = new ArrayList<BrokerConnection>();
    final HashMap<Destination, MockQueue> queues = new HashMap<Destination, MockQueue>();

    private TransportServer transportServer;
    private String uri;
    private String name;
    protected Dispatcher dispatcher;
    private final AtomicBoolean stopping = new AtomicBoolean();
    private boolean useInputQueues = false;

    public boolean isUseInputQueues() {
        return useInputQueues;
    }

    public void setUseInputQueues(boolean useInputQueues) {
        this.useInputQueues = useInputQueues;
    }

    public String getName() {
        return name;
    }

    public void subscribe(Destination destination, DeliveryTarget deliveryTarget) {
        if (destination.getPtp()) {
            queues.get(destination).addConsumer(deliveryTarget);
        } else {
            router.bind(deliveryTarget, destination);
        }
    }

    public void addQueue(MockQueue queue) {
        router.bind(queue, queue.getDestination());
        queues.put(queue.getDestination(), queue);
    }

    // public void createClusterConnection(Destination destination) {
    // RemoteConsumer c = new RemoteConsumer(this.mockBrokerTest, "consumer" +
    // ++consumerCounter, this, destination);
    // consumers.add(c);
    // router.bind(c, destination);
    // }
    // public void createBrokerConnection(MockBroker target, Pipe<Message> pipe)
    // {
    // BrokerConnection bc = this.mockBrokerTest.new BrokerConnection(this,
    // target, pipe);
    // // Set up the pipe for polled access
    // if (dispatchMode != AbstractTestConnection.BLOCKING) {
    // pipe.setMode(Pipe.POLLING);
    // }
    // // Add subscriptions for the target's destinations:
    // for (Destination d : target.router.lookupTable.keySet()) {
    // router.bind(bc, d);
    // }
    // brokerConns.add(bc);
    // }

    public final void stopServices() throws Exception {
        AbstractTestConnection.setInShutdown(true, dispatcher);

        stopping.set(true);
        transportServer.stop();

        for (BrokerConnection connection : connections) {
            connection.stop();
        }
        for (BrokerConnection connection : brokerConnections) {
            connection.stop();
        }
        for (MockQueue queue : queues.values()) {
            queue.stop();
        }
    }

    public final void startServices() throws Exception {
        AbstractTestConnection.setInShutdown(false, dispatcher);

        transportServer = TransportFactory.bind(new URI(uri));
        transportServer.setAcceptListener(this);
        if (transportServer instanceof DispatcherAware) {
            ((DispatcherAware) transportServer).setDispatcher(dispatcher);
        }
        transportServer.start();

        for (MockQueue queue : queues.values()) {
            queue.start();
        }

        for (BrokerConnection connection : brokerConnections) {
            connection.start();
        }
    }

    public void onAccept(final Transport transport) {
        BrokerConnection connection = new BrokerConnection();
        connection.setBroker(this);
        connection.setTransport(transport);
        connection.setPriorityLevels(MockBrokerTest.PRIORITY_LEVELS);
        connection.setDispatcher(dispatcher);
        connection.setUseInputQueue(useInputQueues);
        connections.add(connection);
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

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setDispatcher(Dispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    public URI getConnectURI() {
        return transportServer.getConnectURI();
    }

    public boolean isStopping() {
        return stopping.get();
    }

    protected void createDispatcher() {
        if (dispatcher == null) {
            dispatcher = DispatcherFactory.create("mock-broker", Runtime.getRuntime().availableProcessors());
        }
    }

    /**
     * Run the broker as a standalone app
     * 
     * @param args
     *            The arguments.
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Must supply a bind uri");
        }
        String uri = args[0];

        final MockBroker broker = new MockBroker();
        broker.setUri(uri);
        broker.setName("Broker");
        broker.createDispatcher();
        try {
            broker.getDispatcher().retain();
            broker.startServices();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    broker.stopServices();
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });
    }

}