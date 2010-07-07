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
package org.apache.activemq.queue.actor.perf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.dispatch.Dispatcher;
import org.apache.activemq.dispatch.DispatcherConfig;
import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.queue.actor.transport.Transport;
import org.apache.activemq.queue.actor.transport.TransportFactorySystem;
import org.apache.activemq.queue.actor.transport.TransportServer;
import org.apache.activemq.queue.actor.transport.TransportServerHandler;

import static java.util.concurrent.TimeUnit.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class MockBroker implements TransportServerHandler {

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

    private DispatchQueue brokerDispatchQueue;

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

    public final void stopServices() throws Exception {
        stopping.set(true);
        transportServer.release();

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

    CountDownLatch bindLatch = new CountDownLatch(1);
    
    public final void startServices() throws Exception {
        brokerDispatchQueue = dispatcher.createSerialQueue("broker");
        transportServer = TransportFactorySystem.bind(dispatcher, uri);
        transportServer.setTargetQueue(brokerDispatchQueue);
        transportServer.setHandler(this);
        transportServer.resume();
        
        for (MockQueue queue : queues.values()) {
            queue.start();
        }
        
        for (BrokerConnection connection : brokerConnections) {
            connection.start();
        }
        
        if( !bindLatch.await(5, SECONDS) ) {
            throw new Exception("bind timeout");
        }
    }

    public void onFailure(Exception error) {
        bindLatch.countDown();
        System.out.println("Accept error: " + error);
        error.printStackTrace();
    }

    public void onBind() {
        bindLatch.countDown();
    }
    
    public void onUnbind() {
    }
    
    public void onAccept(final Transport transport) {
        BrokerConnection connection = new BrokerConnection();
        connection.setBroker(this);
        connection.setTransport(transport);
        connection.setPriorityLevels(MockBrokerTest.PRIORITY_LEVELS);
        connection.setDispatcher(dispatcher);
        connection.setName("handler: "+transport.getRemoteAddress());
        connections.add(connection);
        try {
            connection.start();
        } catch (Exception e1) {
            onFailure(e1);
        }
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

    public String getConnectURI() {
        return transportServer.getConnectURI();
    }

    public boolean isStopping() {
        return stopping.get();
    }

    protected void createDispatcher() {
        if (dispatcher == null) {
            dispatcher = DispatcherConfig.create("mock-broker", Runtime.getRuntime().availableProcessors());
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
            broker.getDispatcher().resume();
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

    public void onAccept(org.apache.activemq.transport.Transport transport) {
    }

}