/**
 * 
 */
package org.apache.activemq.flow;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.flow.MockBrokerTest.BrokerConnection;
import org.apache.activemq.flow.MockBrokerTest.DeliveryTarget;
import org.apache.activemq.flow.MockBrokerTest.Router;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;

class MockBroker implements TransportAcceptListener {

    private final MockBrokerTest mockBrokerTest;
    private final TestFlowManager flowMgr;
    
    final ArrayList<RemoteConnection> connections = new ArrayList<RemoteConnection>();
    final ArrayList<LocalProducer> producers = new ArrayList<LocalProducer>();
    final ArrayList<LocalConsumer> consumers = new ArrayList<LocalConsumer>();
    private final ArrayList<BrokerConnection> brokerConns = new ArrayList<BrokerConnection>();

    private final HashMap<Destination, MockQueue> queues = new HashMap<Destination, MockQueue>();
    final Router router;
    private int pCount;
    private int cCount;
    private final String name;
    public final int dispatchMode;

    public final IDispatcher dispatcher;
    public final int priorityLevels = MockBrokerTest.PRIORITY_LEVELS;
    public final int ioWorkAmount = MockBrokerTest.IO_WORK_AMOUNT;
    public final boolean useInputQueues = MockBrokerTest.USE_INPUT_QUEUES;
    public TransportServer transportServer;

    MockBroker(MockBrokerTest mockBrokerTest, String name) throws IOException, URISyntaxException {
        this.mockBrokerTest = mockBrokerTest;
        this.flowMgr = new TestFlowManager();
        this.router = this.mockBrokerTest.new Router();
        this.name = name;
        this.dispatchMode = this.mockBrokerTest.dispatchMode;
        this.dispatcher = this.mockBrokerTest.dispatcher;
    }

    TestFlowManager getFlowManager() {
        return flowMgr;
    }

    public String getName() {
        return name;
    }

    public void createProducerConnection(Destination destination) {
        LocalProducer c = new LocalProducer(this.mockBrokerTest, "producer" + ++pCount, this, destination);
        producers.add(c);
    }

    public void createConsumerConnection(Destination destination) {
        LocalConsumer c = new LocalConsumer(this.mockBrokerTest, "consumer" + ++cCount, this, destination);
        consumers.add(c);
        subscribe(destination, c);
    }

    public void subscribe(Destination destination, DeliveryTarget deliveryTarget) {
        if (destination.ptp) {
            queues.get(destination).addConsumer(deliveryTarget);
        } else {
            router.bind(deliveryTarget, destination);
        }
    }

    public void createClusterConnection(Destination destination) {
        LocalConsumer c = new LocalConsumer(this.mockBrokerTest, "consumer" + ++cCount, this, destination);
        consumers.add(c);
        router.bind(c, destination);
    }

    public void createQueue(Destination destination) {
        MockQueue queue = new MockQueue(this.mockBrokerTest, this, destination);
        queues.put(destination, queue);
    }

    public void createBrokerConnection(MockBroker target, Pipe<Message> pipe) {
        BrokerConnection bc = this.mockBrokerTest.new BrokerConnection(this, target, pipe);
        // Set up the pipe for polled access
        if (dispatchMode != AbstractTestConnection.BLOCKING) {
            pipe.setMode(Pipe.POLLING);
        }
        // Add subscriptions for the target's destinations:
        for (Destination d : target.router.lookupTable.keySet()) {
            router.bind(bc, d);
        }
        brokerConns.add(bc);
    }

    final void stopServices() throws Exception {
        transportServer.stop();
        
        for (RemoteConnection connection : connections) {
            connection.stop();
        }
        for (LocalProducer connection : producers) {
            connection.stop();
        }
        for (LocalConsumer connection : consumers) {
            connection.stop();
        }
        for (BrokerConnection connection : brokerConns) {
            connection.stop();
        }
        for (MockQueue queue : queues.values()) {
            queue.stop();
        }
        dispatcher.shutdown();

    }

    final void startServices() throws Exception {
        
        transportServer = TransportFactory.bind(new URI("tcp://localhost:61616?wireFormat=test"));
        transportServer.setAcceptListener(this);
        transportServer.start();
        
        dispatcher.start();

        for (LocalConsumer connection : consumers) {
            connection.start();
        }

        for (MockQueue queue : queues.values()) {
            queue.start();
        }
        
        for (LocalProducer connection : producers) {
            connection.start();
        }

        for (BrokerConnection connection : brokerConns) {
            connection.start();
        }
    }

    public void onAccept(final Transport transport) {
        RemoteConnection connection = new RemoteConnection();
        connection.setBroker(this);
        connection.setTransport(transport);
        try {
            connection.start();
        } catch (Exception e1) {
            onAcceptError(e1);
        }
    }

    public void onAcceptError(Exception error) {
        error.printStackTrace();
    }
}