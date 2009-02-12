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
import org.apache.activemq.flow.MockBrokerTest.Router;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;

class MockBroker implements TransportAcceptListener {

    private final MockBrokerTest mockBrokerTest;
    private final TestFlowManager flowMgr;
    
    final ArrayList<MockTransportConnection> connections = new ArrayList<MockTransportConnection>();
    final ArrayList<MockProducerConnection> producers = new ArrayList<MockProducerConnection>();
    final ArrayList<MockConsumerConnection> consumers = new ArrayList<MockConsumerConnection>();
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
    private TransportServer transportServer;

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
        MockProducerConnection c = new MockProducerConnection(this.mockBrokerTest, "producer" + ++pCount, this, destination);
        producers.add(c);
    }

    public void createConsumerConnection(Destination destination, boolean ptp) {
        MockConsumerConnection c = new MockConsumerConnection(this.mockBrokerTest, "consumer" + ++cCount, this, destination);
        consumers.add(c);
        if (ptp) {
            queues.get(destination).addConsumer(c);
        } else {
            router.bind(c, destination);
        }

    }

    public void createClusterConnection(Destination destination) {
        MockConsumerConnection c = new MockConsumerConnection(this.mockBrokerTest, "consumer" + ++cCount, this, destination);
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
        
        for (MockTransportConnection connection : connections) {
            connection.stop();
        }
        for (MockProducerConnection connection : producers) {
            connection.stop();
        }
        for (MockConsumerConnection connection : consumers) {
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

        for (MockConsumerConnection connection : consumers) {
            connection.start();
        }

        for (MockQueue queue : queues.values()) {
            queue.start();
        }
        
        for (MockProducerConnection connection : producers) {
            connection.start();
        }

        for (BrokerConnection connection : brokerConns) {
            connection.start();
        }
    }

    public void onAccept(final Transport transport) {
        MockTransportConnection connection = new MockTransportConnection();
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