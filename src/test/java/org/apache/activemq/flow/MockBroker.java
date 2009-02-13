/**
 * 
 */
package org.apache.activemq.flow;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;

class MockBroker implements TransportAcceptListener {

    public interface DeliveryTarget {
        public IFlowSink<Message> getSink();

        public boolean match(Message message);
    }

    final Router router=  new Router();
    
    final ArrayList<RemoteConnection> connections = new ArrayList<RemoteConnection>();
    final ArrayList<RemoteProducer> producers = new ArrayList<RemoteProducer>();
    final ArrayList<RemoteConsumer> consumers = new ArrayList<RemoteConsumer>();
    final ArrayList<BrokerConnection> brokerConnections = new ArrayList<BrokerConnection>();
    final HashMap<Destination, MockQueue> queues = new HashMap<Destination, MockQueue>();
    
    private TransportServer transportServer;
    private String uri;
    private String name;
    private IDispatcher dispatcher;
    private final AtomicBoolean stopping = new AtomicBoolean();

    public String getName() {
        return name;
    }

    public void subscribe(Destination destination, DeliveryTarget deliveryTarget) {
        if (destination.ptp) {
            queues.get(destination).addConsumer(deliveryTarget);
        } else {
            router.bind(deliveryTarget, destination);
        }
    }


    public void addQueue(MockQueue queue) {
        router.bind(queue, queue.getDestination());
        queues.put(queue.getDestination(), queue);
    }

//    public void createClusterConnection(Destination destination) {
//        RemoteConsumer c = new RemoteConsumer(this.mockBrokerTest, "consumer" + ++consumerCounter, this, destination);
//        consumers.add(c);
//        router.bind(c, destination);
//    }
//    public void createBrokerConnection(MockBroker target, Pipe<Message> pipe) {
//        BrokerConnection bc = this.mockBrokerTest.new BrokerConnection(this, target, pipe);
//        // Set up the pipe for polled access
//        if (dispatchMode != AbstractTestConnection.BLOCKING) {
//            pipe.setMode(Pipe.POLLING);
//        }
//        // Add subscriptions for the target's destinations:
//        for (Destination d : target.router.lookupTable.keySet()) {
//            router.bind(bc, d);
//        }
//        brokerConns.add(bc);
//    }

    final void stopServices() throws Exception {
        stopping.set(true);
        transportServer.stop();
        
        for (RemoteProducer connection : producers) {
            connection.stop();
        }
        for (RemoteConsumer connection : consumers) {
            connection.stop();
        }
        for (RemoteConnection connection : connections) {
            connection.stop();
        }
        for (BrokerConnection connection : brokerConnections) {
            connection.stop();
        }
        for (MockQueue queue : queues.values()) {
            queue.stop();
        }
        dispatcher.shutdown();

    }

    final void startServices() throws Exception {
        
        transportServer = TransportFactory.bind(new URI(uri));
        transportServer.setAcceptListener(this);
        transportServer.start();
        
        dispatcher.start();

        for (MockQueue queue : queues.values()) {
            queue.start();
        }

        for (RemoteConsumer connection : consumers) {
            connection.start();
        }
        
        for (RemoteProducer connection : producers) {
            connection.start();
        }

        for (BrokerConnection connection : brokerConnections) {
            connection.start();
        }
    }

    public void onAccept(final Transport transport) {
        RemoteConnection connection = new RemoteConnection();
        connection.setBroker(this);
        connection.setTransport(transport);
        connection.setPriorityLevels(MockBrokerTest.PRIORITY_LEVELS);
        connection.setDispatcher(dispatcher);
        try {
            connection.start();
        } catch (Exception e1) {
            onAcceptError(e1);
        }
    }

    public void onAcceptError(Exception error) {
        System.out.println("Accept error: "+error);
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
    
}