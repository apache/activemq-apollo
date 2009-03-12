package org.apache.activemq.broker;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.Connection;
import org.apache.activemq.broker.openwire.OpenwireBrokerConnection;
import org.apache.activemq.dispatch.IDispatcher;
import org.apache.activemq.transport.DispatchableTransportServer;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;

public class Broker implements TransportAcceptListener {

    public static final int MAX_USER_PRIORITY = 10;
    public static final int MAX_PRIORITY = MAX_USER_PRIORITY + 1;
    
    final Router router = new Router();

    final ArrayList<Connection> clientConnections = new ArrayList<Connection>();
    final HashMap<Destination, Queue> queues = new HashMap<Destination, Queue>();

    private TransportServer transportServer;
    private String uri;
    private String name;
    private IDispatcher dispatcher;
    private final AtomicBoolean stopping = new AtomicBoolean();

    public String getName() {
        return name;
    }


    public void addQueue(Queue queue) {
        Domain domain = router.getDomain(queue.getDestination().getDomain());
        domain.add(queue.getDestination().getName(), queue);
    }

    public final void stop() throws Exception {
        stopping.set(true);
        transportServer.stop();
        
        for (Connection connection : clientConnections) {
            connection.stop();
        }
        for (Queue queue : queues.values()) {
            queue.stop();
        }
        dispatcher.shutdown();

    }

    public final void start() throws Exception {

        dispatcher.start();

        transportServer = TransportFactory.bind(new URI(uri));
        transportServer.setAcceptListener(this);
        if (transportServer instanceof DispatchableTransportServer) {
            ((DispatchableTransportServer) transportServer).setDispatcher(dispatcher);
        }
        transportServer.start();

        for (Queue queue : queues.values()) {
            queue.start();
        }
    }

    public void onAccept(final Transport transport) {
        OpenwireBrokerConnection connection = new OpenwireBrokerConnection();
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

    public Router getRouter() {
        return router;
    }

}