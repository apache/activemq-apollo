package org.apache.activemq.queue.perf;

import java.net.URI;

import org.apache.activemq.transport.DispatchableTransport;
import org.apache.activemq.transport.TransportFactory;

public abstract class ClientConnection extends AbstractTestConnection{

    private URI connectUri;
    
    public void setConnectUri(URI uri) {
        this.connectUri = uri;
    }

    public void start() throws Exception {
        transport = TransportFactory.compositeConnect(connectUri);
        transport.setTransportListener(this);
        super.setTransport(transport);
        super.initialize();
        super.start();
        // Let the remote side know our name.
        write(name);
    }

}
