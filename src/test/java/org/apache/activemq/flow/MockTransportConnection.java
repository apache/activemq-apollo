package org.apache.activemq.flow;

import java.io.IOException;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportListener;

public class MockTransportConnection implements TransportListener {

    private Transport transport;

    public void setTransport(Transport transport) {
        this.transport = transport;
    }

    public void start() throws Exception {
        transport.setTransportListener(this);
        transport.start();
    }
    
    public void stop() throws Exception {
        transport.stop();

    }

    public void onCommand(Object command) {
    }

    public void onException(IOException error) {
    }

    public void transportInterupted() {
    }
    public void transportResumed() {
    }

}
