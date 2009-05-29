package org.apache.activemq.broker.openwire.stomp;

import org.apache.activemq.broker.BrokerTestBase;
import org.apache.activemq.broker.RemoteConsumer;
import org.apache.activemq.broker.RemoteProducer;

public class StompBrokerTest extends BrokerTestBase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        if (tcp) {
            sendBrokerBindURI = "tcp://localhost:10000?wireFormat=multi&transport.useInactivityMonitor=false";
            receiveBrokerBindURI = "tcp://localhost:20000?wireFormat=multi&transport.useInactivityMonitor=false";
            sendBrokerConnectURI = "tcp://localhost:10000?wireFormat=stomp&useInactivityMonitor=false";
            receiveBrokerConnectURI = "tcp://localhost:20000?wireFormat=stomp&useInactivityMonitor=false";
        }
    }
    
    @Override
    protected RemoteProducer cerateProducer() {
        return new StompRemoteProducer();
    }

    @Override
    protected RemoteConsumer createConsumer() {
        return new StompRemoteConsumer();
    }
    

}
