package org.apache.activemq.perf.broker.stomp;

import org.apache.activemq.broker.BrokerTestBase;
import org.apache.activemq.broker.RemoteConsumer;
import org.apache.activemq.broker.RemoteProducer;

public class StompBrokerTest extends BrokerTestBase {

    @Override
    protected RemoteProducer createProducer() {
        return new StompRemoteProducer();
    }

    @Override
    protected RemoteConsumer createConsumer() {
        return new StompRemoteConsumer();
    }
    
    @Override
    protected String getRemoteWireFormat() {
         return "stomp";
    }
}
