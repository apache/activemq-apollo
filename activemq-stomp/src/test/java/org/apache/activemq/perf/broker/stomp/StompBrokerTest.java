package org.apache.activemq.perf.broker.stomp;

import org.apache.activemq.broker.BrokerTestBase;
import org.apache.activemq.broker.RemoteConsumer;
import org.apache.activemq.broker.RemoteProducer;

public class StompBrokerTest extends BrokerTestBase {

    @Override
    protected RemoteProducer cerateProducer() {
        return new StompRemoteProducer();
    }

    @Override
    protected RemoteConsumer createConsumer() {
        return new StompRemoteConsumer();
    }
    
    /* (non-Javadoc)
     * @see org.apache.activemq.broker.BrokerTestBase#getRemoteWireFormat()
     */
    @Override
    protected String getRemoteWireFormat() {
         return "stomp";
    }
}
