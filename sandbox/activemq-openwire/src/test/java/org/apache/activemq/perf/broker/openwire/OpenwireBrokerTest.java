package org.apache.activemq.perf.broker.openwire;

import org.apache.activemq.broker.BrokerTestBase;
import org.apache.activemq.broker.RemoteConsumer;
import org.apache.activemq.broker.RemoteProducer;

public class OpenwireBrokerTest extends BrokerTestBase {

    @Override
    protected RemoteProducer createProducer() {
        return new OpenwireRemoteProducer();
    }

    @Override
    protected RemoteConsumer createConsumer() {
        return new OpenwireRemoteConsumer();
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.broker.BrokerTestBase#getRemoteWireFormat()
     */
    @Override
    protected String getRemoteWireFormat() {
         return "openwire";
    }
    

}
