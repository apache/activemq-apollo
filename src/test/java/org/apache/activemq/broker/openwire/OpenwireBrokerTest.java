package org.apache.activemq.broker.openwire;

import org.apache.activemq.broker.BrokerTestBase;
import org.apache.activemq.broker.RemoteConsumer;
import org.apache.activemq.broker.RemoteProducer;

public class OpenwireBrokerTest extends BrokerTestBase {

    @Override
    protected RemoteProducer cerateProducer() {
        return new OpenwireRemoteProducer();
    }

    @Override
    protected RemoteConsumer createConsumer() {
        return new OpenwireRemoteConsumer();
    }
    

}
