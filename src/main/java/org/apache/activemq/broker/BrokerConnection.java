package org.apache.activemq.broker;

import org.apache.activemq.Connection;

abstract public class BrokerConnection extends Connection {
    
    protected Broker broker;

    public Broker getBroker() {
        return broker;
    }

    public void setBroker(Broker broker) {
        this.broker = broker;
    }
    
    @Override
    public boolean isStopping() {
        return super.isStopping() || broker.isStopping();
    }
    
    
}
