package org.apache.activemq.broker;

import java.util.Collection;

import org.apache.activemq.protobuf.AsciiBuffer;

/**
 * Represents a messaging domain like pub/sub or point to point in JMS terms or an Exchange in
 * AMQP terms.
 * 
 * @author chirino
 */
public interface Domain {

    public void add(AsciiBuffer name, Object value);
    
    public Object remove(AsciiBuffer name);

    public void bind(AsciiBuffer name, DeliveryTarget dt);

    public Collection<DeliveryTarget> route(MessageDelivery msg);
    
}
