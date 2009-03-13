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

    public void add(AsciiBuffer destinationName, Object destination);
    
    public Object remove(AsciiBuffer destinationName);

    public void bind(AsciiBuffer destinationName, DeliveryTarget deliveryTarget);

    public Collection<DeliveryTarget> route(AsciiBuffer destinationName, MessageDelivery message);
    
}
