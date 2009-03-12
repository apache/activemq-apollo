/**
 * 
 */
package org.apache.activemq.broker;

import org.apache.activemq.flow.IFlowSink;

public interface DeliveryTarget {
    
    public IFlowSink<MessageDelivery> getSink();
    
    public boolean match(MessageDelivery message);
    
}