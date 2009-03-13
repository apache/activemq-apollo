package org.apache.activemq.broker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.activemq.protobuf.AsciiBuffer;

public class QueueDomain implements Domain {
    
    final HashMap<AsciiBuffer, Queue> queues = new HashMap<AsciiBuffer, Queue>();

    public void add(AsciiBuffer name, Object queue) {
        queues.put(name, (Queue)queue);
    }
    public Object remove(AsciiBuffer name) {
        return queues.remove(name);
    }

    public void bind(AsciiBuffer name, DeliveryTarget deliveryTarget) {
        queues.get(name).addConsumer(deliveryTarget);
    }

    public Collection<DeliveryTarget> route(AsciiBuffer name, MessageDelivery delivery) {
        Queue queue = queues.get(name);
        if( queue!=null ) {
            ArrayList<DeliveryTarget> rc = new ArrayList<DeliveryTarget>(1);
            rc.add(queue);
            return rc;
        }
        return null;
    }

}
