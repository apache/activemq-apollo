/**
 * 
 */
package org.apache.activemq.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.activemq.flow.MockBroker.DeliveryTarget;

public class Router {
    final HashMap<Destination, Collection<DeliveryTarget>> lookupTable = new HashMap<Destination, Collection<DeliveryTarget>>();

    final synchronized void bind(DeliveryTarget dt, Destination destination) {
        Collection<DeliveryTarget> targets = lookupTable.get(destination);
        if (targets == null) {
            targets = new ArrayList<DeliveryTarget>();
            lookupTable.put(destination, targets);
        }
        targets.add(dt);
    }

    final void route(ISourceController<Message> source, Message msg) {
        Collection<DeliveryTarget> targets = lookupTable.get(msg.getDestination());
        if( targets == null ) 
            return;
        for (DeliveryTarget dt : targets) {
            if (dt.match(msg)) {
                dt.getSink().add(msg, source);
            }
        }
    }
}