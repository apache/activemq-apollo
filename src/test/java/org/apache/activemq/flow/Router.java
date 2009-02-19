/**
 * 
 */
package org.apache.activemq.flow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.activemq.flow.Commands.Destination;
import org.apache.activemq.flow.MockBroker.DeliveryTarget;

public class Router {
    final HashMap<String, Collection<DeliveryTarget>> lookupTable = new HashMap<String, Collection<DeliveryTarget>>();

    final synchronized void bind(DeliveryTarget dt, Destination destination) {
        String key = destination.getName();
        Collection<DeliveryTarget> targets = lookupTable.get(key);
        if (targets == null) {
            targets = new ArrayList<DeliveryTarget>();
            lookupTable.put(key, targets);
        }
        targets.add(dt);
    }

    final void route(ISourceController<Message> source, Message msg) {
        String key = msg.getDestination().getName();
        Collection<DeliveryTarget> targets = lookupTable.get(key);
        if( targets == null ) 
            return;
        for (DeliveryTarget dt : targets) {
            if (dt.match(msg)) {
                dt.getSink().add(msg, source);
            }
        }
    }
}