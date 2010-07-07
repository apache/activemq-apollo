/**
 * 
 */
package org.apache.activemq.queue.actor.perf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;

import org.apache.activemq.dispatch.DispatchQueue;
import org.apache.activemq.flow.Commands.Destination;
import org.fusesource.hawtbuf.AsciiBuffer;

import static org.apache.activemq.dispatch.internal.RunnableSupport.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class Router {
    final HashMap<AsciiBuffer, Collection<DeliveryTarget>> lookupTable = new HashMap<AsciiBuffer, Collection<DeliveryTarget>>();

    final synchronized void bind(DeliveryTarget dt, Destination destination) {
        AsciiBuffer key = destination.getName();
        Collection<DeliveryTarget> targets = lookupTable.get(key);
        if (targets == null) {
            targets = new ArrayList<DeliveryTarget>();
            lookupTable.put(key, targets);
        }
        targets.add(dt);
    }

    final void route(Message msg, DispatchQueue queue, Runnable onRouteCompleted) {
        AsciiBuffer key = msg.getDestination().getName();
        Collection<DeliveryTarget> targets = lookupTable.get(key);
        if( targets == null )  {
            onRouteCompleted.run();
            return;
        }
        
        Runnable r = runOnceAfter(queue, onRouteCompleted, targets.size());
        for (DeliveryTarget dt : targets) {
            if ( dt.match(msg) ) {
                dt.add(msg, r);
            }
        }
    }
}
