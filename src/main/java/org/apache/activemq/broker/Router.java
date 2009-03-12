/**
 * 
 */
package org.apache.activemq.broker;

import java.util.Collection;
import java.util.HashMap;

import org.apache.activemq.protobuf.AsciiBuffer;

final public class Router {
    
    public static final AsciiBuffer TOPIC_DOMAIN = new AsciiBuffer("topic");
    public static final AsciiBuffer QUEUE_DOMAIN = new AsciiBuffer("queue");
    
    private final HashMap<AsciiBuffer, Domain> domains = new HashMap<AsciiBuffer, Domain>();
    
    public Router() {
        domains.put(QUEUE_DOMAIN, new QueueDomain());
        domains.put(TOPIC_DOMAIN, new TopicDomain());
    }
    
    public Domain getDomain(AsciiBuffer name) {
        return domains.get(name);
    }

    public Domain putDomain(AsciiBuffer name, Domain domain) {
        return domains.put(name, domain);
    }

    public Domain removeDomain(Object name) {
        return domains.remove(name);
    }

    
    public synchronized void bind(Destination destination, DeliveryTarget dt) {
        Domain domain = domains.get(destination.getDomain());
        domain.bind(destination.getName(), dt);
    }

    public Collection<DeliveryTarget> route(MessageDelivery msg) {
        Domain domain = domains.get(msg.getDestination().getDomain());
        return domain.route(msg);
    }

}