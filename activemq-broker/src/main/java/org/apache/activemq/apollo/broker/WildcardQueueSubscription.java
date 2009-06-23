package org.apache.activemq.apollo.broker;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.activemq.apollo.broker.ProtocolHandler.ConsumerContext;
import org.apache.activemq.apollo.broker.VirtualHost.QueueLifecyleListener;
import org.apache.activemq.apollo.broker.path.PathFilter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class WildcardQueueSubscription implements BrokerSubscription, QueueLifecyleListener {

    static final private Log LOG = LogFactory.getLog(WildcardQueueSubscription.class);
    
    private final VirtualHost host;
    private final Destination destination;
    private final ConsumerContext consumer;
    private final PathFilter filter;
    
    private final ArrayList<BrokerSubscription> childSubs = new ArrayList<BrokerSubscription>();

    public WildcardQueueSubscription(VirtualHost host, Destination destination, ConsumerContext consumer) {
        this.host = host;
        this.destination = destination;
        this.consumer = consumer;
        filter = PathFilter.parseFilter(destination.getName());
    }

    ///////////////////////////////////////////////////////////////////
    // BrokerSubscription interface implementation
    ///////////////////////////////////////////////////////////////////
    public void connect(ConsumerContext cc) throws Exception {
        assert cc == consumer;
        synchronized(host) {
            Domain domain = host.getRouter().getDomain(Router.QUEUE_DOMAIN);
            Collection<DeliveryTarget> matches = domain.route(destination.getName(), null);
            for (DeliveryTarget target : matches) {
                Queue queue = (Queue) target;
                BrokerSubscription childSub = host.createSubscription(consumer, queue.getDestination());
                childSubs.add(childSub);
                childSub.connect(consumer);
            }
            host.addDestinationLifecyleListener(this);
        }
    }

    public void disconnect(ConsumerContext cc) {
        assert cc == consumer;
        synchronized(host) {
            host.removeDestinationLifecyleListener(this);
            for (BrokerSubscription childSub : childSubs) {
                childSub.disconnect(cc);
            }
            childSubs.clear();
        }
    }

    public Destination getDestination() {
        return destination;
    }

    ///////////////////////////////////////////////////////////////////
    // QueueLifecyleListener interface implementation
    ///////////////////////////////////////////////////////////////////
    public void onCreate(Queue queue) {
        if( filter.matches(queue.getDestination().getName()) ) {
            try {
                BrokerSubscription childSub = host.createSubscription(consumer, queue.getDestination());
                childSubs.add(childSub);
                childSub.connect(consumer);
            } catch (Exception e) {
                LOG.warn("Could not create dynamic subscription to "+queue.getDestination()+": "+e);
                LOG.debug("Could not create dynamic subscription to "+queue.getDestination()+": ", e);
            }
        }
    }

    public void onDestroy(Queue queue) {
    }

}
