/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.broker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

final public class Router {
    static final private Log LOG = LogFactory.getLog(Router.class);

    public static final AsciiBuffer TOPIC_DOMAIN = new AsciiBuffer("topic");
    public static final AsciiBuffer QUEUE_DOMAIN = new AsciiBuffer("queue");
    public static final AsciiBuffer TEMP_TOPIC_DOMAIN = new AsciiBuffer("temp-topic");
    public static final AsciiBuffer TEMP_QUEUE_DOMAIN = new AsciiBuffer("temp-queue");

    private final HashMap<AsciiBuffer, Domain> domains = new HashMap<AsciiBuffer, Domain>();

    private VirtualHost virtualHost;
    private BrokerDatabase database;

    public Router() {
        domains.put(QUEUE_DOMAIN, new Domain());
        domains.put(TOPIC_DOMAIN, new Domain());
        domains.put(TEMP_QUEUE_DOMAIN, new Domain());
        domains.put(TEMP_TOPIC_DOMAIN, new Domain());
    }

    public Domain getDomain(Destination destination) {
        return getDomain(destination.getDomain());
    }

    public Domain getDomain(AsciiBuffer name) {
        return domains.get(name);
    }

    public Domain putDomain(AsciiBuffer name, Domain domain) {
        return domains.put(name, domain);
    }

    public Domain removeDomain(AsciiBuffer name) {
        return domains.remove(name);
    }

    public synchronized void bind(Destination destination, DeliveryTarget target) {
        Collection<Destination> destinationList = destination.getDestinations();
        if (destinationList == null) {
            Domain domain = getDomain(destination);
            domain.bind(destination.getName(), target);
        } else {
            for (Destination d : destinationList) {
                bind(d, target);
            }
        }
    }

    public synchronized void unbind(Destination destination, DeliveryTarget target) {
        Collection<Destination> destinationList = destination.getDestinations();
        if (destinationList == null) {
            Domain domain = getDomain(destination);
            domain.unbind(destination.getName(), target);
        } else {
            for (Destination d : destinationList) {
                unbind(d, target);
            }
        }
    }

    public void route(final MessageDelivery msg, ISourceController<?> controller, boolean autoCreate) {

        //If the message is part of transaction send it to the transaction manager
        if(msg.getTransactionId() >= 0)
        {
            virtualHost.getTransactionManager().newMessage(msg, controller);
            return;
        }
        
        Collection<DeliveryTarget> targets = route(msg.getDestination(), msg, autoCreate);
        
        //Set up the delivery for persistence:
        msg.beginDispatch(database);

        try {
            // TODO:
            // Consider doing some caching of this sub list. Most producers
            // always send to the same destination.
            if (targets != null) {
                // The sinks will request persistence via MessageDelivery.persist()
                // if they require persistence:
                for (DeliveryTarget target : targets) {
                    target.deliver(msg, controller);
                }
            }
        } finally {
            try {
                msg.finishDispatch(controller);
            } catch (IOException ioe) {
                //TODO: Error serializing the message, this should trigger an error
                //This is a pretty severe error as we've already delivered
                //the message to the recipients. If we send an error response
                //back it could result in a duplicate. Does this mean that we
                //should persist the message prior to sending to the recips?
                ioe.printStackTrace();
            }
        }
    }

    private Collection<DeliveryTarget> route(Destination destination, MessageDelivery msg, boolean autoCreate) {
        // Handles routing to composite/multi destinations.
        Collection<Destination> destinationList = destination.getDestinations();
        if (destinationList == null) {
            Domain domain = getDomain(destination);
            Collection<DeliveryTarget> rc = domain.route(destination.getName(), msg);
            // We can auto create queues in the queue domain..
            if (rc.isEmpty() && autoCreate && destination.getDomain().equals(Router.QUEUE_DOMAIN)) {
                try {
                    Queue queue = virtualHost.createQueue(destination);
                    rc = new ArrayList<DeliveryTarget>(1);
                    rc.add(queue);
                } catch (Exception e) {
                    LOG.error("Failed to auto create queue: " + destination.getName() + ": " + e);
                    LOG.debug("Failed to auto create queue: " + destination.getName(), e);
                }
            }
            return rc;
        } else {
            HashSet<DeliveryTarget> rc = new HashSet<DeliveryTarget>();
            for (Destination d : destinationList) {
                Collection<DeliveryTarget> t = route(d, msg, autoCreate);
                if (t != null) {
                    rc.addAll(t);
                }
            }
            return rc;
        }
    }

    public void setVirtualHost(VirtualHost virtualHost) {
        this.virtualHost = virtualHost;
    }

    public VirtualHost getVirtualHost() {
        return virtualHost;
    }

    public void setDatabase(BrokerDatabase database) {
        this.database = database;
    }

}