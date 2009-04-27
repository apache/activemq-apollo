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
package org.apache.activemq.broker;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.activemq.broker.DeliveryTarget;
import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.Domain;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.QueueDomain;
import org.apache.activemq.broker.TopicDomain;
import org.apache.activemq.broker.store.BrokerDatabase;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.protobuf.AsciiBuffer;

final public class Router {

    public static final AsciiBuffer TOPIC_DOMAIN = new AsciiBuffer("topic");
    public static final AsciiBuffer QUEUE_DOMAIN = new AsciiBuffer("queue");

    private final HashMap<AsciiBuffer, Domain> domains = new HashMap<AsciiBuffer, Domain>();
    private VirtualHost virtualHost;
    private BrokerDatabase database;

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

    public void route(final BrokerMessageDelivery msg, ISourceController<?> controller) {

        // final Buffer transactionId = msg.getTransactionId();
        // if( msg.isPersistent() ) {
        // VoidCallback<RuntimeException> tx = new
        // VoidCallback<RuntimeException>() {
        // @Override
        // public void run(Session session) throws RuntimeException {
        // Long messageKey = session.messageAdd(msg.createMessageRecord());
        // if( transactionId!=null ) {
        // session.transactionAddMessage(transactionId, messageKey);
        // }
        // }
        // };
        // Runnable onFlush = new Runnable() {
        // public void run() {
        // if( msg.isResponseRequired() ) {
        // // Let the client know the broker got the message.
        // msg.onMessagePersisted();
        // }
        // }
        // };
        // virtualHost.getStore().execute(tx, onFlush);
        // }
        //        
        Collection<DeliveryTarget> targets = route(msg.getDestination(), msg);

        //Set up the delivery for persistence:
        msg.beginDispatch(database);

        try
        {
            // TODO:
            // Consider doing some caching of this sub list. Most producers
            // always send to the same destination.
            if (targets != null) {
                // The sinks will request persistence via MessageDelivery.persist()
                // if they require persistence:
                for (DeliveryTarget dt : targets) {
                    dt.deliver(msg, controller);
                }
            }
        }
        finally
        {
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

    private Collection<DeliveryTarget> route(Destination destination, MessageDelivery msg) {
        // Handles routing to composite/multi destinations.
        Collection<Destination> destinationList = destination.getDestinations();
        if (destinationList == null) {
            Domain domain = domains.get(destination.getDomain());
            return domain.route(destination.getName(), msg);
        } else {
            HashSet<DeliveryTarget> rc = new HashSet<DeliveryTarget>();
            for (Destination d : destinationList) {
                Collection<DeliveryTarget> t = route(d, msg);
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