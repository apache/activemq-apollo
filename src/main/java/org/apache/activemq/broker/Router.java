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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.activemq.broker.DeliveryTarget;
import org.apache.activemq.broker.Destination;
import org.apache.activemq.broker.Domain;
import org.apache.activemq.broker.MessageDelivery;
import org.apache.activemq.broker.QueueDomain;
import org.apache.activemq.broker.TopicDomain;
import org.apache.activemq.broker.store.Store.Callback;
import org.apache.activemq.broker.store.Store.Session;
import org.apache.activemq.broker.store.Store.VoidCallback;
import org.apache.activemq.flow.ISourceController;
import org.apache.activemq.protobuf.AsciiBuffer;
import org.apache.activemq.protobuf.Buffer;

final public class Router {

    public static final AsciiBuffer TOPIC_DOMAIN = new AsciiBuffer("topic");
    public static final AsciiBuffer QUEUE_DOMAIN = new AsciiBuffer("queue");

    private final HashMap<AsciiBuffer, Domain> domains = new HashMap<AsciiBuffer, Domain>();
    private VirtualHost virtualHost;

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

    public void route(final MessageDelivery msg, ISourceController<?> controller) {

//        final Buffer transactionId = msg.getTransactionId();
//        if( msg.isPersistent() ) {
//            VoidCallback<RuntimeException> tx = new VoidCallback<RuntimeException>() {
//                @Override
//                public void run(Session session) throws RuntimeException {
//                    Long messageKey = session.messageAdd(msg.createMessageRecord());
//                    if( transactionId!=null ) {
//                        session.transactionAddMessage(transactionId, messageKey);
//                    }
//                }
//            };
//            Runnable onFlush = new Runnable() {
//                public void run() {
//                    if( msg.isResponseRequired() ) {
//                        // Let the client know the broker got the message.
//                        msg.onMessagePersisted();
//                    }
//                }
//            };
//            virtualHost.getStore().execute(tx, onFlush);
//        }
//        
        Collection<DeliveryTarget> targets = route(msg.getDestination(), msg);

        // TODO:
        // Consider doing some caching of this target list. Most producers
        // always send to the same destination.
        if (targets != null) {

            if (msg.isResponseRequired()) {
                // We need to ack the message once we ensure we won't loose it.
                // We know we won't loose it once it's persisted or delivered to
                // a consumer Setup a callback to get notifed once one of those happens.
                if (!msg.isPersistent()) {
                    // Let the client know the broker got the message.
                    msg.onMessagePersisted();
                }
            }

            // Deliver the message to all the targets..
            for (DeliveryTarget dt : targets) {
                if (dt.match(msg)) {
                    dt.getSink().add(msg, controller);
                }
            }

        } else {
            // Let the client know we got the message even though there
            // were no valid targets to deliver the message to.
            if (msg.isResponseRequired()) {
                msg.onMessagePersisted();
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

}