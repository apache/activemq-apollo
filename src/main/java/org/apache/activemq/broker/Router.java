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
        return route(msg.getDestination(), msg);
    }

    private Collection<DeliveryTarget> route(Destination destination, MessageDelivery msg) {
        // Handles routing to composite/multi destinations.
        Collection<Destination> destinationList = destination.getDestinations();
        if( destinationList == null ) {
            Domain domain = domains.get(destination.getDomain());
            return domain.route(destination.getName(), msg);
        } else {
            HashSet<DeliveryTarget> rc = new HashSet<DeliveryTarget>();
            for (Destination d : destinationList) {
                Collection<DeliveryTarget> t = route(d, msg);
                if( t!=null ) {
                    rc.addAll(t);
                }
            }            
            return rc;
        }
    }

}