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

import java.util.Collection;
import java.util.HashSet;

import org.apache.activemq.protobuf.AsciiBuffer;

public interface Destination {

    AsciiBuffer getDomain();

    AsciiBuffer getName();

    Collection<Destination> getDestinations();

    public class SingleDestination implements Destination {

        private AsciiBuffer domain;
        private AsciiBuffer name;

        public SingleDestination() {
        }

        public SingleDestination(AsciiBuffer domain, AsciiBuffer name) {
            setDomain(domain);
            setName(name);
        }

        public SingleDestination(String domain, String name) {
            setDomain(domain);
            setName(name);
        }

        public Collection<Destination> getDestinations() {
            return null;
        }

        public AsciiBuffer getDomain() {
            return domain;
        }

        public AsciiBuffer getName() {
            return name;
        }

        public void setDomain(AsciiBuffer domain) {
            this.domain = domain;
        }

        public void setName(AsciiBuffer name) {
            this.name = name;
        }

        private void setName(String name) {
            setName(new AsciiBuffer(name));
        }

        private void setDomain(String domain) {
            setDomain(new AsciiBuffer(domain));
        }

        //        public ActiveMQDestination asActiveMQDestination() {
        //            if(domain.equals(Router.TOPIC_DOMAIN))
        //            {
        //                return new ActiveMQTopic(name.toString());
        //            }
        //            else if(domain.equals(Router.QUEUE_DOMAIN))
        //            {
        //                return new ActiveMQQueue(name.toString());
        //            }
        //            return null;
        //        }
    }

    public class MultiDestination implements Destination {

        private final HashSet<Destination> destinations = new HashSet<Destination>();
        
        public MultiDestination() {
        }


        public Collection<Destination> getDestinations() {
            return destinations;
        }

        public AsciiBuffer getDomain() {
            return null;
        }

        public AsciiBuffer getName() {
            return null;
        }

        public void add(Destination d) {
            destinations.add(d);
            
        }
        
        public void remove(Destination d)
        {
            destinations.remove(d);
        }

        //        public ActiveMQDestination asActiveMQDestination() {
        //            throw new UnsupportedOperationException("Not yet implemented");
        //        }

    }

}
