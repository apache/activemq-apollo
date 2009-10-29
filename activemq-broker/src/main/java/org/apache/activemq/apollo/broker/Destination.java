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

import org.apache.activemq.util.buffer.AsciiBuffer;
import org.apache.activemq.util.buffer.Buffer;

import static org.apache.activemq.util.buffer.AsciiBuffer.*;

public interface Destination {

    AsciiBuffer getDomain();
    AsciiBuffer getName();
    Collection<Destination> getDestinations();
    
    public static class ParserOptions {
        public AsciiBuffer defaultDomain;
        public AsciiBuffer queuePrefix;
        public AsciiBuffer topicPrefix;
        public AsciiBuffer tempQueuePrefix;
        public AsciiBuffer tempTopicPrefix;
    }

    static public class Parser {
        
        /**
         * Parses a simple destination.
         * 
         * @param value
         * @param options
         * @return
         */
        public static Destination parse(AsciiBuffer value, ParserOptions options) {
            if (options.queuePrefix!=null && value.startsWith(options.queuePrefix)) {
                AsciiBuffer name = value.slice(options.queuePrefix.length, value.length).ascii();
                return new SingleDestination(Router.QUEUE_DOMAIN, name);
            } else if (options.topicPrefix!=null && value.startsWith(options.topicPrefix)) {
                AsciiBuffer name = value.slice(options.topicPrefix.length, value.length).ascii();
                return new SingleDestination(Router.TOPIC_DOMAIN, name);
            } else if (options.tempQueuePrefix!=null && value.startsWith(options.tempQueuePrefix)) {
                AsciiBuffer name = value.slice(options.tempQueuePrefix.length, value.length).ascii();
                return new SingleDestination(Router.TEMP_QUEUE_DOMAIN, name);
            } else if (options.tempTopicPrefix!=null && value.startsWith(options.tempTopicPrefix)) {
                AsciiBuffer name = value.slice(options.tempTopicPrefix.length, value.length).ascii();
                return new SingleDestination(Router.TEMP_TOPIC_DOMAIN, name);
            } else {
                if( options.defaultDomain==null ) {
                    throw new IllegalArgumentException("Destination domain not provided: "+value);
                }
                return new SingleDestination(options.defaultDomain, value);
            }
        }
        
        /**
         * Parses a destination which may or may not be a composite.
         * 
         * @param value
         * @param options
         * @param compositeSeparator
         * @return
         */
        public static Destination parse(AsciiBuffer value, ParserOptions options, byte compositeSeparator) {
            if( value == null ) {
                return null;
            }

            if( value.contains(compositeSeparator) ) {
                Buffer[] rc = value.split(compositeSeparator);
                MultiDestination md = new MultiDestination();
                for (Buffer buffer : rc) {
                    md.add(parse(ascii(buffer), options));
                }
                return md;
            }
            return parse(ascii(value), options);
        }        
    }
    
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

        @Override
        public String toString() {
        	return ""+domain+":"+name;
        }
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
        
        @Override
        public String toString() {
        	return destinations.toString();
        }
    }

}
