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
import java.net.URI;

import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;


public final class BrokerFactory {

    private static final FactoryFinder BROKER_FACTORY_HANDLER_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/apollo/broker/");

    public interface Handler {
        Broker createBroker(URI brokerURI) throws Exception;
    }

    
    private BrokerFactory() {        
    }
    
    public static Handler createHandler(String type) throws IOException {
        try {
            return (Handler)BROKER_FACTORY_HANDLER_FINDER.newInstance(type);
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Could not load " + type + " factory:" + e, e);
        }
    }

    /**
     * Creates a broker from a URI configuration
     * 
     * @param brokerURI the URI scheme to configure the broker
     * @throws Exception
     */
    public static Broker createBroker(URI brokerURI) throws Exception {
        return createBroker(brokerURI, false);
    }

    /**
     * Creates a broker from a URI configuration
     * 
     * @param brokerURI the URI scheme to configure the broker
     * @param startBroker whether or not the broker should have its
     *                {@link Broker#start()} method called after
     *                construction
     * @throws Exception
     */
    public static Broker createBroker(URI brokerURI, boolean startBroker) throws Exception {
        if (brokerURI.getScheme() == null) {
            throw new IllegalArgumentException("Invalid broker URI, no scheme specified: " + brokerURI);
        }
        Handler handler = createHandler(brokerURI.getScheme());
        Broker broker = handler.createBroker(brokerURI);
        if (startBroker) {
            broker.start();
        }
        return broker;
    }

    /**
     * Creates a broker from a URI configuration
     * 
     * @param brokerURI the URI scheme to configure the broker
     * @throws Exception
     */
    public static Broker createBroker(String brokerURI) throws Exception {
        return createBroker(new URI(brokerURI));
    }

    /**
     * Creates a broker from a URI configuration
     * 
     * @param brokerURI the URI scheme to configure the broker
     * @param startBroker whether or not the broker should have its
     *                {@link Broker#start()} method called after
     *                construction
     * @throws Exception
     */
    public static Broker createBroker(String brokerURI, boolean startBroker) throws Exception {
        return createBroker(new URI(brokerURI), startBroker);
    }

}
