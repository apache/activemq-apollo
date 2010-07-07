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
package org.apache.activemq.transport;

import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IOExceptionSupport;
import org.fusesource.hawtdispatch.DispatchQueue;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TransportFactory {

    private static final FactoryFinder TRANSPORT_FACTORY_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/transport/");
    private static final ConcurrentHashMap<String, TransportFactorySPI> TRANSPORT_FACTORYS = new ConcurrentHashMap<String, TransportFactorySPI>();

    public interface TransportFactorySPI {
        public TransportServer bind(String location) throws Exception;
        public Transport connect(String location) throws Exception;
    }
    
    /**
     */
    private static TransportFactorySPI factory(String location) throws IOException {
        String scheme = FactoryFinder.getScheme(location);
        if (scheme == null) {
            throw new IOException("Transport not scheme specified: [" + location + "]");
        }
        TransportFactorySPI tf = TRANSPORT_FACTORYS.get(scheme);
        if (tf == null) {
            // Try to load if from a META-INF property.
            try {
                tf = (TransportFactorySPI)TRANSPORT_FACTORY_FINDER.newInstance(scheme);
                TRANSPORT_FACTORYS.put(scheme, tf);
            } catch (Throwable e) {
                throw IOExceptionSupport.create("Transport scheme NOT recognized: [" + scheme + "]", e);
            }
        }
        return tf;
    }


     /**
      * Allow registration of a transport factory without wiring via META-INF classes
     * @param scheme
     * @param tf
     */
    public static void registerTransportFactory(String scheme, TransportFactorySPI tf) {
        TRANSPORT_FACTORYS.put(scheme, tf);
    }

    /**
     * Creates a client transport.
     */
    public static Transport connect(String location) throws Exception {
        return factory(location).connect(location);
    }

    /**
     * Creates a transport server.
     */
    public static TransportServer bind(String location) throws Exception {
        return factory(location).bind(location);
    }


}
