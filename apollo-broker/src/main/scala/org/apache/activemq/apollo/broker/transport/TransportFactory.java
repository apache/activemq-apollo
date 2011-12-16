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
package org.apache.activemq.apollo.broker.transport;

import org.fusesource.hawtdispatch.transport.Transport;
import org.fusesource.hawtdispatch.transport.TransportServer;
import org.apache.activemq.apollo.util.ClassFinder;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class TransportFactory {

    public interface Provider {
        public TransportServer bind(String location) throws Exception;
        public Transport connect(String location) throws Exception;
    }

    public static final ClassFinder<Provider> providers = new ClassFinder<Provider>("META-INF/services/org.apache.activemq.apollo/transport-factory.index", Provider.class);

    /**
     * Creates a client transport.
     */
    public static Transport connect(String location) throws Exception {
        for( Provider provider : providers.jsingletons()) {
          Transport rc = provider.connect(location);
          if( rc!=null ) {
            return rc;
          }
        }
        throw new IllegalArgumentException("Unknown transport connect uri: "+location);
    }

    /**
     * Creates a transport server.
     */
    public static TransportServer bind(String location) throws Exception {
        for( Provider spi : providers.jsingletons()) {
          TransportServer rc = spi.bind(location);
          if( rc!=null ) {
            return rc;
          }
        }
        throw new IllegalArgumentException("Unknown transport bind uri: "+location);
    }

}
