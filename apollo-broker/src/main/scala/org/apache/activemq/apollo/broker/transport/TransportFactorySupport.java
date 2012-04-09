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
import org.apache.activemq.apollo.util.IntrospectionSupport;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class  TransportFactorySupport {

    static public Transport configure(Transport transport, Map<String, String> options) throws IOException {
        IntrospectionSupport.setProperties(transport, new HashMap(options));
        return transport;
    }

    public static Transport verify(Transport transport, Map<String, String> options) {
        if (!options.isEmpty()) {
            throw new IllegalArgumentException("Invalid connect parameters: " + options);
        }
        return transport;
    }

}
