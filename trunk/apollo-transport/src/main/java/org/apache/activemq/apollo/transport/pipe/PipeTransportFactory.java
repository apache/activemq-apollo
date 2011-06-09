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
package org.apache.activemq.apollo.transport.pipe;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.activemq.apollo.transport.TransportFactorySupport.*;

import org.apache.activemq.apollo.transport.Transport;
import org.apache.activemq.apollo.transport.TransportFactory;
import org.apache.activemq.apollo.transport.TransportServer;
import org.apache.activemq.apollo.util.URISupport;
import org.apache.activemq.apollo.util.IntrospectionSupport;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PipeTransportFactory implements TransportFactory.Provider {

    public static final HashMap<String, PipeTransportServer> servers = new HashMap<String, PipeTransportServer>();

    public TransportServer bind(String location) throws URISyntaxException, IOException {
        if( !location.startsWith("pipe:") ) {
            return null;
        }

        URI uri = new URI(location);
        Map<String, String> options = new HashMap<String, String>(URISupport.parseParamters(uri));
        String node = uri.getHost();
        synchronized(servers) {
            if (servers.containsKey(node)) {
                throw new IOException("Server already bound: " + node);
            }
            PipeTransportServer server = new PipeTransportServer();
            server.setConnectURI(uri);
            server.setName(node);
            IntrospectionSupport.setProperties(server, options);

            if (!options.isEmpty()) {
                throw new IllegalArgumentException("Invalid bind parameters: " + options);
            }
            servers.put(node, server);
            return server;
        }

    }

    public Transport connect(String location) throws IOException, URISyntaxException {
        if( !location.startsWith("pipe:") ) {
            return null;
        }

        URI uri = new URI(location);
        String name = uri.getHost();
        synchronized(servers) {
            PipeTransportServer server = lookup(name);
            if (server == null) {
                throw new IOException("Server is not bound: " + name);
            }
            PipeTransport transport = server.connect();

            Map<String, String> options = new HashMap<String, String>(URISupport.parseParamters(uri));
            return verify( configure(transport, options), options);
        }
    }

	static public PipeTransportServer lookup(String name) {
		synchronized(servers) {
			return servers.get(name);
    	}
	}

    static public Map<String, PipeTransportServer> getServers() {
    	synchronized(servers) {
    		return new HashMap<String, PipeTransportServer>(servers);
    	}
    }

	static public void unbind(PipeTransportServer server) {
		synchronized(servers) {
			servers.remove(server.getName());
		}
    }
}
