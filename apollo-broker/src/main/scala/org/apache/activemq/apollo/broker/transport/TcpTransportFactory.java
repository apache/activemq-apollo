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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.fusesource.hawtdispatch.transport.TcpTransport;
import org.fusesource.hawtdispatch.transport.TcpTransportServer;
import org.fusesource.hawtdispatch.transport.Transport;
//import org.apache.activemq.transport.TransportLoggerFactory;
import org.fusesource.hawtdispatch.transport.TransportServer;
import org.apache.activemq.apollo.util.IntrospectionSupport;
import org.apache.activemq.apollo.util.URISupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.activemq.apollo.broker.transport.TransportFactorySupport.configure;
import static org.apache.activemq.apollo.broker.transport.TransportFactorySupport.verify;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 * @author David Martin Clavo david(dot)martin(dot)clavo(at)gmail.com (logging improvement modifications)
 */
public class TcpTransportFactory implements TransportFactory.Provider {
    private static final Logger LOG = LoggerFactory.getLogger(TcpTransportFactory.class);

    public TransportServer bind(String location) throws Exception {

        URI uri = new URI(location);
        Map<String, String> options = new HashMap<String, String>(URISupport.parseParamters(uri));

        TcpTransportServer server = createTcpTransportServer(uri, options);
        if (server == null) return null;

        Map<String, String> copy = new HashMap<String, String>(options);
        IntrospectionSupport.setProperties(server, new HashMap(options));
        return server;
    }


    public Transport connect(String location) throws Exception {
        URI uri = new URI(location);
        TcpTransport transport = createTransport(uri);
        if (transport == null) return null;

        Map<String, String> options = new HashMap<String, String>(URISupport.parseParamters(uri));
        URI localLocation = getLocalLocation(uri);
        configure(transport, options);
        verify(transport, options);

        transport.connecting(uri, localLocation);

        return transport;
    }

    /**
     * Allows subclasses of TcpTransportFactory to create custom instances of
     * TcpTransportServer.
     */
    protected TcpTransportServer createTcpTransportServer(final URI location, final Map<String, String> options) throws IOException, URISyntaxException, Exception {
        if( !location.getScheme().equals("tcp") ) {
            return null;
        }

        return new TcpTransportServer(location) {
            @Override
            protected TcpTransport createTransport() {
                TcpTransport transport = super.createTransport();
                IntrospectionSupport.setProperties(transport, new HashMap(options));
                return transport;
            }
        };
    }

    /**
     * Allows subclasses of TcpTransportFactory to create custom instances of
     * TcpTransport.
     */
    protected TcpTransport createTransport(URI uri) throws NoSuchAlgorithmException, Exception {
        if( !uri.getScheme().equals("tcp") ) {
            return null;
        }
        TcpTransport transport = new TcpTransport();
        return transport;
    }

    protected URI getLocalLocation(URI location) {
        URI localLocation = null;
        String path = location.getPath();
        // see if the path is a local URI location
        if (path != null && path.length() > 0) {
            int localPortIndex = path.indexOf(':');
            try {
                Integer.parseInt(path.substring(localPortIndex + 1, path.length()));
                String localString = location.getScheme() + ":/" + path;
                localLocation = new URI(localString);
            } catch (Exception e) {
                LOG.warn("path isn't a valid local location for TcpTransport to use", e);
            }
        }
        return localLocation;
    }

    protected String getOption(Map options, String key, String def) {
        String rc = (String) options.remove(key);
        if( rc == null ) {
            rc = def;
        }
        return rc;
    }

}
