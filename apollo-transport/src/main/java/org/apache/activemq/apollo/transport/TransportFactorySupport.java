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
package org.apache.activemq.apollo.transport;

import org.apache.activemq.apollo.util.IOExceptionSupport;
import org.apache.activemq.apollo.util.IntrospectionSupport;
import org.apache.activemq.apollo.util.URISupport;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class  TransportFactorySupport {


    static public Transport configure(Transport transport, Map<String, String> options) throws IOException {
        ProtocolCodec wf = createProtocolCodec(options);
        transport.setProtocolCodec(wf);
        IntrospectionSupport.setProperties(transport, options);
        return transport;
    }

    public static Transport verify(Transport transport, Map<String, String> options) {
        if (!options.isEmpty()) {
            // Release the transport resource as we are erroring out...
            try {
                transport.stop();
            } catch (Throwable cleanup) {
            }
            throw new IllegalArgumentException("Invalid connect parameters: " + options);
        }
        return transport;
    }

    static public ProtocolCodec createProtocolCodec(Map<String, String> options) throws IOException {
        ProtocolCodecFactory.Provider factory = createProtocolCodecFactory(options);
        if( factory == null ) {
            return null;
        }
        ProtocolCodec protocolCodec = factory.createProtocolCodec();
        return protocolCodec;
    }

    static public ProtocolCodecFactory.Provider createProtocolCodecFactory(Map<String, String> options) throws IOException {
        String protocolName = (String)options.remove("protocol");
        if (protocolName == null) {
            protocolName = getDefaultProtocolName();
        }
        if( "null".equals(protocolName) ) {
            return null;
        }
        ProtocolCodecFactory.Provider provider = ProtocolCodecFactory.get(protocolName);
        if( provider==null ) {
            throw new IOException("Could not create protocol codec for: " + protocolName );
        }
        
        return provider;
    }

    static public ProtocolCodecFactory.Provider createProtocolCodecFactory(String location) throws IOException, URISyntaxException {
        URI uri = new URI(location);
        Map<String, String> options = new HashMap<String, String>(URISupport.parseParamters(uri));

        String protocolName = uri.getPath();
        if( "null".equals(protocolName) ) {
            return null;
        }

        ProtocolCodecFactory.Provider provider = ProtocolCodecFactory.get(protocolName);
        if( provider==null ) {
            throw new IOException("Could not create protocol codec for: " + protocolName);
        }
        return provider;
    }

    static protected String getDefaultProtocolName() {
        return "stomp";
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        return super.clone();    //To change body of overridden methods use File | Settings | File Templates.
    }
}
