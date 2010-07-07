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
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.activemq.wireformat.WireFormatFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class  TransportFactorySupport{

    private static final FactoryFinder WIREFORMAT_FACTORY_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/wireformat/");

    private static final String WRITE_TIMEOUT_FILTER = "soWriteTimeout";
    private static final String THREAD_NAME_FILTER = "threadName";

    static public Transport configure(Transport transport, Map<String, String> options) throws IOException {
        WireFormat wf = createWireFormat(options);
        transport.setWireformat(wf);
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

    static public WireFormat createWireFormat(Map<String, String> options) throws IOException {
        WireFormatFactory factory = createWireFormatFactory(options);
        if( factory == null ) {
            return null;
        }
        WireFormat format = factory.createWireFormat();
        return format;
    }

    static public WireFormatFactory createWireFormatFactory(Map<String, String> options) throws IOException {
        String wireFormat = (String)options.remove("wireFormat");
        if (wireFormat == null) {
            wireFormat = getDefaultWireFormatType();
        }
        if( "null".equals(wireFormat) ) {
            return null;
        }

        try {
            WireFormatFactory wff = (WireFormatFactory)WIREFORMAT_FACTORY_FINDER.newInstance(wireFormat);
            IntrospectionSupport.setProperties(wff, options, "wireFormat.");
            return wff;
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Could not create wire format factory for: " + wireFormat + ", reason: " + e, e);
        }
    }
    static public WireFormatFactory createWireFormatFactory(String location) throws IOException, URISyntaxException {
        URI uri = new URI(location);
        Map<String, String> options = new HashMap<String, String>(URISupport.parseParamters(uri));

        String wireFormat = uri.getPath();
        if( "null".equals(wireFormat) ) {
            return null;
        }

        try {
            WireFormatFactory wff = (WireFormatFactory)WIREFORMAT_FACTORY_FINDER.newInstance(wireFormat);
            IntrospectionSupport.setProperties(wff, options);
            return wff;
        } catch (Throwable e) {
            throw IOExceptionSupport.create("Could not create wire format factory for: " + wireFormat + ", reason: " + e, e);
        }
    }

    static protected String getDefaultWireFormatType() {
        return "default";
    }

}
