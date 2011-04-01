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
package org.apache.activemq.apollo.transport.tcp;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.NoSuchAlgorithmException;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

public class SslTransportFactory extends TcpTransportFactory {


    /**
     * Allows subclasses of TcpTransportFactory to create custom instances of
     * TcpTransportServer.
     */
    protected TcpTransportServer createTcpTransportServer(final URI uri) throws Exception {
        String protocol = protocol(uri.getScheme());
        if( protocol!=null ) {
            return new SslTransportServer(uri).protocol(protocol);
        }
        return null;

    }


    /**
     * Allows subclasses of TcpTransportFactory to create custom instances of
     * TcpTransport.
     */
    protected TcpTransport createTransport(URI uri) throws Exception {
        String protocol = protocol(uri.getScheme());
        if( protocol !=null ) {
            SslTransport rc = new SslTransport();
            rc.setSSLContext(SSLContext.getInstance(protocol));
            return rc;
        }
        return null;
    }

        /**
     * Maps uri schemes to a protocol algorithm names.
     * Valid algorithm names listed at:
     * http://download.oracle.com/javase/6/docs/technotes/guides/security/StandardNames.html#SSLContext
     */
    protected String protocol(String scheme) {
        if( scheme.equals("tls") ) {
            return "TLS";
        } else if( scheme.equals("tlsv1") ) {
            return "TLSv1";
        } else if( scheme.equals("tlsv1.1") ) {
            return "TLSv1.1";
        } else if( scheme.equals("ssl") ) {
            return "SSL";
        } else if( scheme.equals("sslv2") ) {
            return "SSLv2";
        } else if( scheme.equals("sslv3") ) {
            return "SSLv3";
        }
        return null;
    }
}
