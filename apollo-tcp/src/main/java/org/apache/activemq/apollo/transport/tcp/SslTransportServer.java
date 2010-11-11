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

import org.apache.activemq.apollo.transport.KeyManagerAware;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

public class SslTransportServer extends TcpTransportServer implements KeyManagerAware {

    protected KeyManager[] keyManagers;
    protected String protocol = "TLS";
    protected SSLContext sslContext;

    public SslTransportServer(URI location) {
        super(location);
    }

    public void setKeyManagers(KeyManager[] keyManagers) {
        this.keyManagers = keyManagers;
    }

    public void start(Runnable onCompleted) throws Exception {
        if( keyManagers!=null ) {
            sslContext = SSLContext.getInstance(protocol);
            sslContext.init(keyManagers, null, null);
        } else {
            sslContext = SSLContext.getDefault();
        }
        super.start(onCompleted);
    }

    protected TcpTransport createTransport() {
        SslTransport rc = new SslTransport();
        rc.setSSLContext(sslContext);
        return rc;
    }

    protected SslTransportServer protocol(String value) {
        this.protocol = value;
        return this;
    }

}
