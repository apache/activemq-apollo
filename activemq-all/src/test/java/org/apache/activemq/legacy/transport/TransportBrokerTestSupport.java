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
package org.apache.activemq.legacy.transport;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.activemq.apollo.broker.Broker;
import org.apache.activemq.apollo.broker.BrokerFactory;
import org.apache.activemq.legacy.openwireprotocol.BrokerTest;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;

public abstract class TransportBrokerTestSupport extends BrokerTest {

    protected Broker createBroker() throws Exception {
    	Broker broker = BrokerFactory.createBroker(new URI("jaxb:classpath:non-persistent-activemq.xml"));
    	broker.addTransportServer(TransportFactory.bind(new URI(getBindLocation())));
        return broker;
    }
    
    protected abstract String getBindLocation();
    protected String getConnectLocation() {
    	return null;
    }

    @Override
    protected Transport createTransport() throws URISyntaxException, Exception {
        String connectLocation = getConnectLocation();
        if( connectLocation==null ) {
        	connectLocation = getBindLocation();
        }
        URI connectURI = new URI(connectLocation);
        
        return TransportFactory.connect(connectURI);    
    }

}
