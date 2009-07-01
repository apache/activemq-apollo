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

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.activemq.openwire.BrokerTest;
import org.apache.activemq.openwire.BrokerTestScenario;
import org.apache.activemq.transport.TransportServer;

public abstract class TransportBrokerTestSupport extends BrokerTest {

    protected abstract String getBindLocation();

    /**
     * Need to enhance the BrokerTestScenario a bit to inject the wire format
     */
    @Override
    public Object createBean() throws Exception {
		BrokerTestScenario brokerTestScenario = new BrokerTestScenario() {
			
		    private TransportServer transnportServer;

			@Override
		    public TransportServer createTransnportServer() throws IOException, URISyntaxException {
		    	transnportServer = super.createTransnportServer();
				return transnportServer;
		    }
		    
			@Override
		    public String getConnectURI() {
		    	return transnportServer.getConnectURI().toString();
		    }

			@Override
			public String getBindURI() {
				return getBindLocation();
			}
		    
		};
		return brokerTestScenario;
    }

}
