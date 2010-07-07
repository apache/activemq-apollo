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

import org.apache.activemq.apollo.Combinator.BeanFactory;
import org.apache.activemq.openwire.BrokerTest;
import org.apache.activemq.openwire.BrokerTestScenario;
import org.apache.activemq.transport.TransportServer;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public abstract class TransportBrokerTestSupport extends BrokerTest {

    public static BeanFactory<BrokerTestScenario> transportScenerios(final String bindLocation) {
        return transportScenerios(bindLocation, 4000);
    }
    
    public static BeanFactory<BrokerTestScenario> transportScenerios(final String bindLocation, final int maxWait) {
        return new BeanFactory<BrokerTestScenario>() {
            public BrokerTestScenario createBean() throws Exception {
                BrokerTestScenario rc = new BrokerTestScenario() {
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
                        return bindLocation;
                    }
                };
                rc.maxWait = maxWait;
                return rc;
            }

            public Class<BrokerTestScenario> getBeanClass() {
                return BrokerTestScenario.class;
            }
        };
    }
    
}
