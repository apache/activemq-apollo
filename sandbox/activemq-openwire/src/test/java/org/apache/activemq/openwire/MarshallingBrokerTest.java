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
package org.apache.activemq.openwire;

import org.apache.activemq.apollo.Combinator;
import org.apache.activemq.apollo.Combinator.BeanFactory;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.experimental.theories.Theories;
import org.junit.runner.RunWith;


/**
 * Runs against the broker but marshals all request and response commands.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@Ignore
@RunWith(Theories.class)
public class MarshallingBrokerTest extends BrokerTest {

    public static BeanFactory<BrokerTestScenario> scenarioFactory() {
        return new BeanFactory<BrokerTestScenario>() {
            public BrokerTestScenario createBean() throws Exception {
                return new BrokerTestScenario() {
                    // TODO: need to figure out a way to inject this guy into 
                    // the transport and transport server...
//                    public OpenWireFormatFactory wireFormat = new OpenWireFormatFactory();
                    
                    @Override
                    public String getBindURI() {
                        return PIPE_URI+"?marshal=true";
                    }
                };
            }

            public Class<BrokerTestScenario> getBeanClass() {
                return BrokerTestScenario.class;
            }
        };
    }
    
    @BeforeClass
    static public void createScenarios() throws Exception {
        OpenWireFormatFactory wf1 = new OpenWireFormatFactory();
        wf1.setCacheEnabled(false);
        OpenWireFormatFactory wf2 = new OpenWireFormatFactory();
        wf2.setCacheEnabled(true);
        
        Combinator combinations = combinations();
        for (Combinator combinator : combinations.all()) {
            combinator.put("wireFormat", wf1, wf2);
        }
        SCENARIOS = combinations.asBeans(scenarioFactory());
    }

}
