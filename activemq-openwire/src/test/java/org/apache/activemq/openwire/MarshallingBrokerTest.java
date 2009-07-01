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
import org.testng.annotations.Test;

/**
 * Runs against the broker but marshals all request and response commands.
 * 
 * @version $Revision$
 */
@Test(enabled=false)
public class MarshallingBrokerTest extends BrokerTest {


    /**
     * Makes all the tests run with the OpenWireFormat in both cached and non-cached mode. 
     */
    @Override
    public Combinator combinator() {
    	Combinator combinator = super.combinator();

        OpenWireFormatFactory wf1 = new OpenWireFormatFactory();
        wf1.setCacheEnabled(false);
        OpenWireFormatFactory wf2 = new OpenWireFormatFactory();
        wf2.setCacheEnabled(true);
        combinator.put("wireFormat", wf1, wf2);
        
		return combinator;
    }
    
    /**
     * Need to enhance the BrokerTestScenario a bit to inject the wire format
     */
    @Override
    public Object createBean() throws Exception {
		return new BrokerTestScenario() {
			
			// TODO: need to figure out a way to inject this guy into 
			// the transport and transport server...
		    public OpenWireFormatFactory wireFormat = new OpenWireFormatFactory();
		    
		    @Override
		    public String getBindURI() {
		        return PIPE_URI+"?marshal=true";
		    }
		};
    }

}
