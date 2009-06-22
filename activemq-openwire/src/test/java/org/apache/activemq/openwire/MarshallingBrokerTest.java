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

import java.net.URI;

import junit.framework.Test;

import org.apache.activemq.legacy.openwireprotocol.StubConnection;
import org.apache.activemq.openwire.OpenWireFormat;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Runs against the broker but marshals all request and response commands.
 * 
 * @version $Revision$
 */
public class MarshallingBrokerTest extends BrokerTest {

    public WireFormat wireFormat = new OpenWireFormat();

    public void initCombos() {

        OpenWireFormat wf1 = new OpenWireFormat();
        wf1.setCacheEnabled(false);
        OpenWireFormat wf2 = new OpenWireFormat();
        wf2.setCacheEnabled(true);

        addCombinationValues("wireFormat", new Object[] {wf1, wf2});
    }

    
    @Override
    protected String getBindURI() {
        return PIPE_URI+"?marshal=true";
    }

    public static Test suite() {
        return suite(MarshallingBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
