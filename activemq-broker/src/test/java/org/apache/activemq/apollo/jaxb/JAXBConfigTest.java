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
package org.apache.activemq.apollo.jaxb;


import java.net.URI;

import junit.framework.TestCase;

import org.apache.activemq.apollo.broker.BrokerFactory;
import org.apache.activemq.apollo.broker.Broker;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JAXBConfigTest extends TestCase {

    private static final Log LOG = LogFactory.getLog(JAXBConfigTest.class);
	
    Broker broker;
    
	@Before
	public void setUp() throws Exception {
		broker = createBroker();
	}

	@After
	public void tearDown() throws Exception {
	}
	
	@Test
	public void testBrokerConfiguredCorrectly() throws Exception {
		
	}

    protected Broker createBroker() throws Exception {
    	URI uri = new URI("jaxb:classpath:org/apache/activemq/apollo/jaxb/activemq.xml");
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(uri);
    }
	
}
