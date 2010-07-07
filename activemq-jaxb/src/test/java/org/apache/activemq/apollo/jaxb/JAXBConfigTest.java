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


import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import junit.framework.TestCase;

import org.apache.activemq.apollo.broker.Broker;
import org.apache.activemq.apollo.broker.BrokerFactory;
import org.apache.activemq.broker.store.memory.MemoryStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class JAXBConfigTest extends TestCase {

    private static final Log LOG = LogFactory.getLog(JAXBConfigTest.class);
	
	@Test()
	public void testSimpleConfig() throws Exception {
		String uri = "jaxb:classpath:org/apache/activemq/apollo/jaxb/testSimpleConfig.xml";
		LOG.info("Loading broker configuration from the classpath with URI: " + uri);
		Broker broker = BrokerFactory.createBroker(uri, false);
		
//		assertEquals(4, p.getSize());
//		assertEquals("test dispatcher", p.getName());
		
		
		assertEquals(1, broker.transportServers().size());
		
		ArrayList<String> expected = new ArrayList<String>();
		expected.add("pipe://test1");
		expected.add("tcp://127.0.0.1:61616");
		assertEquals(expected, broker.connectUris() );
		
		assertEquals(2, broker.virtualHosts().size());
		
		assertNotNull(broker.defaultVirtualHost().getDatabase());
		assertNotNull(broker.defaultVirtualHost().getDatabase().getStore());
		assertTrue((broker.defaultVirtualHost().getDatabase().getStore() instanceof MemoryStore));
		
	}
	
	@Test()
	public void testUris() throws Exception {

        // non-existent classpath
		try {
			String uri = "jaxb:classpath:org/apache/activemq/apollo/jaxb/testUris-fail.xml";
			BrokerFactory.createBroker(uri, false);
            fail("Creating broker from non-existing url does not throw an exception!");
		} catch (RuntimeException e) {
		}

        //non-existent file
		try {
			String uri ="jaxb:file:/org/apache/activemq/apollo/jaxb/testUris-fail.xml";
			BrokerFactory.createBroker(uri, false);
            fail("Creating broker from non-existing url does not throw an exception!");
		} catch (RuntimeException e) {
		}

        //non-existent url
		try {
			String uri = "jaxb:http://localhost/testUris.xml";
			BrokerFactory.createBroker(uri, false);
            fail("Creating broker from non-existing url does not throw an exception!");
		} catch (RuntimeException e) {
		}

		// regular file
		String uri = "jaxb:" + Thread.currentThread().getContextClassLoader().getResource("org/apache/activemq/apollo/jaxb/testUris.xml");
		BrokerFactory.createBroker(uri, false);
	}
	
}
