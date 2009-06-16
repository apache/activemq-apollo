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
package org.apache.activemq.apollo.transport.vm;

import java.io.IOException;
import java.net.URI;

import junit.framework.TestCase;

import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;

/**
 * 
 * @author chirino
 */
public class VMTransportTest extends TestCase {

	static {
		System.setProperty("org.apache.activemq.default.directory.prefix", "target/test-data/");
	}
	
	public void testAutoCreateBroker() throws Exception {
		Transport connect = TransportFactory.compositeConnect(new URI("vm://test?wireFormat=mock"));
		connect.start();
		assertNotNull(connect);
		connect.stop();
	}
	
	public void testNoAutoCreateBroker() throws Exception {
		try {
			TransportFactory.compositeConnect(new URI("vm://test?create=false&wireFormat=mock"));
			fail("Expected a IOException");
		} catch (IOException e) {
		}
	}
	
	public void testBadOptions() throws Exception {
		try {
			TransportFactory.compositeConnect(new URI("vm://test?crazy-option=false&wireFormat=mock"));
			fail("Expected a IllegalArgumentException");
		} catch (IllegalArgumentException e) {
		}
	}
	
}
