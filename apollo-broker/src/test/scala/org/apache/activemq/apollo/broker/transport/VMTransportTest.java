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
package org.apache.activemq.apollo.broker.transport;

import java.io.IOException;

import org.fusesource.hawtdispatch.internal.util.RunnableCountDownLatch;
import org.fusesource.hawtdispatch.transport.Transport;
import org.fusesource.hawtdispatch.Dispatch;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class VMTransportTest {

	static {
		System.setProperty("org.apache.activemq.default.directory.prefix", "target/test-data/");
	}
	
    @Ignore
	@Test()
	public void autoCreateBroker() throws Exception {
		Transport connect = TransportFactory.connect("vm://test1");
        connect.setDispatchQueue(Dispatch.createQueue());
        RunnableCountDownLatch cd = new RunnableCountDownLatch(1);
        connect.start(cd);
        cd.await();
		assertNotNull(connect);
        cd = new RunnableCountDownLatch(1);
		connect.stop(cd);
        cd.await();
	}
	
	@Test(expected=IOException.class)
	public void noAutoCreateBroker() throws Exception {
		TransportFactory.connect("vm://test2?create=false");
	}

    @Ignore
	@Test(expected=IllegalArgumentException.class)
	public void badOptions() throws Exception {
		TransportFactory.connect("vm://test3?crazy-option=false");
	}
	
}
