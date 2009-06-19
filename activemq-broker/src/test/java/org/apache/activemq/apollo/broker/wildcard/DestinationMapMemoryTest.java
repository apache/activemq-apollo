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
package org.apache.activemq.apollo.broker.wildcard;

import junit.framework.TestCase;

import org.apache.activemq.apollo.broker.Destination;
import org.apache.activemq.apollo.broker.Router;
import org.apache.activemq.apollo.broker.wildcard.DestinationMap;
import org.apache.activemq.protobuf.AsciiBuffer;

public class DestinationMapMemoryTest extends TestCase {

    public void testLongDestinationPath() throws Exception {
    	Destination d1 = new Destination.SingleDestination(Router.TOPIC_DOMAIN, new AsciiBuffer("1.2.3.4.5.6.7.8.9.10.11.12.13.14.15.16.17.18"));
        DestinationMap<String> map = new DestinationMap<String>();
        map.put(d1, "test");
    }

    public void testVeryLongestinationPaths() throws Exception {

        for (int i = 1; i < 100; i++) {
            String name = "1";
            for (int j = 2; j <= i; j++) {
                name += "." + j;
            }
            // System.out.println("Checking: " + name);
            try {
            	Destination d1 = new Destination.SingleDestination(Router.TOPIC_DOMAIN, new AsciiBuffer(name));
                DestinationMap<String> map = new DestinationMap<String>();
                map.put(d1, "test");
            } catch (Throwable e) {
                fail("Destination name too long: " + name + " : " + e);
            }
        }
    }

}
