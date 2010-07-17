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
package org.apache.activemq.apollo.util.path;

import java.util.Set;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PathMapMemoryTest {

    @Test()
	public void testLongPath() throws Exception {
    	AsciiBuffer d1 = new AsciiBuffer("1.2.3.4.5.6.7.8.9.10.11.12.13.14.15.16.17.18");
        PathMap<String> map = new PathMap<String>();
        map.put(d1, "test");
    }

    @Test()
	public void testVeryLongPaths() throws Exception {

        for (int i = 1; i < 100; i++) {
            String name = "1";
            for (int j = 2; j <= i; j++) {
                name += "." + j;
            }
            // System.out.println("Checking: " + name);
            try {
            	AsciiBuffer d1 = new AsciiBuffer(name);
                PathMap<String> map = new PathMap<String>();
                map.put(d1, "test");
            } catch (Throwable e) {
                fail(("Destination name too long: " + name + " : " + e));
            }
        }
    }
    
    @Test()
	public void testLotsOfPaths() throws Exception {
        PathMap<Object> map = new PathMap<Object>();
        Object value = new Object();
        int count = 1000;
        for (int i = 0; i < count; i++) {
            AsciiBuffer queue = new AsciiBuffer("connection:"+i);
            map.put(queue, value);
        }
        for (int i = 0; i < count; i++) {
            AsciiBuffer queue = new AsciiBuffer("connection:"+i);
            map.remove(queue, value);
            Set<Object> set = map.get(queue);
            assertTrue(set.isEmpty());
        }
    }    

}
