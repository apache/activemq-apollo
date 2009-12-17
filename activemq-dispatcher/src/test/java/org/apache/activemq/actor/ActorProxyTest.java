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
package org.apache.activemq.actor;

import java.util.concurrent.TimeUnit;

import org.apache.activemq.dispatch.internal.AbstractSerialDispatchQueue;
import org.junit.Test;

import static junit.framework.Assert.*;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ActorProxyTest {

    public static interface TestInterface {
        void strings(String value, String[] value2);
        void shorts(short value, short[] value2);
        String returnString();
    }
    
    static class TestMock implements TestInterface {
        public void shorts(short value, short[] value2) {
            fail();
        }
        public void strings(String value, String[] value2) {
            fail();
        }
        public String returnString() {
            return null;
        }
    }

    private TestMock service;
    private TestInterface proxy;
            
    private AbstractSerialDispatchQueue createQueue() {
        return new AbstractSerialDispatchQueue("mock queue") {
            public void dispatchAsync(Runnable runnable) {
                runnable.run();
            }

            public void dispatchAfter(Runnable runnable, long delay, TimeUnit unit) {
                throw new UnsupportedOperationException("Not implemented");
            }
        };
    }
    
    @Test
    public void strings() throws Exception {

        final String expected1 = "hello";
        final String expected2[] = {"world"};
        
        service = new TestMock() {
            public void strings(String actual1, String[] actual2) {
                assertEquals(expected1, actual1);
                assertEquals(expected2, actual2);
            }
        };
        
        proxy = ActorProxy.create(TestInterface.class, service, createQueue());
        proxy.strings(expected1, expected2);

    }

    @Test
    public void shorts() throws Exception {

        final short expected1 = 37;
        final short expected2[] = {45,37};
        
        service = new TestMock() {
            public void shorts(short actual1, short[] actual2) {
                assertEquals(expected1, actual1);
                assertEquals(expected2, actual2);
            }
        };
        
        proxy = ActorProxy.create(TestInterface.class, service, createQueue());
        proxy.shorts(expected1, expected2);

    }
    
    @Test
    public void returnString() throws Exception {

        service = new TestMock() {
            public String returnString() {
                return "hello";
            }
        };
        
        proxy = ActorProxy.create(TestInterface.class, service, createQueue());
        String actual = proxy.returnString();
        assertNull(actual);

    }}
