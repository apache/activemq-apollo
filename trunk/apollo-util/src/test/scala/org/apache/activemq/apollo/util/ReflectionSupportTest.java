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
package org.apache.activemq.apollo.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ReflectionSupportTest {
	
    String favoritesString = "[queue://test, topic://test]";
    String nonFavoritesString = "[topic://test1]";
    
    @Test()
	public void testSetBoolean() {
                  
        TestWitBoolean target = new TestWitBoolean();
        assertTrue((!target.getKeepAlive()));

        IntrospectionSupport.setProperty(target, "keepAlive", "TRUE");
        assertTrue(target.getKeepAlive());
        
        IntrospectionSupport.setProperty(target, "keepAlive", "false");
        assertTrue((!target.getKeepAlive()));
    }

    public static class TestWitBoolean {
        private Boolean keepAlive = new Boolean(false);
        public Boolean getKeepAlive() {
            return keepAlive;
        }
        public void setKeepAlive(Boolean keepAlive) {
            this.keepAlive = keepAlive;
        }
    }
}

