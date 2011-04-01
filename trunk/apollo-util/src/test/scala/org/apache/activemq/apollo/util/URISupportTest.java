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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class URISupportTest {
    
    @Test()
	public void testEmptyCompositePath() throws Exception {
        URISupport.CompositeData data = URISupport.parseComposite(new URI("broker:()/localhost?persistent=false"));
        assertEquals(0, data.getComponents().length);        
    }
            
    @Test()
	public void testCompositePath() throws Exception {
        URISupport.CompositeData data = URISupport.parseComposite(new URI("test:(path)/path"));
        assertEquals("path", data.getPath());        
        data = URISupport.parseComposite(new URI("test:path"));
        assertNull(data.getPath());
    }

	@Test()
	public void testSimpleComposite() throws Exception {
        URISupport.CompositeData data = URISupport.parseComposite(new URI("test:part1"));
        assertEquals(1, data.getComponents().length);
    }

    @Test()
	public void testComposite() throws Exception {
        URISupport.CompositeData data = URISupport.parseComposite(new URI("test:(part1://host,part2://(sub1://part,sube2:part))"));
        assertEquals(2, data.getComponents().length);
    }

    @Test()
	public void testParsingURI() throws Exception {
        URI source = new URI("tcp://localhost:61626/foo/bar?cheese=Edam&x=123");
        
        Map map = URISupport.parseParamters(source);
    
        assertEquals(("Size: " + map), 2, map.size());
        assertMapKey(map, "cheese", "Edam");
        assertMapKey(map, "x", "123");
        
        URI result = URISupport.removeQuery(source);
        
        assertEquals("result", new URI("tcp://localhost:61626/foo/bar"), result);
    }
    
    protected void assertMapKey(Map map, String key, Object expected) {
        assertEquals(("Map key: " + key), map.get(key), expected);
    }
    
    @Test()
	public void testParsingCompositeURI() throws URISyntaxException {
        URISupport.parseComposite(new URI("broker://(tcp://localhost:61616)?name=foo"));
    }
    
    @Test()
	public void testCheckParenthesis() throws Exception {
        String str = "fred:(((ddd))";
        assertFalse(URISupport.checkParenthesis(str));
        str += ")";
        assertTrue(URISupport.checkParenthesis(str));
    }
    
}
