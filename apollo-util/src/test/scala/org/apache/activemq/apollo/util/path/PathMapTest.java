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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.fusesource.hawtbuf.AsciiBuffer;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class PathMapTest {

    protected AsciiBuffer d1 = createDestination("TEST.D1");
    protected AsciiBuffer d2 = createDestination("TEST.BAR.D2");
    protected AsciiBuffer d3 = createDestination("TEST.BAR.D3");

    protected String v1 = "value1";
    protected String v2 = "value2";
    protected String v3 = "value3";
    protected String v4 = "value4";
    protected String v5 = "value5";
    protected String v6 = "value6";

    @Test()
	public void testCompositePaths() throws Exception {
        AsciiBuffer d1 = createDestination("TEST.BAR.D2");
        AsciiBuffer d2 = createDestination("TEST.BAR.D3");
        PathMap<String> map = new PathMap<String>();
        map.put(d1, v1);
        map.put(d2, v2);
        map.get(createDestination("TEST.BAR.D2,TEST.BAR.D3"));
    }

    @Test()
	public void testSimplePaths() throws Exception {
        PathMap<String> map = new PathMap<String>();
        map.put(d1, v1);
        map.put(d2, v2);
        map.put(d3, v3);

        assertMapValue(map, d1, v1);
        assertMapValue(map, d2, v2);
        assertMapValue(map, d3, v3);
    }

    @Test()
	public void testSimpleDestinationsWithMultipleValues() throws Exception {
        PathMap<String> map = new PathMap<String>();
        map.put(d1, v1);
        map.put(d2, v2);
        map.put(d2, v3);

        assertMapValue(map, d1, v1);
        assertMapValue(map, "TEST.BAR.D2", v2, v3);
        assertMapValue(map, d3);
    }


    @Test()
	public void testLookupOneStepWildcardPaths() throws Exception {
        PathMap<String> map = new PathMap<String>();
        map.put(d1, v1);
        map.put(d2, v2);
        map.put(d3, v3);

        assertMapValue(map, "TEST.D1", v1);
        assertMapValue(map, "TEST.*", v1);
        assertMapValue(map, "*.D1", v1);
        assertMapValue(map, "*.*", v1);

        assertMapValue(map, "TEST.BAR.D2", v2);
        assertMapValue(map, "TEST.*.D2", v2);
        assertMapValue(map, "*.BAR.D2", v2);
        assertMapValue(map, "*.*.D2", v2);

        assertMapValue(map, "TEST.BAR.D3", v3);
        assertMapValue(map, "TEST.*.D3", v3);
        assertMapValue(map, "*.BAR.D3", v3);
        assertMapValue(map, "*.*.D3", v3);

        assertMapValue(map, "TEST.BAR.D4");

        assertMapValue(map, "TEST.BAR.*", v2, v3);
    }

    @Test()
	public void testLookupMultiStepWildcardPaths() throws Exception {
        PathMap<String> map = new PathMap<String>();
        map.put(d1, v1);
        map.put(d2, v2);
        map.put(d3, v3);

        assertMapValue(map, ">", v1, v2, v3);
        assertMapValue(map, "TEST.>", v1, v2, v3);
        assertMapValue(map, "*.>", v1, v2, v3);

        assertMapValue(map, "FOO.>");
    }

    @Test()
	public void testStoreWildcardWithOneStepPath() throws Exception {
        PathMap<String> map = new PathMap<String>();
        put(map, "TEST.*", v1);
        put(map, "TEST.D1", v2);
        put(map, "TEST.BAR.*", v2);
        put(map, "TEST.BAR.D3", v3);

        assertMapValue(map, "FOO");
        assertMapValue(map, "TEST.FOO", v1);
        assertMapValue(map, "TEST.D1", v1, v2);

        assertMapValue(map, "TEST.FOO.FOO");
        assertMapValue(map, "TEST.BAR.FOO", v2);
        assertMapValue(map, "TEST.BAR.D3", v2, v3);

        assertMapValue(map, "TEST.*", v1, v2);
        assertMapValue(map, "*.D1", v1, v2);
        assertMapValue(map, "*.*", v1, v2);
        assertMapValue(map, "TEST.*.*", v2, v3);
        assertMapValue(map, "TEST.BAR.*", v2, v3);
        assertMapValue(map, "*.*.*", v2, v3);
        assertMapValue(map, "*.BAR.*", v2, v3);
        assertMapValue(map, "*.BAR.D3", v2, v3);
        assertMapValue(map, "*.*.D3", v2, v3);
    }

    @Test()
	public void testStoreWildcardInMiddleOfPath() throws Exception {
        PathMap<String> map = new PathMap<String>();
        put(map, "TEST.*", v1);
        put(map, "TEST.D1", v2);
        put(map, "TEST.BAR.*", v2);
        put(map, "TEST.XYZ.D3", v3);
        put(map, "TEST.XYZ.D4", v4);
        put(map, "TEST.BAR.D3", v5);
        put(map, "TEST.*.D2", v6);

        assertMapValue(map, "TEST.*.D3", v2, v3, v5);
        assertMapValue(map, "TEST.*.D4", v2, v4);

        assertMapValue(map, "TEST.*", v1, v2);
        assertMapValue(map, "TEST.*.*", v2, v3, v4, v5, v6);
        assertMapValue(map, "TEST.*.>", v1, v2, v3, v4, v5, v6);
        assertMapValue(map, "TEST.>", v1, v2, v3, v4, v5, v6);
        assertMapValue(map, "TEST.>.>", v1, v2, v3, v4, v5, v6);
        assertMapValue(map, "*.*.D3", v2, v3, v5);
        assertMapValue(map, "TEST.BAR.*", v2, v5, v6);

        assertMapValue(map, "TEST.BAR.D2", v2, v6);
        assertMapValue(map, "TEST.*.D2", v2, v6);
        assertMapValue(map, "TEST.BAR.*", v2, v5, v6);
    }

    @Test()
	public void testDoubleWildcardDoesNotMatchLongerPattern() throws Exception {
        PathMap<String> map = new PathMap<String>();
        put(map, "TEST.*", v1);
        put(map, "TEST.BAR.D3", v2);

        assertMapValue(map, "*.*.D3", v2);
    }

    @Test()
	public void testWildcardAtEndOfPathAndAtBeginningOfSearch() throws Exception {
        PathMap<String> map = new PathMap<String>();
        put(map, "TEST.*", v1);

        assertMapValue(map, "*.D1", v1);
    }

    @Test()
	public void testAnyPathWildcardInMap() throws Exception {
        PathMap<String> map = new PathMap<String>();
        put(map, "TEST.FOO.>", v1);

        assertMapValue(map, "TEST.FOO.BAR.WHANOT.A.B.C", v1);
        assertMapValue(map, "TEST.FOO.BAR.WHANOT", v1);
        assertMapValue(map, "TEST.FOO.BAR", v1);

        assertMapValue(map, "TEST.*.*", v1);
        assertMapValue(map, "TEST.BAR", new Object[]{});

        assertMapValue(map, "TEST.FOO", v1);
    }

    @Test()
	public void testSimpleAddRemove() throws Exception {
        PathMap<String> map = new PathMap<String>();
        put(map, "TEST.D1", v2);

        assertEquals("Root child count", 1, map.getRootNode().getChildCount());

        assertMapValue(map, "TEST.D1", v2);

        remove(map, "TEST.D1", v2);

        assertEquals("Root child count", 0, map.getRootNode().getChildCount());
        assertMapValue(map, "TEST.D1");
    }

    @Test()
	public void testStoreAndLookupAllWildcards() throws Exception {
        PathMap<String> map = new PathMap<String>();
        loadSample2(map);

        assertSample2(map);

        // lets remove everything and add it back
        remove(map, "TEST.FOO", v1);

        assertMapValue(map, "TEST.FOO", v2, v3, v4);
        assertMapValue(map, "TEST.*", v2, v3, v4, v6);
        assertMapValue(map, "*.*", v2, v3, v4, v6);

        remove(map, "TEST.XYZ", v6);

        assertMapValue(map, "TEST.*", v2, v3, v4);
        assertMapValue(map, "*.*", v2, v3, v4);

        remove(map, "TEST.*", v2);

        assertMapValue(map, "TEST.*", v3, v4);
        assertMapValue(map, "*.*", v3, v4);

        remove(map, ">", v4);

        assertMapValue(map, "TEST.*", v3);
        assertMapValue(map, "*.*", v3);

        remove(map, "TEST.>", v3);
        remove(map, "TEST.FOO.BAR", v5);

        assertMapValue(map, "FOO");
        assertMapValue(map, "TEST.FOO");
        assertMapValue(map, "TEST.D1");

        assertMapValue(map, "TEST.FOO.FOO");
        assertMapValue(map, "TEST.BAR.FOO");
        assertMapValue(map, "TEST.FOO.BAR");
        assertMapValue(map, "TEST.BAR.D3");

        assertMapValue(map, "TEST.*");
        assertMapValue(map, "*.*");
        assertMapValue(map, "*.D1");
        assertMapValue(map, "TEST.*.*");
        assertMapValue(map, "TEST.BAR.*");

        loadSample2(map);

        assertSample2(map);

        remove(map, ">", v4);
        remove(map, "TEST.*", v2);

        assertMapValue(map, "FOO");
        assertMapValue(map, "TEST.FOO", v1, v3);
        assertMapValue(map, "TEST.D1", v3);

        assertMapValue(map, "TEST.FOO.FOO", v3);
        assertMapValue(map, "TEST.BAR.FOO", v3);
        assertMapValue(map, "TEST.FOO.BAR", v3, v5);
        assertMapValue(map, "TEST.BAR.D3", v3);

        assertMapValue(map, "TEST.*", v1, v3, v6);
        assertMapValue(map, "*.*", v1, v3, v6);
        assertMapValue(map, "*.D1", v3);
        assertMapValue(map, "TEST.*.*", v3, v5);
        assertMapValue(map, "TEST.BAR.*", v3);
    }

    @Test()
	public void testAddAndRemove() throws Exception {
        PathMap<String> map = new PathMap<String>();

        put(map, "FOO.A", v1);
        assertMapValue(map, "FOO.>", v1);

        put(map, "FOO.B", v2);
        assertMapValue(map, "FOO.>", v1, v2);

        map.removeAll(createDestination("FOO.A"));

        assertMapValue(map, "FOO.>", v2);

    }

    protected void loadSample2(PathMap<String> map) {
        put(map, "TEST.FOO", v1);
        put(map, "TEST.*", v2);
        put(map, "TEST.>", v3);
        put(map, ">", v4);
        put(map, "TEST.FOO.BAR", v5);
        put(map, "TEST.XYZ", v6);
    }

    protected void assertSample2(PathMap<String> map) {
        assertMapValue(map, "FOO", v4);
        assertMapValue(map, "TEST.FOO", v1, v2, v3, v4);
        assertMapValue(map, "TEST.D1", v2, v3, v4);

        assertMapValue(map, "TEST.FOO.FOO", v3, v4);
        assertMapValue(map, "TEST.BAR.FOO", v3, v4);
        assertMapValue(map, "TEST.FOO.BAR", v3, v4, v5);
        assertMapValue(map, "TEST.BAR.D3", v3, v4);

        assertMapValue(map, "TEST.*", v1, v2, v3, v4, v6);
        assertMapValue(map, "*.*", v1, v2, v3, v4, v6);
        assertMapValue(map, "*.D1", v2, v3, v4);
        assertMapValue(map, "TEST.*.*", v3, v4, v5);
        assertMapValue(map, "TEST.BAR.*", v3, v4);
    }

    protected void put(PathMap<String> map, String name, String value) {
        map.put(createDestination(name), value);
    }

    protected void remove(PathMap<String> map, String name, String value) {
        AsciiBuffer destination = createDestination(name);
        map.remove(destination, value);
    }

    protected void assertMapValue(PathMap<String> map, String destinationName, Object... expected) {
        AsciiBuffer destination = createDestination(destinationName);
        assertMapValue(map, destination, expected);
    }

    @SuppressWarnings("unchecked")
    protected void assertMapValue(PathMap<String> map, AsciiBuffer destination, Object... expected) {
        List expectedList = Arrays.asList(expected);
        Collections.sort(expectedList);
        Set actualSet = map.get(destination);
        List actual = new ArrayList(actualSet);
        Collections.sort(actual);
        assertEquals(("map value for destinationName:  " + destination), expectedList, actual);
    }

    protected AsciiBuffer createDestination(String name) {
   		return new AsciiBuffer(new AsciiBuffer(name));
    }
}
