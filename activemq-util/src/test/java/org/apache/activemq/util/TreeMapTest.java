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
package org.apache.activemq.util;

import java.util.Comparator;
import java.util.Iterator;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author cmacnaug
 * 
 */
public class TreeMapTest {

    @Test()
	public void testOrdering() {
        Integer[] keys = new Integer[101];
        TreeMap<Integer, Integer> testMap = new TreeMap<Integer, Integer>(new Comparator<Integer>() {

            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        });
        
        java.util.TreeMap<Integer, Integer> refMap = new java.util.TreeMap<Integer, Integer>(new Comparator<Integer>() {

            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        });
        
        for (int i = 0; i < keys.length; i++) {
            keys[i] = i * 2;
            testMap.put(keys[i], keys[i]);
            refMap.put(keys[i], keys[i]);
        }
        
        Assert.assertEquals(refMap.get(4), testMap.get(4));
        Assert.assertEquals(refMap.get(3), testMap.get(3));
        Assert.assertEquals(refMap.size(), testMap.size());
        
        //Test lookup:
        Assert.assertEquals(testMap.lowerEntry(-2), null);
        Assert.assertEquals(testMap.lowerEntry(-1), null);
        Assert.assertEquals(testMap.lowerEntry(0), null);
        Assert.assertEquals(testMap.lowerEntry(1).getValue(), new Integer(0));
        Assert.assertEquals(testMap.lowerEntry(2).getValue(), new Integer(0));
        Assert.assertEquals(testMap.floorEntry(49).getValue(), new Integer(48));
        Assert.assertEquals(testMap.floorEntry(50).getValue(), new Integer(50));
        Assert.assertEquals(testMap.floorEntry(51).getValue(), new Integer(50));
        Assert.assertEquals(testMap.lowerEntry(198).getValue(), new Integer(196));
        Assert.assertEquals(testMap.lowerEntry(199).getValue(), new Integer(198));
        Assert.assertEquals(testMap.lowerEntry(200).getValue(), new Integer(198));
        Assert.assertEquals(testMap.lowerEntry(201).getValue(), new Integer(200));
        Assert.assertEquals(testMap.lowerEntry(202).getValue(), new Integer(200));
        
        Assert.assertEquals(testMap.floorEntry(-2), null);
        Assert.assertEquals(testMap.floorEntry(-1), null);
        Assert.assertEquals(testMap.floorEntry(0).getValue(), new Integer(0));
        Assert.assertEquals(testMap.floorEntry(1).getValue(), new Integer(0));
        Assert.assertEquals(testMap.floorEntry(2).getValue(), new Integer(2));
        Assert.assertEquals(testMap.floorEntry(49).getValue(), new Integer(48));
        Assert.assertEquals(testMap.floorEntry(50).getValue(), new Integer(50));
        Assert.assertEquals(testMap.floorEntry(51).getValue(), new Integer(50));
        Assert.assertEquals(testMap.floorEntry(198).getValue(), new Integer(198));
        Assert.assertEquals(testMap.floorEntry(199).getValue(), new Integer(198));
        Assert.assertEquals(testMap.floorEntry(200).getValue(), new Integer(200));
        Assert.assertEquals(testMap.floorEntry(201).getValue(), new Integer(200));
        Assert.assertEquals(testMap.floorEntry(202).getValue(), new Integer(200));
        
        Assert.assertEquals(testMap.upperEntry(-2).getValue(), new Integer(0));
        Assert.assertEquals(testMap.upperEntry(-1).getValue(), new Integer(0));
        Assert.assertEquals(testMap.upperEntry(0).getValue(), new Integer(2));
        Assert.assertEquals(testMap.upperEntry(1).getValue(), new Integer(2));
        Assert.assertEquals(testMap.upperEntry(2).getValue(), new Integer(4));
        Assert.assertEquals(testMap.upperEntry(49).getValue(), new Integer(50));
        Assert.assertEquals(testMap.upperEntry(50).getValue(), new Integer(52));
        Assert.assertEquals(testMap.upperEntry(51).getValue(), new Integer(52));
        Assert.assertEquals(testMap.upperEntry(198).getValue(), new Integer(200));
        Assert.assertEquals(testMap.upperEntry(199).getValue(), new Integer(200));
        Assert.assertEquals(testMap.upperEntry(200), null);
        Assert.assertEquals(testMap.upperEntry(201), null);
        Assert.assertEquals(testMap.upperEntry(202), null);
        
        Assert.assertEquals(testMap.ceilingEntry(-2).getValue(), new Integer(0));
        Assert.assertEquals(testMap.ceilingEntry(-1).getValue(), new Integer(0));
        Assert.assertEquals(testMap.ceilingEntry(0).getValue(), new Integer(0));
        Assert.assertEquals(testMap.ceilingEntry(1).getValue(), new Integer(2));
        Assert.assertEquals(testMap.ceilingEntry(2).getValue(), new Integer(2));
        Assert.assertEquals(testMap.ceilingEntry(49).getValue(), new Integer(50));
        Assert.assertEquals(testMap.ceilingEntry(50).getValue(), new Integer(50));
        Assert.assertEquals(testMap.ceilingEntry(51).getValue(), new Integer(52));
        Assert.assertEquals(testMap.ceilingEntry(198).getValue(), new Integer(198));
        Assert.assertEquals(testMap.ceilingEntry(199).getValue(), new Integer(200));
        Assert.assertEquals(testMap.ceilingEntry(200).getValue(), new Integer(200));
        Assert.assertEquals(testMap.ceilingEntry(201), null);
        Assert.assertEquals(testMap.ceilingEntry(202), null);
        
        //Test iterators:
        assertEquals(refMap.keySet().iterator(), testMap.keySet().iterator());
        assertEquals(refMap.values().iterator(), testMap.values().iterator());
        assertEquals(refMap.entrySet().iterator(), testMap.entrySet().iterator());
        
        //Test removal:
        Assert.assertEquals(testMap.remove(testMap.firstKey()), refMap.remove(refMap.firstKey()));
        Iterator<Integer> refIt = refMap.values().iterator();
        Iterator<Integer> testIt = testMap.values().iterator();
        refIt.next();
        testIt.next();
        refIt.remove();
        testIt.remove();
        assertEquals(testIt, refIt);
    }
    
    private static <T> void assertEquals(Iterator<T> i1, Iterator<T> i2)
    {
        Assert.assertEquals(i2.hasNext(), i1.hasNext());
        if(i1.hasNext())
        {
            Assert.assertEquals(i2.next(), i1.next());
        }
    }
}
