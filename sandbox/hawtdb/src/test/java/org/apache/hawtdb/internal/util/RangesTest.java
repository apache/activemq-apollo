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
package org.apache.hawtdb.internal.util;

import static org.apache.hawtdb.internal.util.Ranges.range;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.apache.hawtdb.internal.util.Ranges;
import org.apache.hawtdb.internal.util.Ranges.Range;
import org.junit.Test;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class RangesTest {

    @Test
    public void test() {

        Ranges ranges = new Ranges();
        
        // Example of a simple range merges..
        ranges.add(0, 5);
        ranges.add(15, 5);
        ranges.add(5,10);
        assertEquals(ranges(range(0,20)), ranges.toArrayList());

        // Remove which splits an existing range into 2.
        ranges.remove(5,10);
        assertEquals(ranges(range(0,5),range(15,20)), ranges.toArrayList());
        
        // overlapping add...
        ranges.add(4,12);
        assertEquals(ranges(range(0,20)), ranges.toArrayList());

        // Removes are idempotent 
        ranges.remove(5,10);
        assertEquals(ranges(range(0,5),range(15,20)), ranges.toArrayList());
        ranges.remove(5,10);
        assertEquals(ranges(range(0,5),range(15,20)), ranges.toArrayList());

        // Adds are idempotent 
        ranges.add(5,10);
        assertEquals(ranges(range(0,20)), ranges.toArrayList());
        ranges.add(5,10);
        assertEquals(ranges(range(0,20)), ranges.toArrayList());
    }
    
    ArrayList<Range> ranges(Range... args) {
        ArrayList<Range> rc = new ArrayList<Range>();
        for (Range range : args) {
            rc.add(range);
        }
        return rc;
    }
    
}
