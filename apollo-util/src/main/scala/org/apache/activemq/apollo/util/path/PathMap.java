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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.fusesource.hawtbuf.AsciiBuffer;

/**
 * A Map-like data structure allowing values to be indexed by
 * {@link AsciiBuffer} and retrieved by path - supporting both *
 * and &gt; style of wildcard as well as composite paths. <br>
 * This class assumes that the index changes rarely but that fast lookup into
 * the index is required. So this class maintains a pre-calculated index for
 * path steps. So looking up the values for "TEST.*" or "*.TEST" will be
 * pretty fast. <br>
 * Looking up of a value could return a single value or a List of matching
 * values if a wildcard or composite path is used.
 * 
 * @version $Revision: 1.3 $
 */
public class PathMap<Value> {

    private final PathMapNode<Value> root = new PathMapNode<Value>(null);

    /**
     * Looks up the value(s) matching the given AsciiBuffer key. For simple
     * paths this is typically a List of one single value, for wild cards
     * or composite paths this will typically be a List of matching
     * values.
     * 
     * @param key the path to lookup
     * @return a List of matching values or an empty list if there are no
     *         matching values.
     */
    public Set<Value> get(Path[] key) {
        return findWildcardMatches(key);
    }

    public void put(Path[] key, Value value) {
        root.add(key, 0, value);
    }

    /**
     * Removes the value from the associated path
     */
    public void remove(Path[] paths, Value value) {
        root.remove(paths, 0, value);

    }

    public PathMapNode<Value> getRootNode() {
        return root;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected Set<Value> findWildcardMatches(Path[] paths) {
        HashSet<Value> answer = new HashSet<Value>();
        root.appendMatchingValues(answer, paths, 0);
        return answer;
    }

    /**
     * @param key
     * @return
     */
    public Set<Value> removeAll(Path[] key) {
    	HashSet<Value> rc = new HashSet<Value>();
        root.removeAll(rc, key, 0);
        return rc;
    }

    /**
     * Returns the value which matches the given path or null if there is
     * no matching value. If there are multiple values, the results are sorted
     * and the last item (the biggest) is returned.
     *
     * @param path the path to find the value for
     * @return the largest matching value or null if no value matches
     */
    public Value chooseValue(Path[] path) {
        Set<Value> set = get(path);
        if (set == null || set.isEmpty()) {
            return null;
        }
        SortedSet<Value> sortedSet = new TreeSet<Value>(set);
        return sortedSet.last();
    }
}
