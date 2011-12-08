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
package org.apache.activemq.apollo.util.path

import java.util.HashSet
import java.util.Set
import java.util.SortedSet
import java.util.TreeSet

/**
  * A Map-like data structure allowing values to be indexed by
  * {@link String} and retrieved by path - supporting both *
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
class PathMap[Value] {
  /**
    * Looks up the value(s) matching the given String key. For simple
    * paths this is typically a List of one single value, for wild cards
    * or composite paths this will typically be a List of matching
    * values.
    *
    * @param key the path to lookup
    * @return a List of matching values or an empty list if there are no
    *         matching values.
    */
  def get(key: Path): Set[Value] = {
    return findWildcardMatches(key)
  }

  def put(key: Path, value: Value): Unit = {
    root.add(key, 0, value)
  }

  /**
    * Removes the value from the associated path
    */
  def remove(path: Path, value: Value): Boolean = {
    return root.remove(path, 0, value)
  }

  def getRootNode = root

  protected def findWildcardMatches(path: Path): Set[Value] = {
    var answer: HashSet[Value] = new HashSet[Value]
    root.appendMatchingValues(answer, path, 0)
    return answer
  }

  /**
    * @param key
    * @return
    */
  def removeAll(key: Path): Set[Value] = {
    var rc: HashSet[Value] = new HashSet[Value]
    root.removeAll(rc, key, 0)
    return rc
  }

  /**
    * Returns the value which matches the given path or null if there is
    * no matching value. If there are multiple values, the results are sorted
    * and the last item (the biggest) is returned.
    *
    * @param path the path to find the value for
    * @return the largest matching value or null if no value matches
    */
  def chooseValue(path: Path): Value = {
    var set: Set[Value] = get(path)
    if ((set == null) || set.isEmpty) {
      return null.asInstanceOf[Value]
    }
    var first: Value = set.iterator().next()
    if( set.size()==1 || !first.isInstanceOf[java.lang.Comparable[_]]) {
      return first;
    }
    var sortedSet: SortedSet[Value] = new TreeSet[Value](set)
    return sortedSet.last
  }

  private final val root = new PathMapNode[Value](null)
}