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

import org.junit.Test
import org.junit.Assert._
import collection.JavaConversions._

/**
  * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
  */
class PathMapTest {

  val parser = new PathParser
  val d1 = createDestination("TEST.D1")
  val d2 = createDestination("TEST.BAR.D2")
  val d3 = createDestination("TEST.BAR.D3")
  val v1 = "value1"
  val v2 = "value2"
  val v3 = "value3"
  val v4 = "value4"
  val v5 = "value5"
  val v6 = "value6"

  @Test def testSimplePaths: Unit = {
    var map: PathMap[String] = new PathMap[String]
    map.put(d1, v1)
    map.put(d2, v2)
    map.put(d3, v3)
    assertMapValue(map, d1, v1)
    assertMapValue(map, d2, v2)
    assertMapValue(map, d3, v3)
  }

  @Test def testSimpleDestinationsWithMultipleValues: Unit = {
    var map: PathMap[String] = new PathMap[String]
    map.put(d1, v1)
    map.put(d2, v2)
    map.put(d2, v3)
    assertMapValue(map, d1, v1)
    assertMapValue(map, "TEST.BAR.D2", v2, v3)
    assertMapValue(map, d3)
  }

  @Test def testLookupOneStepWildcardPaths: Unit = {
    var map: PathMap[String] = new PathMap[String]
    map.put(d1, v1)
    map.put(d2, v2)
    map.put(d3, v3)
    assertMapValue(map, "TEST.D1", v1)
    assertMapValue(map, "TEST.*", v1)
    assertMapValue(map, "*.D1", v1)
    assertMapValue(map, "*.*", v1)
    assertMapValue(map, "TEST.BAR.D2", v2)
    assertMapValue(map, "TEST.*.D2", v2)
    assertMapValue(map, "*.BAR.D2", v2)
    assertMapValue(map, "*.*.D2", v2)
    assertMapValue(map, "TEST.BAR.D3", v3)
    assertMapValue(map, "TEST.*.D3", v3)
    assertMapValue(map, "*.BAR.D3", v3)
    assertMapValue(map, "*.*.D3", v3)
    assertMapValue(map, "TEST.BAR.D4")
    assertMapValue(map, "TEST.BAR.*", v2, v3)
  }

  @Test def testLookupMultiStepWildcardPaths: Unit = {
    var map: PathMap[String] = new PathMap[String]
    map.put(d1, v1)
    map.put(d2, v2)
    map.put(d3, v3)
    assertMapValue(map, "**", v1, v2, v3)
    assertMapValue(map, "TEST.**", v1, v2, v3)
    assertMapValue(map, "*.**", v1, v2, v3)
    assertMapValue(map, "FOO.**")
  }

  @Test def testStoreWildcardWithOneStepPath: Unit = {
    var map: PathMap[String] = new PathMap[String]
    put(map, "TEST.*", v1)
    put(map, "TEST.D1", v2)
    put(map, "TEST.BAR.*", v2)
    put(map, "TEST.BAR.D3", v3)
    assertMapValue(map, "FOO")
    assertMapValue(map, "TEST.FOO", v1)
    assertMapValue(map, "TEST.D1", v1, v2)
    assertMapValue(map, "TEST.FOO.FOO")
    assertMapValue(map, "TEST.BAR.FOO", v2)
    assertMapValue(map, "TEST.BAR.D3", v2, v3)
    assertMapValue(map, "TEST.*", v1, v2)
    assertMapValue(map, "*.D1", v1, v2)
    assertMapValue(map, "*.*", v1, v2)
    assertMapValue(map, "TEST.*.*", v2, v3)
    assertMapValue(map, "TEST.BAR.*", v2, v3)
    assertMapValue(map, "*.*.*", v2, v3)
    assertMapValue(map, "*.BAR.*", v2, v3)
    assertMapValue(map, "*.BAR.D3", v2, v3)
    assertMapValue(map, "*.*.D3", v2, v3)
  }

  @Test def testStoreWildcardInMiddleOfPath: Unit = {
    var map: PathMap[String] = new PathMap[String]
    put(map, "TEST.*", v1)
    put(map, "TEST.D1", v2)
    put(map, "TEST.BAR.*", v2)
    put(map, "TEST.XYZ.D3", v3)
    put(map, "TEST.XYZ.D4", v4)
    put(map, "TEST.BAR.D3", v5)
    put(map, "TEST.*.D2", v6)
    assertMapValue(map, "TEST.*.D3", v2, v3, v5)
    assertMapValue(map, "TEST.*.D4", v2, v4)
    assertMapValue(map, "TEST.*", v1, v2)
    assertMapValue(map, "TEST.*.*", v2, v3, v4, v5, v6)
    assertMapValue(map, "TEST.*.**", v1, v2, v3, v4, v5, v6)
    assertMapValue(map, "TEST.**", v1, v2, v3, v4, v5, v6)
    assertMapValue(map, "TEST.**.**", v1, v2, v3, v4, v5, v6)
    assertMapValue(map, "*.*.D3", v2, v3, v5)
    assertMapValue(map, "TEST.BAR.*", v2, v5, v6)
    assertMapValue(map, "TEST.BAR.D2", v2, v6)
    assertMapValue(map, "TEST.*.D2", v2, v6)
    assertMapValue(map, "TEST.BAR.*", v2, v5, v6)
  }

  @Test def testCustomRegexWildcardPaths: Unit = {
    var map: PathMap[String] = new PathMap[String]
    put(map, "TEST.a{[0-9]+}a", v1)
    put(map, "TEST.BAR.aa", v2)
    put(map, "TEST.BAR.a123a", v3)
    put(map, "TEST.BAR.a2ca", v4)
    put(map, "TEST.BAR.aba", v5)

    assertMapValue(map, "TEST.a99a", v1)
    assertMapValue(map, "TEST.BAR.a{[0-9]+}a", v3)
  }

  @Test def testRegexWildcardPaths: Unit = {
    var map: PathMap[String] = new PathMap[String]
    put(map, "TEST.a*a", v1)
    put(map, "TEST.BAR.aa", v2)
    put(map, "TEST.BAR.aba", v3)
    put(map, "TEST.BAR.cat", v4)
    put(map, "TEST.BAR.a", v5)

    assertMapValue(map, "TEST.aba", v1)
    assertMapValue(map, "TEST.BAR.a*a", v2, v3)
  }

  @Test def testDoubleWildcardDoesNotMatchLongerPattern: Unit = {
    var map: PathMap[String] = new PathMap[String]
    put(map, "TEST.*", v1)
    put(map, "TEST.BAR.D3", v2)
    assertMapValue(map, "*.*.D3", v2)
  }

  @Test def testWildcardAtEndOfPathAndAtBeginningOfSearch: Unit = {
    var map: PathMap[String] = new PathMap[String]
    put(map, "TEST.*", v1)
    assertMapValue(map, "*.D1", v1)
  }

  @Test def testAnyPathWildcardInMap: Unit = {
    var map: PathMap[String] = new PathMap[String]
    put(map, "TEST.FOO.**", v1)
    assertMapValue(map, "TEST.FOO.BAR.WHANOT.A.B.C", v1)
    assertMapValue(map, "TEST.FOO.BAR.WHANOT", v1)
    assertMapValue(map, "TEST.FOO.BAR", v1)
    assertMapValue(map, "TEST.*.*", v1)
    assertMapValue(map, "TEST.BAR")
    assertMapValue(map, "TEST.FOO", v1)
  }

  @Test def testSimpleAddRemove: Unit = {
    var map: PathMap[String] = new PathMap[String]
    put(map, "TEST.D1", v2)
    assertEquals("Root child count", 1, map.getRootNode.getChildCount)
    assertMapValue(map, "TEST.D1", v2)
    remove(map, "TEST.D1", v2)
    assertEquals("Root child count", 0, map.getRootNode.getChildCount)
    assertMapValue(map, "TEST.D1")
  }

  @Test def testStoreAndLookupAllWildcards: Unit = {
    var map: PathMap[String] = new PathMap[String]
    loadSample2(map)
    assertSample2(map)
    remove(map, "TEST.FOO", v1)
    assertMapValue(map, "TEST.FOO", v2, v3, v4)
    assertMapValue(map, "TEST.*", v2, v3, v4, v6)
    assertMapValue(map, "*.*", v2, v3, v4, v6)
    remove(map, "TEST.XYZ", v6)
    assertMapValue(map, "TEST.*", v2, v3, v4)
    assertMapValue(map, "*.*", v2, v3, v4)
    remove(map, "TEST.*", v2)
    assertMapValue(map, "TEST.*", v3, v4)
    assertMapValue(map, "*.*", v3, v4)
    remove(map, "**", v4)
    assertMapValue(map, "TEST.*", v3)
    assertMapValue(map, "*.*", v3)
    remove(map, "TEST.**", v3)
    remove(map, "TEST.FOO.BAR", v5)
    assertMapValue(map, "FOO")
    assertMapValue(map, "TEST.FOO")
    assertMapValue(map, "TEST.D1")
    assertMapValue(map, "TEST.FOO.FOO")
    assertMapValue(map, "TEST.BAR.FOO")
    assertMapValue(map, "TEST.FOO.BAR")
    assertMapValue(map, "TEST.BAR.D3")
    assertMapValue(map, "TEST.*")
    assertMapValue(map, "*.*")
    assertMapValue(map, "*.D1")
    assertMapValue(map, "TEST.*.*")
    assertMapValue(map, "TEST.BAR.*")
    loadSample2(map)
    assertSample2(map)
    remove(map, "**", v4)
    remove(map, "TEST.*", v2)
    assertMapValue(map, "FOO")
    assertMapValue(map, "TEST.FOO", v1, v3)
    assertMapValue(map, "TEST.D1", v3)
    assertMapValue(map, "TEST.FOO.FOO", v3)
    assertMapValue(map, "TEST.BAR.FOO", v3)
    assertMapValue(map, "TEST.FOO.BAR", v3, v5)
    assertMapValue(map, "TEST.BAR.D3", v3)
    assertMapValue(map, "TEST.*", v1, v3, v6)
    assertMapValue(map, "*.*", v1, v3, v6)
    assertMapValue(map, "*.D1", v3)
    assertMapValue(map, "TEST.*.*", v3, v5)
    assertMapValue(map, "TEST.BAR.*", v3)
  }

  @Test def testAddAndRemove: Unit = {
    var map: PathMap[String] = new PathMap[String]
    put(map, "FOO.A", v1)
    assertMapValue(map, "FOO.**", v1)
    put(map, "FOO.B", v2)
    assertMapValue(map, "FOO.**", v1, v2)
    map.removeAll(createDestination("FOO.A"))
    assertMapValue(map, "FOO.**", v2)
  }

  protected def loadSample2(map: PathMap[String]): Unit = {
    put(map, "TEST.FOO", v1)
    put(map, "TEST.*", v2)
    put(map, "TEST.**", v3)
    put(map, "**", v4)
    put(map, "TEST.FOO.BAR", v5)
    put(map, "TEST.XYZ", v6)
  }

  protected def assertSample2(map: PathMap[String]): Unit = {
    assertMapValue(map, "FOO", v4)
    assertMapValue(map, "TEST.FOO", v1, v2, v3, v4)
    assertMapValue(map, "TEST.D1", v2, v3, v4)
    assertMapValue(map, "TEST.FOO.FOO", v3, v4)
    assertMapValue(map, "TEST.BAR.FOO", v3, v4)
    assertMapValue(map, "TEST.FOO.BAR", v3, v4, v5)
    assertMapValue(map, "TEST.BAR.D3", v3, v4)
    assertMapValue(map, "TEST.*", v1, v2, v3, v4, v6)
    assertMapValue(map, "*.*", v1, v2, v3, v4, v6)
    assertMapValue(map, "*.D1", v2, v3, v4)
    assertMapValue(map, "TEST.*.*", v3, v4, v5)
    assertMapValue(map, "TEST.BAR.*", v3, v4)
  }

  protected def put(map: PathMap[String], name: String, value: String): Unit = {
    map.put(createDestination(name), value)
  }

  protected def remove(map: PathMap[String], name: String, value: String): Unit = {
    var destination: Path = createDestination(name)
    map.remove(destination, value)
  }

  protected def assertMapValue(map: PathMap[String], destinationName: String, expected: String*): Unit = {
    var destination: Path = createDestination(destinationName)
    assertMapValue(map, destination, expected:_*)
  }

  @SuppressWarnings(Array("unchecked"))
  protected def assertMapValue(map: PathMap[String], destination: Path, expected: String*): Unit = {
    var expectedList = expected.toList.sortWith( _ < _)
    var actualSet = map.get(destination)
    var actualList = actualSet.toList.sortWith( _ < _)
    assertEquals(("map value for destinationName:  " + destination), expectedList, actualList)
  }

  protected def createDestination(name: String): Path = {
    return parser.decode_path(name)
  }

}