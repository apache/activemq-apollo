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

import java.util.Set
import org.junit.Test
import org.junit.Assert._

/**
  * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
  */
class PathMapMemoryTest {
  protected def createDestination(name: String): Path = {
    return parser.decode_path(name)
  }

  @Test def testLongPath: Unit = {
    var d1: Path = createDestination("1.2.3.4.5.6.7.8.9.10.11.12.13.14.15.16.17.18")
    var map: PathMap[String] = new PathMap[String]
    map.put(d1, "test")
  }

  @Test
  def testVeryLongPaths = {
    var i = 1
    while (i < 100) {
      i += 1
      var name: String = "1"
      var j = 2
      while (j <= i) {
        name += "." + j
        j += 1;
      }
      try {
        var d1: Path = createDestination(name)
        var map: PathMap[String] = new PathMap[String]
        map.put(d1, "test")
      } catch {
        case e: Throwable => fail("Destination name too long: " + name + " : " + e)
      }
    }
  }

  @Test def testLotsOfPaths: Unit = {
    var map: PathMap[AnyRef] = new PathMap[AnyRef]
    var value: AnyRef = new AnyRef
    var count: Int = 1000;
    {
      var i: Int = 0
      while (i < count) {
          var queue: Path = createDestination("connection-" + i)
          map.put(queue, value)
          i += 1; i
      }
    }
    {
      var i: Int = 0
      while (i < count) {
          var queue: Path = createDestination("connection-" + i)
          map.remove(queue, value)
          var set: Set[AnyRef] = map.get(queue)
          assertTrue(set.isEmpty)
          i += 1; i
      }
    }
  }

  private[path] var parser: PathParser = new PathParser
}