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

import java.util.ArrayList
import java.util.Collection
import java.util.Set
import collection.JavaConversions._

/**
  * An implementation of {@link PathNode} which navigates all the children of the given node
  * ignoring the name of the current path (so for navigating using * in a wildcard).
  *
  */
class AnyChildPathNode[Value](val node: PathNode[Value]) extends PathNode[Value] {

  def appendMatchingValues(answer: Set[Value], path: Path, startIndex: Int): Unit = {
    for (child <- getChildNodes) {
      child.appendMatchingValues(answer, path, startIndex)
    }
  }

  def appendMatchingWildcards(answer: Set[Value], path: Path, startIndex: Int): Unit = {
    for (child <- getChildNodes) {
      child.appendMatchingWildcards(answer, path, startIndex)
    }
  }

  def appendDescendantValues(answer: Set[Value]): Unit = {
    for (child <- getChildNodes) {
      child.appendDescendantValues(answer)
    }
  }

  def getChild(part: Part): PathNode[Value] = {
    val list: Collection[PathNode[Value]] = new ArrayList[PathNode[Value]]
    for (child <- getChildNodes) {
      var answer: PathNode[Value] = child.getChild(part)
      if (answer != null) {
        list.add(answer)
      }
    }
    if (!list.isEmpty) {
      return new AnyChildPathNode[Value]((this)) {
        protected override def getChildNodes: Collection[PathNode[Value]] = {
          return list
        }
      }
    }
    return null
  }

  def getDesendentValues: Collection[Value] = {
    var answer: Collection[Value] = new ArrayList[Value]
    for (child <- getChildNodes) {
      answer.addAll(child.getDesendentValues)
    }
    return answer
  }

  def getValues: Collection[Value] = {
    var answer: Collection[Value] = new ArrayList[Value]
    for (child <- getChildNodes) {
      answer.addAll(child.getValues)
    }
    return answer
  }

  def getChildren: Collection[PathNode[Value]] = {
    var answer: Collection[PathNode[Value]] = new ArrayList[PathNode[Value]]
    for (child <- getChildNodes) {
      answer.addAll(child.getChildren)
    }
    return answer
  }

  def removeDesendentValues: Collection[Value] = {
    var answer: Collection[Value] = new ArrayList[Value]
    for (child <- getChildNodes) {
      answer.addAll(child.removeDesendentValues)
    }
    return answer
  }

  def removeValues: Collection[Value] = {
    var answer: Collection[Value] = new ArrayList[Value]
    for (child <- getChildNodes) {
      answer.addAll(child.removeValues)
    }
    return answer
  }

  protected def getChildNodes: Collection[PathNode[Value]] = {
    return node.getChildren
  }

}