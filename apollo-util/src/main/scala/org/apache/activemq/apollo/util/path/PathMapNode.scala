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
import java.util.HashMap
import java.util.HashSet
import java.util.List
import java.util.Map
import java.util.Set
import collection.JavaConversions._

/**
  * An implementation class used to implement {@link PathMap}
  *
  * @version $Revision: 1.2 $
  */
class PathMapNode[Value](val parent: PathMapNode[Value]) extends PathNode[Value] {

  val childNodes: Map[Part, PathNode[Value]] = new HashMap[Part, PathNode[Value]]
  val values: List[Value] = new ArrayList[Value]
  var part: Part = RootPart

  var pathLength:Int = if (parent == null) {
    0
  } else {
    parent.pathLength + 1
  }

  /**
    * Returns the child node for the given named path or null if it does not
    * exist
    */
  def getChild(part: Part): PathMapNode[Value] = {
    return childNodes.get(part).asInstanceOf[PathMapNode[Value]]
  }

  /**
    * Returns the child nodes
    */
  def getChildren: Collection[PathNode[Value]] = {
    return childNodes.values
  }

  def getChildCount: Int = {
    return childNodes.size
  }

  /**
    * Returns the child node for the given named path, lazily creating one if
    * it does not yet exist
    */
  def getChildOrCreate(part: Part): PathMapNode[Value] = {
    var answer: PathMapNode[Value] = childNodes.get(part).asInstanceOf[PathMapNode[Value]]
    if (answer == null) {
      answer = createChildNode
      answer.part = part
      childNodes.put(part, answer)
    }
    return answer
  }

  /**
    * Returns a mutable List of the values available at this node in the tree
    */
  def getValues: List[Value] = {
    return values
  }

  /**
    * Returns a mutable List of the values available at this node in the tree
    */
  def removeValues: List[Value] = {
    var v: ArrayList[Value] = new ArrayList[Value](values)
    values.clear
    pruneIfEmpty
    return v
  }

  def removeDesendentValues: Set[Value] = {
    var answer: Set[Value] = new HashSet[Value]
    removeDesendentValues(answer)
    return answer
  }

  protected def removeDesendentValues(answer: Set[Value]): Unit = {
    answer.addAll(removeValues)
  }

  /**
    * Returns a list of all the values from this node down the tree
    */
  def getDesendentValues: Set[Value] = {
    var answer: Set[Value] = new HashSet[Value]
    appendDescendantValues(answer)
    return answer
  }

  def add(path: Path, idx: Int, value: Value): Unit = {
    if (idx >= path.parts.size) {
      values.add(value)
    }
    else {
      getChildOrCreate(path.parts.get(idx)).add(path, idx + 1, value)
    }
  }

  def remove(path: Path, idx: Int, value: Value): Boolean = {
    if (idx >= path.parts.size) {
      var rc = values.remove(value)
      pruneIfEmpty
      return rc
    } else {
      return getChildOrCreate(path.parts.get(idx)).remove(path, idx+1, value)
    }
  }

  def removeAll(answer: Set[Value], path: Path, startIndex: Int): Unit = {
    var node: PathNode[Value] = this
    var size: Int = path.parts.size;
    var i: Int = startIndex

    while (i < size && node != null) {
       path.parts.get(i) match {
        case AnyDescendantPart =>
          answer.addAll(node.removeDesendentValues)
          i = size;
        case AnyChildPart =>
          node.appendMatchingWildcards(answer, path, i)
          node = new AnyChildPathNode[Value](node)
          i += 1;
        case RegexChildPart(r) =>
          node.appendMatchingWildcards(answer, path, i)
          node = new RegexChildPathNode[Value](node, r)
          i += 1;
        case part =>
          node.appendMatchingWildcards(answer, path, i)
          node = node.getChild(part)
          i += 1;
      }
    }
    if (node != null) {
      answer.addAll(node.removeValues)
    }
  }

  def appendDescendantValues(answer: Set[Value]): Unit = {
    answer.addAll(values)
    for (child <- childNodes.values) {
      child.appendDescendantValues(answer)
    }
  }

  /**
    * Factory method to create a child node
    */
  protected def createChildNode: PathMapNode[Value] = {
    return new PathMapNode[Value](this)
  }

  /**
    * Matches any entries in the map containing wildcards
    */
  def appendMatchingWildcards(answer: Set[Value], parts: Path, idx: Int): Unit = {
    if (idx - 1 > pathLength) {
      return
    }

    childNodes.foreach { case (path,node) =>
      path match {
        case AnyChildPart => node.appendMatchingValues(answer, parts, idx + 1)
        case x:RegexChildPart => node.appendMatchingValues(answer, parts, idx + 1)
        case AnyDescendantPart => answer.addAll(node.getDesendentValues)
        case x:LiteralPart =>
        case RootPart =>
      }
    }
  }

  def appendMatchingValues(answer: Set[Value], path: Path, startIndex: Int): Unit = {
    var node: PathNode[Value] = this
    var couldMatchAny: Boolean = true
    var size: Int = path.parts.size;
    var i: Int = startIndex
    while (i < size && node != null) {
      var part: Part = path.parts.get(i)
      part match {
        case AnyDescendantPart =>
          answer.addAll(node.getDesendentValues)
          i += size
          couldMatchAny = false
        case AnyChildPart =>
          node.appendMatchingWildcards(answer, path, i)
          i += 1
          node = new AnyChildPathNode[Value](node)
        case RegexChildPart(r) =>
          node.appendMatchingWildcards(answer, path, i)
          i += 1
          node = new RegexChildPathNode[Value](node, r)
        case x:LiteralPart =>
          node.appendMatchingWildcards(answer, path, i)
          i += 1;
          node = node.getChild(part)
        case RootPart =>
      }
    }
    if (node != null) {
      answer.addAll(node.getValues)
      if (couldMatchAny) {
        var child: PathNode[Value] = node.getChild(AnyDescendantPart)
        if (child != null) {
          answer.addAll(child.getValues)
        }
      }
    }
  }

  def getPart: Part = {
    return part
  }

  protected def pruneIfEmpty: Unit = {
    if (parent != null && childNodes.isEmpty && values.isEmpty) {
      parent.removeChild(this)
    }
  }

  protected def removeChild(node: PathMapNode[Value]): Unit = {
    childNodes.remove(node.getPart)
    pruneIfEmpty
  }

}