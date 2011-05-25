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

import java.util.LinkedList
import java.util.regex._
import collection.JavaConversions._
import org.apache.activemq.apollo.util.path.PathParser.PartFilter
/**
  * Holds the delimiters used to parse paths.
  *
  * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
  */
object PathParser {
  def containsWildCards(path: Path): Boolean = {
    for (p <- path.parts) {
      p match {
        case AnyDescendantPart =>
          return true
        case AnyChildPart =>
          return true
        case _ =>
      }
    }
    return false
  }

  val DEFAULT = new PathParser

  class PathException(message: String) extends RuntimeException(message) 
  
  trait PartFilter {
    def matches(remaining: LinkedList[Part]): Boolean
  }

  class AnyChildPathFilter(val next: PartFilter) extends PartFilter {

    def matches(remaining: LinkedList[Part]): Boolean = {
      if (!remaining.isEmpty) {
        var p: Part = remaining.removeFirst
        if (next != null) {
          return next.matches(remaining)
        }
        else {
          return remaining.isEmpty
        }
      }
      else {
        return false
      }
    }

  }

  class AnyDecendentPathFilter(val next: PartFilter) extends PartFilter {
    def matches(remaining: LinkedList[Part]): Boolean = {
      if (!remaining.isEmpty) {
        remaining.clear
        return true
      }
      else {
        return false
      }
    }
  }

}

class PathParser {

  var any_descendant_wildcard = "**"
  var any_child_wildcard = "*"
  var path_separator = "."
  var part_pattern = Pattern.compile("[a-zA-Z0-9\\_\\-\\%\\~]+")

  def copy(other:PathParser) = {
    any_descendant_wildcard = other.any_descendant_wildcard
    any_child_wildcard = other.any_child_wildcard
    path_separator = other.path_separator
    part_pattern = other.part_pattern
    this
  }

  def decode_path(subject: java.util.Collection[String]): Path = decode_path(subject.toIterable)

  def decode_path(subject: Iterable[String]): Path = {
    return new Path(subject.toList.map(decode_part(_)))
  }

  def parts(subject: String): Array[String] = {
    subject.split(Pattern.quote(path_separator))
  }

  def decode_path(subject: String): Path = {
    return decode_path(parts(subject))
  }

  private def decode_part(value: String): Part = {
    if (value == any_child_wildcard) {
      return AnyChildPart
    } else if (value == any_descendant_wildcard) {
      return AnyDescendantPart
    } else {
      if (part_pattern == null || part_pattern.matcher(value.toString).matches) {
        return LiteralPart(value)
      } else {
        throw new PathParser.PathException(String.format("Invalid destination path part: '%s', it does not match regex: %s", value, part_pattern))
      }
    }
  }

  /**
    * Converts the path back to the string representation.
    * @return
    */
  def encode_path(path: Path): String = encode_path(path_parts(path))

  def path_parts(path: Path):Array[String] = {
    (path.parts.map( _ match {
      case RootPart => ""
      case AnyChildPart => any_child_wildcard
      case AnyDescendantPart => any_descendant_wildcard
      case LiteralPart(value) => value
    })).toArray
  }

  def encode_path(parts: Iterable[String]): String = {
    var buffer: StringBuffer = new StringBuffer
    for (p <- parts) {
      if ( buffer.length() != 0) {
        buffer.append(path_separator)
      }
      buffer.append(p)
    }
    return buffer.toString
  }

  def decode_filter(path: String): PathFilter = {
    var last: PathParser.PartFilter = null
    for (p <- decode_path(path).parts.reverse ) {
      p match {
        case p:LiteralPart =>
          last = new LitteralPathFilter(last, p)
        case AnyChildPart =>
          last = new PathParser.AnyChildPathFilter(last)
        case AnyDescendantPart =>
          last = new PathParser.AnyDecendentPathFilter(last)
        case _ =>
      }
    }
    val filter: PathParser.PartFilter = last
    return new PathFilter {
      def matches(path: Path): Boolean = {
        return filter.matches(new LinkedList[Part](path.parts))
      }
    }
  }

  class LitteralPathFilter(val next: PartFilter, val path: LiteralPart) extends PartFilter {

    def matches(remaining: LinkedList[Part]): Boolean = {
      if (!remaining.isEmpty) {
        var p: Part = remaining.removeFirst
        if (!path.matches(p)) {
          return false
        }
        if (next != null) {
          return next.matches(remaining)
        }
        else {
          return remaining.isEmpty
        }
      }
      else {
        return false
      }
    }

  }

}