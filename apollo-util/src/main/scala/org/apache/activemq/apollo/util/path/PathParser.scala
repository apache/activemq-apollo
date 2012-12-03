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
import collection.mutable.ListBuffer
import org.fusesource.hawtbuf.{Buffer, DataByteArrayOutputStream, AsciiBuffer}
import java.lang.String

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
        case x:RegexChildPart =>
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

  class RegexChildPathFilter(val regex:Pattern,  next: PartFilter) extends PartFilter {

    def matches(remaining: LinkedList[Part]): Boolean = {
      if (!remaining.isEmpty) {
        var p: Part = remaining.removeFirst
        p match {
          case LiteralPart(v)=>
            if ( regex.matcher(v).matches ) {
              if (next != null) {
                return next.matches(remaining)
              } else {
                return remaining.isEmpty
              }
            } else {
              false
            }
          case _ => false
        }
      } else {
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
  var regex_wildcard_start = "{"
  var regex_wildcard_end = "}"
  var path_separator = "."
  var part_pattern = Pattern.compile("""[ a-zA-Z0-9\_\-\%\~\:\(\)]+""")

  def copy(other:PathParser) = {
    any_descendant_wildcard = other.any_descendant_wildcard
    any_child_wildcard = other.any_child_wildcard
    path_separator = other.path_separator
    part_pattern = other.part_pattern
    this
  }

  def url_encode(value:String, allowed:Pattern) = {
    // UTF-8 Encode..
    var ascii = new Buffer(value.getBytes("UTF-8")).ascii().toString
    val matcher = allowed.matcher(ascii);
    var pos = 0;
    val rc = new StringBuffer(ascii.length())

    def escape_until(end:Int) = while( pos < end ) {
      rc.append("%%%02x".format(ascii.charAt(pos).toInt))
      pos+=1
    }
    while( matcher.find(pos) ) {
      escape_until(matcher.start)
      rc.append(ascii.substring(matcher.start, matcher.end))
      pos+=(matcher.end-matcher.start)
    }
    escape_until(ascii.length)
    rc.toString
  }

  def url_decode(value:String):String = {
    val rc = new DataByteArrayOutputStream
    var pos = value
    while( pos.length() > 0 ) {
      if( pos.startsWith("%") && pos.length()> 3 ) {
        val dec = pos.substring(1,3)
        rc.writeByte(Integer.parseInt(dec, 16))
        pos = pos.substring(3);
      } else {
        rc.writeByte(pos.charAt(0))
        pos = pos.substring(1)
      }
    }
    rc.toBuffer.utf8().toString
  }


  def parts(subject: String): Array[String] = {
    val rc = if(path_separator!=null) {
      subject.split(Pattern.quote(path_separator))
    } else {
      Array(subject)
    }
    rc
  }

  def decode_path(parts: Iterable[String]): Path = {
    return new Path(parts.toList.map(decode_part(_)))
  }

  def decode_path(subject: String): Path = {
    return decode_path(parts(subject))
  }

  def regex_map[T](text:String, pattern: Pattern)(func: Either[CharSequence, Matcher] => T) = {
    var lastIndex = 0;
    val m = pattern.matcher(text);
    val rc = new ListBuffer[T]();
    while (m.find()) {
      rc += func(Left(text.subSequence(lastIndex, m.start)))
      rc += func(Right(m))
      lastIndex = m.end
    }
    rc += func(Left(text.subSequence(lastIndex,  text.length)))
    rc.toList
  }

  lazy val wildcard_part_pattern = if (regex_wildcard_start!=null && regex_wildcard_end!=null) {
    var p = Pattern.quote(regex_wildcard_start)+"(.*?)"+Pattern.quote(regex_wildcard_end)
    if(any_child_wildcard!=null) {
      p += "|" + Pattern.quote(any_child_wildcard)
    }
    p.r.pattern
  } else {
    null
  }
  
  private def decode_part(value: String): Part = {
    if (any_child_wildcard!=null && value == any_child_wildcard) {
      AnyChildPart
    } else if (any_descendant_wildcard!=null && value == any_descendant_wildcard) {
      AnyDescendantPart
    } else if (wildcard_part_pattern!=null && wildcard_part_pattern.matcher(value).find() ) {
      val regex = regex_map(value, wildcard_part_pattern) { _ match {
        // It's a literal part.
        case Left(x) =>
          if (x=="") {
            ""
          } else {
            if( part_pattern!=null ) {
              if (!part_pattern.matcher(x).matches) {
                throw new PathParser.PathException(String.format("Invalid destination: '%s', it does not match regex: %s", value, part_pattern))
              } else {
                Pattern.quote(url_decode(x.toString))
              }
            } else {
              Pattern.quote(x.toString)
            }
          }
        // It was a regex part..
        case Right(wildcard) =>
          if ( wildcard.group() == any_child_wildcard ) {
            ".*?"
          } else {
            wildcard.group(1)
          }
      } }.mkString("")
      var regex_string: String = "^" + regex + "$"
      RegexChildPart(regex_string.r.pattern)      
    } else {
      if (part_pattern != null ) {
        if ( !part_pattern.matcher(value.toString).matches) {
          throw new PathParser.PathException(String.format("Invalid destination: '%s', it does not match regex: %s", value, part_pattern))
        } else {
          LiteralPart(url_decode(value))
        }
      } else {
        LiteralPart(value)
      }
    }
  }

  /**
    * Converts the path back to the string representation.
    * @return
    */
  def encode_path(path: Path): String = encode_path_iter(path_parts(path))

  def path_parts(path: Path):Array[String] = {
    (path.parts.map( _ match {
      case RootPart => ""
      case AnyChildPart => any_child_wildcard
      case AnyDescendantPart => any_descendant_wildcard
      case RegexChildPart(regex) => regex_wildcard_start + regex.pattern() + regex_wildcard_end
      case LiteralPart(value) =>
        if( part_pattern !=null ) {
          url_encode(value, part_pattern)
        } else {
          value
        }
    })).toArray
  }

  def encode_path_iter(parts: Iterable[String]): String = {
    val buffer: StringBuffer = new StringBuffer
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
        case RegexChildPart(r) =>
          last = new PathParser.RegexChildPathFilter(r, last)
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