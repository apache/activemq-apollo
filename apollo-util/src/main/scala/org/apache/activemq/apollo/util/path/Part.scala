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

import java.util.regex.Pattern
import java.lang.String

/**
  * Holds the delimiters used to parse paths.
  *
  * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
  */
sealed trait Part {
  def matches(p: Part) = true
}

object RootPart extends Part {
  override def matches(p: Part) = p match {
    case RootPart => true
    case _ => false
  }
}

object AnyChildPart extends Part
object AnyDescendantPart extends Part

case class RegexChildPart(regex:Pattern) extends Part

case class LiteralPart(value: String) extends Part {
  override def matches(p: Part) = p match {
    case LiteralPart(v) => v == value
    case _ => true
  }
  override def toString = value
}
