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
package org.apache.activemq.apollo.util
import java.{lang=>jl}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object OptionSupport {

  implicit def o(value:jl.Boolean):Option[Boolean] = value match {
    case null => None
    case x => Some(x.booleanValue)
  }

  implicit def o(value:jl.Character):Option[Char] = value match {
    case null => None
    case x => Some(x.charValue)
  }

  implicit def o(value:jl.Short):Option[Short] = value match {
    case null => None
    case x => Some(x.shortValue)
  }

  implicit def o(value:jl.Integer):Option[Int] = value match {
    case null => None
    case x => Some(x.intValue)
  }

  implicit def o(value:jl.Long):Option[Long] = value match {
    case null => None
    case x => Some(x.longValue)
  }

  implicit def o(value:jl.Double):Option[Double] = value match {
    case null => None
    case x => Some(x.doubleValue)
  }

  implicit def o(value:jl.Float):Option[Float] = value match {
    case null => None
    case x => Some(x.floatValue)
  }

  implicit def o[T](value:T):Option[T] = value match {
    case null => None
    case x => Some(x)
  }

}