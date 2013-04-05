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

import scala.Function1
import scala.runtime.BoxedUnit

abstract class Fn0[+R] {
  def apply(): R
}

class UnitFn0 extends Fn0[BoxedUnit] {
  def call() = {}
  def apply() = {
    call()
    BoxedUnit.UNIT;
  }
}

abstract class Fn1[-T1,+R] {
  def apply(v1: T1): R
}

abstract class UnitFn1[-T1] extends Fn1[T1, BoxedUnit] {
  def call(v1: T1)
  def apply(v1: T1) = {
    call(v1)
    BoxedUnit.UNIT;
  }
}

abstract class Fn2[-T1,-T2,+R] {
  def apply(v1: T1, v2: T2): R
}

class UnitFn2[-T1,-T2] extends Fn2[T1,T2, BoxedUnit] {
  def call (v1: T1, v2: T2) = {}
  def apply(v1: T1, v2: T2) = {
    call(v1, v2)
    BoxedUnit.UNIT;
  }
}

/**
 * Created with IntelliJ IDEA.
 * User: chirino
 * Date: 3/26/13
 * Time: 3:27 PM
 * To change this template use File | Settings | File Templates.
 */
object Scala2JavaHelper {
  def toScala[R](func:Fn0[R]):Function0[R] = () => { func.apply() }
  def toScala[T1,R](func:Fn1[T1,R]):Function1[T1,R] = (v1:T1) => { func.apply(v1) }
  def toScala[T1,T2,R](func:Fn2[T1,T2,R]):Function2[T1,T2,R] = (v1:T1, v2:T2) => { func.apply(v1,v2) }
  def none[T]:Option[T] = None
  def some[T](t:T):Option[T] = Some(t)
  def toList[T](args:Array[T]):List[T] = List(args:_*)

  def trace(log:Log, message:String, args:Array[Object]) = log.trace(message, args:_*)
  def debug(log:Log, message:String, args:Array[Object]) = log.debug(message, args:_*)
  def info (log:Log, message:String, args:Array[Object]) = log.info (message, args:_*)
  def warn (log:Log, message:String, args:Array[Object]) = log.warn (message, args:_*)
  def error(log:Log, message:String, args:Array[Object]) = log.error(message, args:_*)

  def trace(log:Log, e:Throwable, message:String, args:Array[Object]) = log.trace(e, message, args:_*)
  def debug(log:Log, e:Throwable, message:String, args:Array[Object]) = log.debug(e, message, args:_*)
  def info (log:Log, e:Throwable, message:String, args:Array[Object]) = log.info (e, message, args:_*)
  def warn (log:Log, e:Throwable, message:String, args:Array[Object]) = log.warn (e, message, args:_*)
  def error(log:Log, e:Throwable, message:String, args:Array[Object]) = log.error(e, message, args:_*)
}
