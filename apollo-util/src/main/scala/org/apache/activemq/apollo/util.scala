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
package org.apache.activemq.apollo

import org.fusesource.hawtdispatch._
import org.fusesource.hawtdispatch.Future

/**
 *
 */
package object util {

  type FutureResult[T] = Future[Result[T, Throwable]]

  def FutureResult[T]() = Future[Result[T, Throwable]]()

  def FutureResult[T](value:Result[T, Throwable]) = {
    val rc = Future[Result[T, Throwable]]()
    rc.set(value)
    rc
  }

  def sync[T](dispached:Dispatched)(func: =>FutureResult[T]):FutureResult[T] = {
    val rc = Future[Result[T, Throwable]]()
    dispached.dispatch_queue.apply {
      try {
        func.onComplete(x=> rc.apply(x))
      } catch {
        case e:Throwable => rc.apply(Failure(e))
      }
    }
    rc
  }

  def sync_all[T,D<:Dispatched](values:Iterable[D])(func: (D)=>FutureResult[T]) = {
    Future.all {
      values.map { value=>
        sync(value) {
          func(value)
        }
      }
    }
  }

  implicit def wrap_future_result[T](value:T):FutureResult[T] = {
    val rc = FutureResult[T]()
    rc.apply(Success(value))
    rc
  }

  implicit def unwrap_future_result[T](value:FutureResult[T]):T = {
    value.await() match {
      case Success(value) => value
      case Failure(value) => throw value
    }
  }


}