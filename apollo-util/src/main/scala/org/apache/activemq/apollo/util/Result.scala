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
import language.implicitConversions

object ResultSupport {

  case class RichResult[A,F](self: Result[A,F]) {
    def andThen[B](r2: =>Result[B,F]):Result[B,F] = {
      if( self.failed ) {
        Failure(self.failure)
      } else {
        r2
      }
    }
  }

  implicit def to_rich_result[A,F](value:Result[A,F]) = new RichResult[A,F](value)
}

/**
 * <p>A Result can either be a Success or a Failure</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
sealed abstract class Result[+S,+F] {
  def failed:Boolean

  def success:S
  def failure:F

  def success_option:Option[S] = if (failed) None else Some(success)
  def failure_option:Option[F] = if (failed) Some(failure) else None

  def map_success[B](f: S => B): Result[B, F] =
    if (failed) Failure(failure)  else Success(f(this.success))

  def map_failure[B](f: F => B): Result[S, B] =
    if (failed) Failure(f(this.failure)) else Success(this.success)

}

/**
 * <p>A Success Result</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final case class Success[+S](x: S) extends Result[S, Nothing] {
  def get = x
  def success = x
  def failure = throw new NoSuchElementException("Success.failure")
  def failed = false
}

/**
 * <p>A Failure Result</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
final case class Failure[+F](x: F) extends Result[Nothing,F] {
  def get = x
  def success = throw new NoSuchElementException("Failure.success")
  def failure = x
  def failed = true
}

sealed class Zilch
final case object Zilch extends Zilch