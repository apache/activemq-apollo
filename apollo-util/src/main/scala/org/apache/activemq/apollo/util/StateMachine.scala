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

/**
 * Used to enforce a single state on an object.  The object can only
 * transitioned to a new state by the current state of the of the object.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract class StateMachine {

  /**
   * A state of the object.
   */
  abstract class State {

    def init() = {}

    /**
     * Changes to the new state only if it is still the current state.
     * @param next
     */
    final protected def become(next: State) = {
      if( _state == this ) {
        _state = next
        next.init()
      }
    }

    /**
     * Executes the code block only if we are still the current state.
     * @param func
     */
    final def react(func: =>Unit) = {
      if( _state == this ) {
        func
      }
    }
  }

  case class Pause(next:State) extends State {
    def continue = become(next)
  }


  private var _state = init()
  protected def init():State
  def state = _state

  def react[T <: State : Manifest](func: (T)=>Unit) = {
    var m = manifest[T]
    if( m.runtimeClass == _state.getClass ) {
      func(_state.asInstanceOf[T])
    }
  }

}