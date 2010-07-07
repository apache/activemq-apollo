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
package org.apache.activemq.apollo.broker

import org.apache.activemq.Service
import org.fusesource.hawtdispatch.DispatchQueue
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._

/**
 * <p>
 * The BaseService provides helpers for dealing async service state.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait BaseService extends Service {

  sealed class State {
    override def toString = getClass.getSimpleName
  }

  trait CallbackSupport {
    var callbacks:List[Runnable] = Nil
    def << (r:Runnable) = if(r!=null) { callbacks ::= r }
    def done = callbacks.foreach(_.run)
  }

  object CREATED extends State
  class  STARTING extends State with CallbackSupport
  object STARTED extends State
  class  STOPPING extends State with CallbackSupport
  object STOPPED extends State

  val dispatchQueue:DispatchQueue

  final def start() = start(null)
  final def stop() = stop(null)

  protected var _serviceState:State = CREATED
  protected def serviceState = _serviceState

  private def error(msg:String) {
    try {
      throw new AssertionError(msg)
    } catch {
      case e:Exception =>
      e.printStackTrace
    }
  }

  final def start(onCompleted:Runnable) = ^{
    def do_start = {
      val state = new STARTING()
      state << onCompleted
      _serviceState = state
      _start(^{
        _serviceState = STARTED
        state.done
      })
    }
    def done = {
      if( onCompleted!=null ) {
        onCompleted.run
      }
    }
    _serviceState match {
      case CREATED =>
        do_start
      case STOPPED =>
        do_start
      case state:STARTING =>
        state << onCompleted
      case STARTED =>
        done
      case state =>
        done
        error("start should not be called from state: "+state);
    }
  } |>>: dispatchQueue

  final def stop(onCompleted:Runnable) = ^{
    def done = {
      if( onCompleted!=null ) {
        onCompleted.run
      }
    }
    _serviceState match {
      case STARTED =>
        val state = new STOPPING
        state << onCompleted
        _serviceState = state
        _stop(^{
          _serviceState = STOPPED
          state.done
        })
      case state:STOPPING =>
        state << onCompleted
      case STOPPED =>
        done
      case state =>
        done
        error("stop should not be called from state: "+state);
    }
  } |>>: dispatchQueue

  protected def _start(onCompleted:Runnable)
  protected def _stop(onCompleted:Runnable)

}
