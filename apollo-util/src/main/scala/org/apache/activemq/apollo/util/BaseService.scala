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

import org.fusesource.hawtdispatch.DispatchQueue
import org.fusesource.hawtdispatch._

object BaseService extends Log

/**
 * <p>
 * The BaseService provides helpers for dealing async service state.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait BaseService extends Service with Logging {

  override protected def log:Log = BaseService

  sealed class State {

    val since = System.currentTimeMillis

    override def toString = getClass.getSimpleName
    def isCreated = false
    def isStarting = false
    def isStarted = false
    def isStopping = false
    def isStopped= false
    def isFailed= false
  }

  trait CallbackSupport {
    var callbacks:List[Runnable] = Nil
    def << (r:Runnable) = if(r!=null) { callbacks ::= r }
    def done = { callbacks.foreach(_.run); callbacks=Nil }
  }

  protected class CREATED extends State { override def isCreated = true  }
  protected class STARTING extends State with CallbackSupport { override def isStarting = true  }
  protected class FAILED extends State { override def isFailed = true  }
  protected class STARTED extends State { override def isStarted = true  }
  protected class STOPPING extends State with CallbackSupport { override def isStopping = true  }
  protected class STOPPED extends State { override def isStopped = true  }

  protected val dispatchQueue:DispatchQueue

  final def start() = start(null)
  final def stop() = stop(null)

  @volatile
  protected var _serviceState:State = new CREATED

  def serviceState = _serviceState

  @volatile
  protected var _serviceFailure:Exception = null
  def serviceFailure = _serviceFailure

  final def start(onCompleted:Runnable) = ^{
    def do_start = {
      val state = new STARTING()
      state << onCompleted
      _serviceState = state
      try {
        _start(^ {
          _serviceState = new STARTED
          state.done
        })
      }
      catch {
        case e:Exception =>
          error(e, "Start failed due to %s", e)
          _serviceFailure = e
          _serviceState = new FAILED
          state.done
      }
    }
    def done = {
      if( onCompleted!=null ) {
        onCompleted.run
      }
    }
    _serviceState match {
      case state:CREATED =>
        do_start
      case state:STOPPED =>
        do_start
      case state:STARTING =>
        state << onCompleted
      case state:STARTED =>
        done
      case state =>
        done
        error("Start should not be called from state: %s", state);
    }
  } |>>: dispatchQueue

  final def stop(onCompleted:Runnable) = {
    def stop_task = {
      def done = {
        if( onCompleted!=null ) {
          onCompleted.run
        }
      }
      _serviceState match {
        case state:STARTED =>
          val state = new STOPPING
          state << onCompleted
          _serviceState = state
          try {
            _stop(^ {
              _serviceState = new STOPPED
              state.done
            })
          }
          catch {
            case e:Exception =>
              error(e, "Stop failed due to: %s", e)
              _serviceFailure = e
              _serviceState = new FAILED
              state.done
          }
        case state:STOPPING =>
          state << onCompleted
        case state:STOPPED =>
          done
        case state =>
          done
          error("Stop should not be called from state: %s", state);
      }
    }
    ^{ stop_task } |>>: dispatchQueue
  }

  protected def _start(onCompleted:Runnable)
  protected def _stop(onCompleted:Runnable)

}
