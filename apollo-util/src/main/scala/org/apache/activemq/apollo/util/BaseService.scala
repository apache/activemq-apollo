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
import collection.mutable.ListBuffer

object BaseService extends Log

/**
 * <p>
 * The BaseService provides helpers for dealing async service state.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait BaseService extends Service with Dispatched {

  import BaseService._

  sealed class State {

    val since = System.currentTimeMillis

    override def toString = getClass.getSimpleName
    def is_created = false
    def is_starting = false
    def is_started = false
    def is_upward = is_started | is_starting | is_created

    def is_starting_or_started = is_starting | is_started

    def is_stopping = false
    def is_stopped= false
    def is_failed= false
    def is_downward = is_stopped | is_stopping | is_failed
  }

  protected class CREATED extends State { override def is_created = true  }
  protected class STARTING extends State { override def is_starting = true  }
  protected class FAILED extends State { override def is_failed = true  }
  protected class STARTED extends State { override def is_started = true  }
  protected class STOPPING extends State { override def is_stopping = true  }
  protected class STOPPED extends State { override def is_stopped = true  }

  @volatile
  protected var _service_state:State = new CREATED

  def service_state = _service_state

  @volatile
  protected var _serviceFailure:Exception = null
  def serviceFailure = _serviceFailure

  private val pending_actions = ListBuffer[Task]()

  final def start(on_completed:Task):Unit = {
    def start_task:Task = ^{
      def done = {
        pending_actions.foreach(dispatch_queue.execute _)
        pending_actions.clear()
        if( on_completed!=null ) {
          on_completed.run
        }
      }

      def do_start = {
        val state = new STARTING()
        _service_state = state
        try {
          _start(^ {
            _service_state = new STARTED
            done
          })
        }
        catch {
          case e:Exception =>
            error(e, "Start failed due to %s", e)
            _serviceFailure = e
            _service_state = new FAILED
            done
        }
      }

      _service_state match {
        case state:CREATED =>
          do_start
        case state:STOPPED =>
          do_start
        case state:STOPPING =>
          pending_actions += start_task
        case state:STARTING =>
          pending_actions += start_task
        case state:STARTED =>
          done
        case state =>
          done
          error("Start should not be called from state: %s", state);
      }
    }
    start_task >>: dispatch_queue
  }

  final def stop(on_completed:Task):Unit = {
    def stop_task:Task = ^{
      def done = {
        val tmp = pending_actions.toArray
        pending_actions.clear
        tmp.foreach(dispatch_queue.execute _)
        if( on_completed!=null ) {
          on_completed.run
        }
      }
      _service_state match {
        case state:STARTED =>
          val state = new STOPPING
          _service_state = state
          try {
            _stop(^ {
              _service_state = new STOPPED
              done
            })
          }
          catch {
            case e:Exception =>
              error(e, "Stop failed due to: %s", e)
              _serviceFailure = e
              _service_state = new FAILED
              done
          }
        case state:CREATED =>
          done
        case state:STOPPED =>
          done
        case state:STOPPING =>
          pending_actions += stop_task
        case state:STARTING =>
          pending_actions += stop_task
        case state =>
          done
          error("Stop should not be called from state: %s", state);
      }
    }
    stop_task >>: dispatch_queue
  }

  protected def _start(on_completed:Task)
  protected def _stop(on_completed:Task)

}
