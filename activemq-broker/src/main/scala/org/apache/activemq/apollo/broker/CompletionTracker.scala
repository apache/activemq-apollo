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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.HashSet
import org.fusesource.hawtdispatch.DispatchQueue
import org.fusesource.hawtdispatch.ScalaDispatch._

object CompletionTracker extends Log

/**
 * <p>
 * A CompletionTracker is used to track multiple async processing tasks and
 * call a callback once they all complete.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class CompletionTracker(val name:String, val parent:DispatchQueue=getGlobalQueue) extends Logging {

  override protected def log = CompletionTracker

  private[this] val tasks = new HashSet[Runnable]()
  private[this] var _callback:Runnable = null
  val queue = parent.createSerialQueue("tracker: "+name);

  def task(name:String="unknown"):Runnable = {
    val rc = new Runnable() {
      def run = {
        trace("completed task: %s", name)
        remove(this)
      }
      override def toString = name 
    }
    ^ {
      assert(_callback==null)
      tasks.add(rc)
    }  >>: queue
    return rc
  }

  def callback(handler: =>Unit ) {
    var start = System.currentTimeMillis
    ^ {
      _callback = handler _
      checkDone()
    }  >>: queue

    def displayStatus = {
      if( _callback!=null ) {
        val duration = (System.currentTimeMillis-start)/1000
        info("%s is taking a long time (%d seconds). Waiting on %s", name, duration, tasks)
        schedualCheck
      }
    }
    def schedualCheck:Unit = queue.dispatchAfter(1, TimeUnit.SECONDS, ^{displayStatus})
    schedualCheck
  }

  private def remove(r:Runnable) = ^{
    if( tasks.remove(r) ) {
      checkDone()
    }
  } >>: queue

  private def checkDone() = {
    if( tasks.isEmpty && _callback!=null ) {
      trace("executing callback for %s", name)
      _callback >>: queue
      _callback = null
      queue.release
    } else {
      if( _callback==null ) {
        trace("still for callback to bet set")
      }
      if( _callback==null ) {
        trace("still waiting for tasks %s", tasks)
      }
    }
  }

  def await() = {
    val latch =new CountDownLatch(1)
    callback {
      latch.countDown
    }
    latch.await
  }

  def await(timeout:Long, unit:TimeUnit) = {
    val latch = new CountDownLatch(1)
    callback {
      latch.countDown
    }
    latch.await(timeout, unit)
  }

  override def toString = tasks.synchronized { name+" waiting on: "+tasks }
}
