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

import org.fusesource.hawtdispatch._
import org.fusesource.hawtdispatch.{TaskTracker, DispatchQueue}

/**
 * <p>
 * A TaskTracker which logs an informational message if that tasks don't complete
 * within the timeout.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class LoggingTracker(name:String, val log:Log=Log(classOf[LoggingTracker]), timeout:Long=1000) extends TaskTracker(name, timeout) {
  assert(log!=null)
  import log._

  var status:Option[(Long, List[String])] = None

  override protected def onTimeout(startedAt:Long, tasks: List[String]):Long = {
    status match {
      case None =>
        info("%s is waiting on %s", name, tasks.mkString(", "))
        status = Some((startedAt, tasks))
      case Some(data)=>
        if( data._2 != tasks ) {
          info("%s is now waiting on %s", name, tasks.mkString(", "))
          status = Some((startedAt, tasks))
        }
    }
    timeout
  }


  override def callback(handler: Runnable) {
    super.callback(^{
      status match {
        case None =>
        case Some(data)=>
          info("%s is no longer waiting.  It waited a total of %d seconds.", name, ((System.currentTimeMillis()-data._1)/1000))
          status = None
      }
      handler.run();
    })
  }

  def start(service:Service) = {
    service.start(task("start "+service))
  }

  def stop(service:Service) = {
    service.stop(task("stop "+service))
  }

}
