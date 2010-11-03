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

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.HashSet
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
class LoggingTracker(name:String, parent:DispatchQueue=globalQueue) extends TaskTracker(name, parent) with Logging {

  timeout = 1000;

  override protected def log = LoggingTracker

  override protected def onTimeout(duration:Long, tasks: List[String]):Long = {
    info("%s is taking a long time (%d seconds). Waiting on %s", name, (duration/1000), tasks)
    timeout
  }

  def start(service:Service) = {
    service.start(task(service.toString))
  }

  def stop(service:Service) = {
    service.stop(task(service.toString))
  }

}

object LoggingTracker extends Log {
  def apply[R](name:String, parent:DispatchQueue=globalQueue)(func: (LoggingTracker)=>Unit ) = {
    val t = new LoggingTracker(name, parent)
    func(t)
    t.await
  }
}
