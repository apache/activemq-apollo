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

/**
 * <p>
 * Trait that exposes the {@link DispatchQueue} used to guard
 * mutable access to the state of the object implementing this interface.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Dispatched {
  def dispatch_queue:DispatchQueue
  def assert_executing = dispatch_queue.assertExecuting()
}

trait DeferringDispatched extends Dispatched {

  def defer(func: =>Unit) = {
    dispatch_queue_task_source.merge(new Task(){
      def run() {
        func
      }
    })
  }

  lazy val dispatch_queue_task_source = {
    val x = createSource(new ListEventAggregator[Task](), dispatch_queue)
    x.setEventHandler(^{ x.getData.foreach(_.run()) });
    x.resume()
    x
  }
}