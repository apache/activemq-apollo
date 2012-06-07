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
package org.apache.activemq.apollo.broker.network

import dto.ClusterMemberDTO
import org.apache.activemq.apollo.util.{BaseService, Service}
import org.fusesource.hawtdispatch._

trait ClusterMembershipMonitor extends Service {
  var listener:ClusterMembershipListener = _
}

trait ClusterMembershipListener {
  def on_cluster_change(members:Set[ClusterMemberDTO])
}

case class StaticClusterMembershipMonitor(members:Set[ClusterMemberDTO]) extends BaseService with ClusterMembershipMonitor {
  val dispatch_queue = createQueue("bridge manager")
  protected def _start(on_completed: Task) = {
    dispatch_queue {
      listener.on_cluster_change(members)
    }
    on_completed.run()
  }
  protected def _stop(on_completed: Task) = {
    on_completed.run()
  }
}
