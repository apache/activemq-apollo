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

import dto.{MembershipMonitorDTO, NetworkManagerDTO, JVMMembershipMonitorDTO, ClusterMemberDTO}
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.dto.{CustomServiceDTO, VirtualHostDTO}
import java.util.concurrent.TimeUnit
import org.apache.activemq.apollo.broker.{BrokerRegistry, Broker}
import collection.mutable.HashSet
import org.apache.activemq.apollo.util._

trait MembershipMonitor extends Service {
  var listener:MembershipListener = _
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object MembershipMonitorFactory {

  trait SPI {
    def create(broker:Broker, dto:MembershipMonitorDTO):MembershipMonitor
  }

  val finder = new ClassFinder[SPI]("META-INF/services/org.apache.activemq.apollo/cluster-membership-factory.index",classOf[SPI])

  def create(broker:Broker, dto:MembershipMonitorDTO):MembershipMonitor = {
    if( dto == null ) {
      return null
    }
    finder.singletons.foreach { provider=>
      val rc = provider.create(broker, dto)
      if( rc!=null ) {
        return rc;
      }
    }
    return null
  }
}


trait MembershipListener {
  def on_membership_change(members:collection.Set[ClusterMemberDTO])
}

case class StaticMembershipMonitor(members:collection.Set[ClusterMemberDTO]) extends BaseService with MembershipMonitor {
  val dispatch_queue = createQueue("static cluster membership manager")
  protected def _start(on_completed: Task) = {
    dispatch_queue {
      listener.on_membership_change(members)
    }
    on_completed.run()
  }
  protected def _stop(on_completed: Task) = {
    on_completed.run()
  }
}

object JVMMembershipMonitorFactory extends MembershipMonitorFactory.SPI {
  def create(broker: Broker, dto: MembershipMonitorDTO): MembershipMonitor = dto match {
    case dto:JVMMembershipMonitorDTO=> new JVMMembershipMonitor
    case _ => null
  }
}

class JVMMembershipMonitor extends BaseService with MembershipMonitor {
  val dispatch_queue = createQueue("jvm cluster membership manager")
  protected def _start(on_completed: Task) = {
    schedule_reoccurring(1, TimeUnit.SECONDS) {
      val brokers = HashSet[ClusterMemberDTO]()
      for( broker <- BrokerRegistry.list() ) {
        import collection.JavaConversions._
        broker.config.services.foreach(_ match {
          case x:NetworkManagerDTO=>
            if(x.self!=null ) {
              if(NetworkManager.has_variables(x.self.id)) {
                for( host <- broker.virtual_hosts.values ) {
                  var resolved = NetworkManager.resolve_variables(x.self, broker, host)
                  if( !NetworkManager.has_variables(resolved) ) {
                    brokers += resolved
                  } else {
                    println("not resolved")
                  }
                }
              } else {
                var resolved = NetworkManager.resolve_variables(x.self, broker, null)
                if( !NetworkManager.has_variables(resolved) ) {
                  brokers += resolved
                } else {
                  println("not resolved")
                }
              }
            }
          case _ =>
        })
      }
      listener.on_membership_change(brokers)
    }
    on_completed.run()
  }

  protected def _stop(on_completed: Task) = on_completed.run()
}

case class MulitMonitor(monitors:Seq[MembershipMonitor]) extends BaseService with MembershipMonitor {

  val dispatch_queue = createQueue("static cluster membership manager")

  case class MonitorData(monitor:MembershipMonitor) {
    var last:collection.Set[ClusterMemberDTO] = HashSet[ClusterMemberDTO]();
  }
  
  val monitor_data = monitors.map(MonitorData(_))

  protected def _start(on_completed: Task) = {
    val tracker = new LoggingTracker("membership monitor start")
    for( data <- monitor_data ) {
      data.monitor.listener = new MembershipListener {
        def on_membership_change(members: collection.Set[ClusterMemberDTO]) = dispatch_queue {
          if( data.last != members ) {
            data.last = members
            fire_changes
          }
        }
      }
      tracker.start(data.monitor)
    }
    tracker.callback(on_completed)
  }

  def fire_changes = {
    val members = HashSet[ClusterMemberDTO]()
    monitor_data.foreach(x=> members ++= x.last )
    listener.on_membership_change(members)
  }

  protected def _stop(on_completed: Task) = {
    val tracker = new LoggingTracker("membership monitor stop")
    for( data <- monitor_data ) {
      tracker.stop(data.monitor)
    }
    tracker.callback(^{
      on_completed.run()
    })
  }
}