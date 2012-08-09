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

import dto.LoadStatusDTO
import org.apache.activemq.apollo.dto.JsonCodec
import dto.ClusterMemberDTO
import org.fusesource.hawtdispatch._
import collection.mutable.HashMap
import java.net.URL
import java.util.concurrent.TimeUnit._
import org.apache.activemq.apollo.broker.Broker
import org.apache.activemq.apollo.util.FileSupport._
import org.apache.activemq.apollo.util.{Log, StateMachine, BaseService, Service}

/**
 *
 */
trait BrokerLoadMonitor extends Service {
  var listener:BrokerLoadListener = _
  def add(member:ClusterMemberDTO)
  def remove(member:ClusterMemberDTO)
}

trait BrokerLoadListener {
  def on_load_change(broker_load:LoadStatusDTO)
}
object RestLoadMonitor extends Log
class RestLoadMonitor(manager:NetworkManager) extends BaseService with BrokerLoadMonitor {
  import collection.JavaConversions._
  import RestLoadMonitor._
  
  val dispatch_queue = createQueue("rest load monitor")
  val members = HashMap[String, LoadMonitor]()

  protected def _start(on_completed: Task) = {
    schedule_reoccurring(manager.monitoring_interval, SECONDS) {
      for(monitor <- members.values) {
        monitor.poll
      }
    }
    on_completed.run()
  }

  protected def _stop(on_completed: Task) = {
    on_completed.run()
  }

  case class LoadMonitor(id:String, url:URL) extends StateMachine {
    var next_poll = Broker.now

    protected def init() = IdleState()
    
    case class IdleState() extends State {
      def poll = react {
        become(PollingState())
      }
    }
    
    case class PollingState() extends State {
      override def init() = {
        val started = Broker.now

        // Switch to the blockable thread poll since accessing the data
        // is a blocking operation.
        Broker.BLOCKABLE_THREAD_POOL {
          try {
            val dto = using(url.openStream()) {
              is =>
                JsonCodec.mapper.readValue(is, classOf[LoadStatusDTO])
            }
            dto.id = id
            dto.timestamp = Broker.now
            listener.on_load_change(dto)
          } catch {
            case e => debug(e, "Failed poll broker load of %s", id)
          }
          dispatch_queue {
            become(IdleState())
          }
        }
      }
    }
    
    def poll = {
      if ( Broker.now >= next_poll ) {
        state match {
          case state:IdleState => state.poll
          case _ =>
        }
      }
    }
  }

  def add(member: ClusterMemberDTO) = {
    dispatch_queue {
      for(service <- member.services) {
        if( service.kind == "web_admin" ) {
          var monitor = LoadMonitor(member.id, new URL(service.address))
          members.put(member.id, monitor)
          monitor.poll
        }
      }
    }
  }

  def remove(member: ClusterMemberDTO) = dispatch_queue {
    members.remove(member.id)
  }

}