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

import org.apache.activemq.apollo.util._
import org.fusesource.hawtdispatch._
import dto._
import CollectionsSupport._
import java.util.concurrent.TimeUnit._
import collection.mutable.{LinkedHashMap, HashSet, ListBuffer, HashMap}
import org.apache.activemq.apollo.dto.CustomServiceDTO
import org.apache.activemq.apollo.broker.{AcceptingConnector, VirtualHost, Broker, CustomServiceFactory}
import java.net.InetSocketAddress

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object NetworkManagerFactory extends CustomServiceFactory with Log {
  def create(broker: Broker, dto: CustomServiceDTO): Service = dto match {
    case dto:NetworkManagerDTO => 
      val rc = new NetworkManager(broker)
      rc.config = dto
      rc
    case _ => null
  }
}

object NetworkManager extends Log {
  
  def has_variables(x:String) = x.contains("{{"):Boolean

  def has_variables(dto:ClusterMemberDTO):Boolean = {
    import collection.JavaConversions._
    has_variables(dto.id) || dto.services.foldLeft(false){ case (x,y) =>
      x || has_variables(y.address)
    }
  }
  
  def resolve_variables(dto:ClusterMemberDTO, broker:Broker, host:VirtualHost):ClusterMemberDTO = {
    import collection.JavaConversions._
    def resolve(x:String) = if( !x.contains("{{") ) { x } else {
      var rc = x;
      if( host!=null ) {
        rc = rc.replaceAllLiterally("{{host}}", host.id)
      }
      if( broker.web_server!=null && broker.web_server.uris()!=null && !broker.web_server.uris().isEmpty) {
        rc = rc.replaceAllLiterally("{{web_admin.url}}", broker.web_server.uris()(0).toString.stripSuffix("/"))
      }
      for( (id, connector) <- broker.connectors ) {
        connector match {
          case connector:AcceptingConnector =>
            connector.socket_address match {
              case address:InetSocketAddress =>
                rc = rc.replaceAllLiterally("{{connector."+id+".port}}", ""+address.getPort)
              case _ =>
            }
          case _ =>
        }
      }
      rc
    }

    val rc = new ClusterMemberDTO
    rc.id = resolve(dto.id)
    for( service <- dto.services) {
      val s = new ClusterServiceDTO
      s.kind = service.kind
      s.address = resolve(service.address)
      rc.services.add(s)
    }
    rc
  }
}

class NetworkManager(broker: Broker) extends BaseService with MembershipListener with BrokerLoadListener {
  import NetworkManager._

  val dispatch_queue = createQueue("bridge manager")

  var config = new NetworkManagerDTO
  var membership_monitor:MembershipMonitor = _
  var members = collection.Set[ClusterMemberDTO]()
  var members_by_id = HashMap[String, ClusterMemberDTO]()
  var load_monitor: BrokerLoadMonitor = _
  var metrics_map = HashMap[String, BrokerMetrics]()
  val bridges = HashMap[BridgeInfo, BridgeDeployer]()

  def network_user = Option(config.user).getOrElse("network")
  def network_password = config.password
  def monitoring_interval = OptionSupport(config.monitoring_interval).getOrElse(5)

  protected def _start(on_completed: Task) = {
    import collection.JavaConversions._

    // TODO: also support dynamic membership discovery..
    var monitors = List[MembershipMonitor]()
    var static_set = config.members.toSet
    if( !has_variables(config.self) ) {
      static_set += config.self
    }

    monitors ::= StaticMembershipMonitor(static_set)

    for( monitor_dto <- config.membership_monitors ) {
      var monitor = MembershipMonitorFactory.create(broker, monitor_dto)
      if(monitor!=null) {
        monitors ::= monitor
      } else {
        warn("Could not create the membership monitor for: "+monitor_dto)
      }
    }

    membership_monitor = MulitMonitor(monitors)
    membership_monitor.listener = this
    membership_monitor.start(NOOP)

    load_monitor = new RestLoadMonitor(this)
    load_monitor.listener = this
    load_monitor.start(NOOP)

    schedule_reoccurring(1, SECONDS) {
      load_analysis
    }
    on_completed.run()
  }

  protected def _stop(on_completed: Task) = {
    membership_monitor.stop(NOOP)
    on_completed.run()
  }

  def on_membership_change(value: collection.Set[ClusterMemberDTO]) = dispatch_queue {
    val (added, _, removed) = diff(members, value)
    for( m <- removed ) {
      info("Broker host left the network: %s", m.id)
      load_monitor.remove(m)
    }
    for( m <- added ) {
      info("Broker host joined the network: %s", m.id)
      load_monitor.add(m)
    }
    members = value
    members_by_id = HashMap(members.toSeq.map(x=> (x.id->x)) : _*)
  }

  def on_load_change(dto: LoadStatusDTO) = dispatch_queue {
    metrics_map.getOrElseUpdate(dto.id, new BrokerMetrics()).update(dto, network_user)
  }

  def load_analysis = {
    dispatch_queue.assertExecuting()

    // Lets remove load entries for members that were removed from the cluster.
    val keys = metrics_map.keySet
    val current = members.map(_.id).toSet
    metrics_map = metrics_map -- (keys -- current)


    class DemandStatus {
      val needs_consumers = ListBuffer[(String,DestinationMetrics)]()
      val has_consumers = ListBuffer[(String,DestinationMetrics)]()
    }

    val queue_demand_map = HashMap[String, DemandStatus]()

    for( (broker, broker_load) <- metrics_map) {
      for( (dest_name, dest) <- broker_load.queue_load ) {
        val status = queue_demand_map.getOrElseUpdate(dest_name, new DemandStatus)
        var needsmoreconsumers = needs_more_consumers(dest)
        if( can_bridge_from(broker) && needsmoreconsumers ) {
          // The broker needs more consumers to drain the queue..
          status.needs_consumers += (broker->dest)
        } else {
          // The broker can drain the queue of other brokers..
          if( can_bridge_to(broker) && dest.consumer_count > 0 ) {
            status.has_consumers += (broker->dest)
          }
        }
      }
    }

    val desired_bridges = HashSet[BridgeInfo]()
    for( (id, demand) <- queue_demand_map ) {
      for( (from, from_metrics)<- demand.needs_consumers; (to, to_metrics) <-demand.has_consumers ) {
        // we could get fancy and compare the to_metrics and from_metrics to avoid
        // setting up bridges that won't make a big difference..
        desired_bridges += BridgeInfo(from, to, "queue", id)
      }
    }

    val (bridges_added, _, bridges_removed) = diff(bridges.keySet, desired_bridges)

    // Stop and remove the bridges that are no longer needed..
    for( info <- bridges_removed ) {
      bridges.remove(info).get.undeploy
    }

    // Create and start the new bridges..
    for( info <- bridges_added ) {
      val controller = BridgeDeployer(info)
      bridges.put(info, controller)
      controller.deploy
    }

  }

  def is_local_broker_id(id:String):Boolean = {
    if( has_variables(config.self.id) ) {
      for( host <- broker.virtual_hosts.values ) {
        if( config.self.id.replaceAllLiterally("{{host}}", host.id) == id )
          return true
      }
      false
    } else {
      config.self.id == id
    }
  }

  def can_bridge_from(broker:String):Boolean = is_local_broker_id(broker)
  def can_bridge_to(broker:String):Boolean = {
    if ( is_local_broker_id(broker) ) {
      OptionSupport(config.duplex).getOrElse(false)
    } else {
      true
    }
  }

  def needs_more_consumers(dest:DestinationMetrics):Boolean = {

    // nothing to drain.. so no need for consumers.
    if( dest.message_size == 0 && dest.enqueue_size_rate.mean == 0) {
      return false
    }

    val drain_rate = dest.dequeue_size_rate - dest.enqueue_size_rate.mean
    if( drain_rate < 0 ) {
      // Not draining...
      return true
    }

    // Might need a consumer due to being drained too slowly..
    val drain_eta_in_seconds = dest.message_size / drain_rate
    return drain_eta_in_seconds > 60
  }


  val bridging_strategies = LinkedHashMap[String, BridgingStrategy]()
  bridging_strategies.put("stomp", new StompBridgingStrategy(this))

  case class BridgeDeployer(info:BridgeInfo) {

    def to = members_by_id.get(info.to)
    def from = members_by_id.get(info.from)

    var bridging_strategy:BridgingStrategy = _
    var bridging_strategy_info : BridgeInfo = _

    def deploy:Unit = {
      // Lets find a service kind that we can use to bridge...
      import collection.JavaConversions._
      for( to <- to ; from <-from ) {

        // bridging_strategies are kept in preferred order
        for( (service_kind, strategy) <- bridging_strategies ) {

          // Lets look to see if we can use the strategy with services exposed by the broker..
          for( to_service <- to.services; from_service <- from.services ) {
            if( bridging_strategy==null && to_service.kind==service_kind && to_service.kind==from_service.kind ) {
              bridging_strategy = strategy
              bridging_strategy_info = BridgeInfo(from_service.address, to_service.address, info.kind, info.dest)
              bridging_strategy.deploy( bridging_strategy_info )
            }
          }
        }
      }
    }

    def undeploy = {
      if( bridging_strategy!=null ) {
        bridging_strategy.undeploy(bridging_strategy_info)
        bridging_strategy = null
        bridging_strategy_info = null
      }
    }
  }

}

