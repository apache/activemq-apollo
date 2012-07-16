/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.broker.network

import dto._
import org.apache.activemq.jmdns._
import java.io.IOException

import java.net.InetAddress
import org.apache.activemq.apollo.broker.Broker
import org.apache.activemq.apollo.util.BaseService
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.dto.JsonCodec
import org.fusesource.hawtbuf.Buffer
import collection.mutable
import collection.mutable.ListBuffer
import java.util.concurrent.TimeUnit
import org.apache.activemq.apollo.util.CollectionsSupport._

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ZeroconfMembershipMonitorFactory extends MembershipMonitorFactory.SPI {
  def create(broker: Broker, dto: MembershipMonitorDTO): MembershipMonitor = dto match {
    case dto:ZeroconfMembershipMonitorDTO=> new ZeroconfMembershipMonitor(broker, dto)
    case _ => null
  }
}

/**
 * <p>
 *   Uses Zeroconf to discover the cluster members.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ZeroconfMembershipMonitor(val broker: Broker, val dto:ZeroconfMembershipMonitorDTO) extends BaseService with MembershipMonitor {

  def dispatch_queue: DispatchQueue = createQueue()
  val members = mutable.HashMap[String, ClusterMemberDTO]()
  var jmdns:JmDNS = _

  var localservices = Set[ServiceInfo]()

  protected def _start(on_completed: Task) = {

    if (dto.group == null) {
      throw new IOException("You must specify a group to discover")
    }

    jmdns = new JmDNS(if (dto.address != null) {
      InetAddress.getByName(dto.address)
    } else {
      InetAddress.getLocalHost
    })

    Broker.BLOCKABLE_THREAD_POOL.future {
      // Listen for service record events.
      val kind = "_" + dto.group + ".apollo."
      jmdns.addListener(new DNSListener {
        def updateRecord(jmdns: JmDNS, now: Long, record: DNSRecord) {
          if ( record.getName.endsWith(kind) ) {
            record match {
              case record:DNSRecord.Text =>
                var member = JsonCodec.decode(new Buffer(record.text), classOf[ClusterMemberDTO])
                if( record.isExpired(Broker.now) ) {
                  members.remove(member.id)
                } else {
                  members.put(member.id, member)
                }
                if ( listener!=null ) {
                  listener.on_membership_change(members.values.toSet)
                }
              case _ =>
            }
          }
        }
      }, null)

    }.onComplete(_ => on_completed.run() )

    schedule_reoccurring(1, TimeUnit.SECONDS) {

      val next = local_services.toSet
      val (added, _, removed) = diff(localservices, next)
      localservices = next

      if( !added.isEmpty || !removed.isEmpty) {
        Broker.BLOCKABLE_THREAD_POOL.future {
          for( s <- added ) {
            jmdns.registerService(s)
          }
          for( s <- removed ) {
            jmdns.unregisterService(s)
          }
        }
      }
    }
  }


  def local_services:Seq[ServiceInfo] = {
    import collection.JavaConversions._
    val kind = "_" + dto.group + ".apollo."
    val rc = ListBuffer[ServiceInfo]()
    broker.config.services.foreach(_ match {
      case x:NetworkManagerDTO=>
        if(x.self!=null ) {
          // multiple hosts??
          if(NetworkManager.has_variables(x.self.id)) {
            for( host <- broker.virtual_hosts.values.toIterable ) {
              var resolved = NetworkManager.resolve_variables(x.self, broker, host)
              if( !NetworkManager.has_variables(resolved) ) {
                rc += new ServiceInfo(kind, host.id, 0, 0, 0, JsonCodec.encode(resolved).toByteArray)
              }
            }
          } else {
            var resolved = NetworkManager.resolve_variables(x.self, broker, null)
            if( !NetworkManager.has_variables(resolved) ) {
              rc += new ServiceInfo(kind, x.self.id, 0, 0, 0, JsonCodec.encode(resolved).toByteArray)
            }
          }
        }
      case _ =>
    })
    rc
  }

  protected def _stop(on_completed: Task) = {
    Broker.BLOCKABLE_THREAD_POOL.future {
      jmdns.close()
      jmdns = null;
    }.onComplete(x => on_completed.run())
  }
}
