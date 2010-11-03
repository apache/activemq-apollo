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
package org.apache.activemq.apollo.web.resources;

import javax.ws.rs._
import core.Response
import Response.Status._
import java.util.List
import org.apache.activemq.apollo.dto._
import java.{lang => jl}
import collection.JavaConversions
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.broker._
import collection.mutable.ListBuffer

/**
 * <p>
 * The RuntimeResource resource manages access to the runtime state of a broker.  It is used
 * to see the status of the broker and to apply management operations against the broker.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@Produces(Array("application/json", "application/xml","text/xml", "text/html;qs=5"))
case class RuntimeResource(parent:BrokerResource) extends Resource(parent) {

  private def with_broker[T](func: (org.apache.activemq.apollo.broker.Broker, Option[T]=>Unit)=>Unit):T = {
    val broker:org.apache.activemq.apollo.broker.Broker = BrokerRegistry.get(parent.id)
    if( broker == null ) {
      result(NOT_FOUND)
    } else {
      Future[Option[T]] { cb=>
        broker.dispatchQueue {
          func(broker, cb)
        }
      }.getOrElse(result(NOT_FOUND))
    }
  }

  private def with_virtual_host[T](id:Long)(func: (VirtualHost, Option[T]=>Unit)=>Unit):T = {
    with_broker { case (broker, cb) =>
      broker.virtualHosts.valuesIterator.find( _.id == id) match {
        case Some(virtualHost)=>
          virtualHost.dispatchQueue {
            func(virtualHost, cb)
          }
        case None=> cb(None)
      }
    }
  }


  @GET
  def get() = {
    with_broker[BrokerStatusDTO] { case (broker, cb) =>
      val result = new BrokerStatusDTO

      result.id = broker.id
      result.current_time = System.currentTimeMillis
      result.state = broker.serviceState.toString
      result.state_since = broker.serviceState.since
      result.config = broker.config

      broker.virtualHosts.values.foreach{ host=>
        // TODO: may need to sync /w virtual host's dispatch queue
        result.virtual_hosts.add( new LongIdLabeledDTO(host.id, host.config.id) )
      }

      broker.connectors.foreach{ c=>
        result.connectors.add( new LongIdLabeledDTO(c.id, c.config.id) )
      }

      broker.connectors.foreach{ connector=>
        connector.connections.foreach { case (id,connection) =>
          // TODO: may need to sync /w connection's dispatch queue
          result.connections.add( new LongIdLabeledDTO(id, connection.transport.getRemoteAddress ) )
        }
      }

      cb(Some(result))
    }
  }


  @GET @Path("virtual-hosts")
  def virtualHosts = {
    val rc = new LongIdListDTO
    rc.items.addAll(get.virtual_hosts)
    rc
  }

  @GET @Path("virtual-hosts/{id}")
  def virtualHost(@PathParam("id") id : Long):VirtualHostStatusDTO = {
    with_virtual_host(id) { case (virtualHost,cb) =>
      val result = new VirtualHostStatusDTO
      result.id = virtualHost.id
      result.state = virtualHost.serviceState.toString
      result.state_since = virtualHost.serviceState.since
      result.config = virtualHost.config

      virtualHost.router.routing_nodes.foreach { node=>
        result.destinations.add(new LongIdLabeledDTO(node.id, node.name.toString))
      }

      if( virtualHost.store != null ) {
        virtualHost.store.storeStatusDTO { x=>
          result.store = x
          cb(Some(result))
        }
      } else {
        cb(Some(result))
      }
    }
  }

  @GET @Path("virtual-hosts/{id}/store")
  def store(@PathParam("id") id : Long):StoreStatusDTO = {
    val rc =  virtualHost(id).store
    if( rc == null ) {
      result(NOT_FOUND)
    }
    rc
  }

  @GET @Path("virtual-hosts/{id}/destinations/{dest}")
  def destination(@PathParam("id") id : Long, @PathParam("dest") dest : Long):DestinationStatusDTO = {
    with_virtual_host(id) { case (virtualHost,cb) =>
      cb(virtualHost.router.routing_nodes.find { _.id == dest } map { node=>
        val result = new DestinationStatusDTO
        result.id = node.id
        result.name = node.name.toString
        node.queues.foreach { q=>
          result.queues.add(new LongIdLabeledDTO(q.id, q.binding.label))
        }
        node.broadcast_consumers.flatMap( _.connection ).foreach { connection=>
          result.consumers.add(new LongIdLabeledDTO(connection.id, connection.transport.getRemoteAddress))
        }
        node.broadcast_producers.flatMap( _.producer.connection ).foreach { connection=>
          result.producers.add(new LongIdLabeledDTO(connection.id, connection.transport.getRemoteAddress))
        }

        result
      })
    }
  }

  @GET @Path("virtual-hosts/{id}/destinations/{dest}/queues/{queue}")
  def queue(@PathParam("id") id : Long, @PathParam("dest") dest : Long, @PathParam("queue") qid : Long, @QueryParam("entries") entries:Boolean ):QueueStatusDTO = {
    with_virtual_host(id) { case (virtualHost,cb) =>
      import JavaConversions._
      val rc = virtualHost.router.routing_nodes.find { _.id == dest } flatMap { node=>
        node.queues.find  { _.id == qid } map { q=>

          val result = new QueueStatusDTO
          result.id = q.id
          result.label = q.binding.label
          result.capacity_used = q.capacity_used
          result.capacity = q.capacity

          result.enqueue_item_counter = q.enqueue_item_counter
          result.dequeue_item_counter = q.dequeue_item_counter
          result.enqueue_size_counter = q.enqueue_size_counter
          result.dequeue_size_counter = q.dequeue_size_counter
          result.nack_item_counter = q.nack_item_counter
          result.nack_size_counter = q.nack_size_counter

          result.queue_size = q.queue_size
          result.queue_items = q.queue_items

          result.loading_size = q.loading_size
          result.flushing_size = q.flushing_size
          result.flushed_items = q.flushed_items

          if( entries ) {
            var cur = q.head_entry
            while( cur!=null ) {

              val e = new EntryStatusDTO
              e.seq = cur.seq
              e.count = cur.count
              e.size = cur.size
              e.consumer_count = cur.parked.size
              e.prefetch_count = cur.prefetched
              e.state = cur.label

              result.entries.add(e)

              cur = if( cur == q.tail_entry ) {
                null
              } else {
                cur.nextOrTail
              }
            }
          }

          q.inbound_sessions.flatMap( _.producer.connection ).foreach { connection=>
            result.producers.add(new LongIdLabeledDTO(connection.id, connection.transport.getRemoteAddress))
          }
          q.all_subscriptions.keysIterator.toSeq.flatMap( _.connection ).foreach { connection=>
            result.consumers.add(new LongIdLabeledDTO(connection.id, connection.transport.getRemoteAddress))
          }

          result
        }
      }
      cb(rc)
    }
  }


  @GET @Path("connectors")
  def connectors = {
    val rc = new LongIdListDTO
    rc.items.addAll(get.connectors)
    rc
  }

  @GET @Path("connectors/{id}")
  def connector(@PathParam("id") id : Long):ConnectorStatusDTO = {
    with_broker { case (broker, cb) =>
      broker.connectors.find(_.id == id) match {
        case None=> cb(None)
        case Some(connector)=>

          val result = new ConnectorStatusDTO
          result.id = connector.id
          result.state = connector.serviceState.toString
          result.state_since = connector.serviceState.since
          result.config = connector.config

          result.accepted = connector.accept_counter.get
          connector.connections.foreach { case (id,connection) =>
            // TODO: may need to sync /w connection's dispatch queue
            result.connections.add( new LongIdLabeledDTO(id, connection.transport.getRemoteAddress ) )
          }

          cb(Some(result))
      }
    }
  }


  @GET @Path("connections")
  def connections:LongIdListDTO = {
    with_broker { case (broker, cb) =>
      val rc = new LongIdListDTO

      broker.connectors.foreach { connector=>
        connector.connections.foreach { case (id,connection) =>
          // TODO: may need to sync /w connection's dispatch queue
          rc.items.add(new LongIdLabeledDTO(id, connection.transport.getRemoteAddress ))
        }
      }
      
      cb(Some(rc))
    }
  }

  @GET @Path("connections/{id}")
  def connections(@PathParam("id") id : Long):ConnectionStatusDTO = {
    with_broker { case (broker, cb) =>
      broker.connectors.flatMap{ _.connections.get(id) }.headOption match {
        case None => cb(None)
        case Some(connection:BrokerConnection) =>
          connection.dispatchQueue {
            cb(Some(connection.get_connection_status))
          }
      }
    }
  }

}
