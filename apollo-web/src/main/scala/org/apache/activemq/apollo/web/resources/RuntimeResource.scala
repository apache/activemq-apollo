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
import scala.util.continuations._

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

  @POST
  @Path("action/shutdown")
  def command_shutdown:Unit = {
    info("JVM shutdown requested via web interface")

    // do the the exit async so that we don't
    // kill the current request.
    Broker.BLOCKABLE_THREAD_POOL {
      Thread.sleep(200);
      System.exit(0)
    }
  }
  private def with_broker[T](func: (org.apache.activemq.apollo.broker.Broker, Option[T]=>Unit)=>Unit):T = {
    BrokerRegistry.list.headOption match {
      case None=> result(NOT_FOUND)
      case Some(broker)=>

        Future[Option[T]] { cb=>
          broker.dispatch_queue {
            func(broker, cb)
          }
        }.getOrElse(result(NOT_FOUND))

    }
  }

  private def with_virtual_host[T](id:Long)(func: (VirtualHost, Option[T]=>Unit)=>Unit):T = {
    with_broker { case (broker, cb) =>
      broker.virtual_hosts.valuesIterator.find( _.id == id) match {
        case Some(virtualHost)=>
          virtualHost.dispatch_queue {
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
      result.state = broker.service_state.toString
      result.state_since = broker.service_state.since
      result.config = broker.config

      broker.virtual_hosts.values.foreach{ host=>
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
      result.state = virtualHost.service_state.toString
      result.state_since = virtualHost.service_state.since
      result.config = virtualHost.config

      virtualHost.router.routing_nodes.foreach { node=>
        result.destinations.add(new LongIdLabeledDTO(node.id, node.name.toString))
      }

      if( virtualHost.store != null ) {
        virtualHost.store.get_store_status { x=>
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

  def link(connection:BrokerConnection) = {
    val link = new LinkDTO()
    link.kind = "connection"
    link.ref = connection.id.toString
    link.label = connection.transport.getRemoteAddress
    link
  }

  def link(queue:Queue) = {
    val link = new LinkDTO()
    link.kind = "queue"
    link.ref = queue.id.toString
    link.label = queue.binding.label
    link
  }

  @GET @Path("virtual-hosts/{id}/destinations/{dest}")
  def destination(@PathParam("id") id : Long, @PathParam("dest") dest : Long):DestinationStatusDTO = {
    with_virtual_host(id) { case (virtualHost,cb) =>
      cb(virtualHost.router.routing_nodes.find { _.id == dest } map { node=>
        val result = new DestinationStatusDTO
        result.id = node.id
        result.name = node.name.toString
        result.config = node.config
        node.queues.foreach { q=>
          result.queues.add(new LongIdLabeledDTO(q.id, q.binding.label))
        }
        node.broadcast_consumers.foreach { consumer=>
          consumer match {
            case queue:Queue =>
              result.consumers.add(link(queue))
            case _ =>
              consumer.connection.foreach{c=>
                result.consumers.add(link(c))
              }
          }
        }
        node.broadcast_producers.flatMap( _.producer.connection ).foreach { connection=>
          result.producers.add(link(connection))
        }

        result
      })
    }
  }

  @GET @Path("virtual-hosts/{id}/queues/{queue}")
  def queue(@PathParam("id") id : Long, @PathParam("queue") qid : Long, @QueryParam("entries") entries:Boolean):QueueStatusDTO = {
    with_virtual_host(id) { case (virtualHost,cb) =>
      reset {
        val queue = virtualHost.router.get_queue(qid)
        status(queue, entries, cb)
      }
    }
  }

  @GET @Path("virtual-hosts/{id}/destinations/{dest}/queues/{queue}")
  def destination_queue(@PathParam("id") id : Long, @PathParam("dest") dest : Long, @PathParam("queue") qid : Long, @QueryParam("entries") entries:Boolean ):QueueStatusDTO = {
    with_virtual_host(id) { case (virtualHost,cb) =>
      import JavaConversions._
      val queue = virtualHost.router.routing_nodes.find { _.id == dest } flatMap { node=> node.queues.find  { _.id == qid } }
      status(queue, entries, cb)
    }
  }

  def status(qo:Option[Queue], entries:Boolean=false, cb:Option[QueueStatusDTO]=>Unit):Unit = if(qo==None) {
    cb(None)
  } else {
    val q = qo.get
    q.dispatch_queue {
      val rc = new QueueStatusDTO
      rc.id = q.id
      rc.binding = q.binding.binding_dto
      rc.capacity_used = q.capacity_used
      rc.capacity = q.capacity
      rc.config = q.config

      rc.enqueue_item_counter = q.enqueue_item_counter
      rc.dequeue_item_counter = q.dequeue_item_counter
      rc.enqueue_size_counter = q.enqueue_size_counter
      rc.dequeue_size_counter = q.dequeue_size_counter
      rc.nack_item_counter = q.nack_item_counter
      rc.nack_size_counter = q.nack_size_counter

      rc.queue_size = q.queue_size
      rc.queue_items = q.queue_items

      rc.loading_size = q.loading_size
      rc.flushing_size = q.flushing_size
      rc.flushed_items = q.flushed_items

      if( entries ) {
        var cur = q.head_entry
        while( cur!=null ) {

          val e = new EntryStatusDTO
          e.seq = cur.seq
          e.count = cur.count
          e.size = cur.size
          e.consumer_count = cur.parked.size
          e.is_prefetched = cur.is_prefetched
          e.state = cur.label

          rc.entries.add(e)

          cur = if( cur == q.tail_entry ) {
            null
          } else {
            cur.nextOrTail
          }
        }
      } else {
//        rc.entries = null
      }

      q.inbound_sessions.flatMap( _.producer.connection ).foreach { connection=>
        rc.producers.add(link(connection))
      }
      q.all_subscriptions.valuesIterator.toSeq.foreach{ sub =>
        val status = new QueueConsumerStatusDTO
        sub.consumer.connection.foreach(x=> status.link = link(x))
        status.total_dispatched_count = sub.total_dispatched_count
        status.total_dispatched_size = sub.total_dispatched_size
        status.total_ack_count = sub.total_ack_count
        status.total_nack_count = sub.total_nack_count
        status.acquired_size = sub.acquired_size
        status.acquired_count = sub.acquired_count
        status.waiting_on = if( sub.full ) {
          "ack"
        } else if( sub.pos.is_tail ) {
          "producer"
        } else if( !sub.pos.is_loaded ) {
          "load"
        } else {
          "dispatch"
        }
        rc.consumers.add(status)
      }
      cb(Some(rc))
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
          result.state = connector.service_state.toString
          result.state_since = connector.service_state.since
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

  def with_connection[T](id:Long)(func: BrokerConnection=>T):T = {
    with_broker { case (broker, cb) =>
      broker.connectors.flatMap{ _.connections.get(id) }.headOption match {
        case None => cb(None)
        case Some(connection:BrokerConnection) =>
          connection.dispatch_queue {
            cb(Some(func(connection)))
          }
      }
    }
  }

  @GET @Path("connections/{id}")
  def connections(@PathParam("id") id : Long):ConnectionStatusDTO = {
    with_connection(id){ connection=>
      connection.get_connection_status
    }
  }

  @POST @Path("connections/{id}/action/shutdown")
  @Produces(Array("application/json", "application/xml","text/xml"))
  def post_connection_shutdown(@PathParam("id") id : Long):Unit = {
    with_connection(id){ connection=>
      connection.stop
    }
  }


  @POST @Path("connections/{id}/action/shutdown")
  @Produces(Array("text/html;qs=5"))
  def post_connection_shutdown_and_redirect(@PathParam("id") id : Long):Unit = {
    post_connection_shutdown(id)
    result(strip_resolve("../../.."))
  }

}
