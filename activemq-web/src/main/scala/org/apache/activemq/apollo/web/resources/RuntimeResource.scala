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
import org.fusesource.hawtdispatch.{ScalaDispatch, Future}
import ScalaDispatch._
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
      result.currentTime = System.currentTimeMillis
      result.state = broker.serviceState.toString
      result.stateSince - broker.serviceState.since
      result.config = broker.config

      broker.virtualHosts.values.foreach{ host=>
        result.virtualHosts.add( host.id )
      }

      broker.connectors.foreach{ c=>
        result.connectors.add(c.id)
      }

      broker.connectors.foreach{ connector=>
        connector.connections.keysIterator.foreach { id =>
          result.connections.add(id)
        }
      }

      cb(Some(result))
    }
  }


  @GET @Path("virtual-hosts")
  def virtualHosts :Array[jl.Long] = {
    val list: List[jl.Long] = get.virtualHosts
    list.toArray(new Array[jl.Long](list.size))
  }

  @GET @Path("virtual-hosts/{id}")
  def virtualHost(@PathParam("id") id : Long):VirtualHostStatusDTO = {
    with_virtual_host(id) { case (virtualHost,cb) =>
      val result = new VirtualHostStatusDTO
      result.id = virtualHost.id
      result.state = virtualHost.serviceState.toString
      result.stateSince = virtualHost.serviceState.since
      result.config = virtualHost.config


      if( virtualHost.store != null ) {
        result.storeType = virtualHost.store.storeType
      }
      virtualHost.router.destinations.valuesIterator.foreach { node=>
        val summary = new DestinationSummaryDTO
        summary.id = node.id
        summary.name = node.destination.getName.toString
        summary.domain = node.destination.getDomain.toString
        result.destinations.add(summary)
      }
      cb(Some(result))
    }
  }

  @GET @Path("virtual-hosts/{id}/destinations/{dest}")
  def destination(@PathParam("id") id : Long, @PathParam("dest") dest : Long):DestinationStatusDTO = {
    with_virtual_host(id) { case (virtualHost,cb) =>
      cb(virtualHost.router.destinations.valuesIterator.find { _.id == dest } map { node=>
        val result = new DestinationStatusDTO
        result.id = node.id
        result.name = node.destination.getName.toString
        result.domain = node.destination.getDomain.toString

        node match {
          case qdn:virtualHost.router.QueueDestinationNode =>
            result.queues.add(qdn.queue.id)
          case _ =>
        }
        result
      })
    }
  }

  @GET @Path("virtual-hosts/{id}/queues/{queue}")
  def queue(@PathParam("id") id : Long, @PathParam("queue") qid : Long, @QueryParam("entries") entries:Boolean ):QueueStatusDTO = {
    with_virtual_host(id) { case (virtualHost,cb) =>
      import JavaConversions._
      virtualHost.queues.valuesIterator.find { _.id == qid } match {
        case Some(q:Queue)=>
          q.dispatchQueue {

            val result = new QueueStatusDTO
            result.id = q.id
            result.capacityUsed = q.capacity_used
            result.capacity = q.capacity

            result.enqueueItemCounter = q.enqueue_item_counter
            result.dequeueItemCounter = q.dequeue_item_counter
            result.enqueueSizeCounter = q.enqueue_size_counter
            result.dequeueSizeCounter = q.dequeue_size_counter
            result.nackItemCounter = q.nack_item_counter
            result.nackSizeCounter = q.nack_size_counter

            result.queueSize = q.queue_size
            result.queueItems = q.queue_items

            result.loadingSize = q.loading_size
            result.flushingSize = q.flushing_size
            result.flushedItems = q.flushed_items

            if( entries ) {
              var cur = q.head_entry
              while( cur!=null ) {

                val e = new EntryStatusDTO
                e.seq = cur.seq
                e.count = cur.count
                e.size = cur.size
                e.consumerCount = cur.parked.size
                e.prefetchCount = cur.prefetched
                e.state = cur.label

                result.entries.add(e)

                cur = if( cur == q.tail_entry ) {
                  null
                } else {
                  cur.nextOrTail
                }
              }
            }

            cb(Some(result))
          }
        case None=>
          cb(None)
      }
    }
  }


  @GET @Path("connectors")
  def connectors :Array[jl.Long] = {
    val list: List[jl.Long] = get.connectors
    list.toArray(new Array[jl.Long](list.size))
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
          result.stateSince = connector.serviceState.since
          result.config = connector.config

          result.accepted = connector.accept_counter.get
          connector.connections.keysIterator.foreach { id=>
            result.connections.add(id)
          }
          cb(Some(result))
      }
    }
  }


  @GET @Path("connections")
  def connections :Array[Long] = {
    val rc:Array[Long] = with_broker { case (broker, cb) =>
      val rc = ListBuffer[Long]()
      broker.connectors.foreach { connector=>
        connector.connections.keys.foreach { id =>
          rc += id.longValue
        }
      }
      cb(Some(rc.toArray))
    }
    rc.sorted
  }

  @GET @Path("connections/{id}")
  def connections(@PathParam("id") id : Long):ConnectionStatusDTO = {
    with_broker { case (broker, cb) =>
      broker.connectors.flatMap{ _.connections.get(id) }.headOption match {
        case None => cb(None)
        case Some(connection:BrokerConnection) =>
          connection.dispatchQueue {
            val result = new ConnectionStatusDTO
            result.id = connection.id
            result.state = connection.serviceState.toString
            result.stateSince = connection.serviceState.since
            result.protocol = connection.protocol
            result.transport = connection.transport.getTypeId
            result.remoteAddress = connection.transport.getRemoteAddress
            val wf = connection.transport.getWireformat
            if( wf!=null ) {
              result.writeCounter = wf.getWriteCounter
              result.readCounter = wf.getReadCounter
            }
            cb(Some(result))
          }
      }
    }
  }

}
