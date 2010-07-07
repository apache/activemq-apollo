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
import org.apache.activemq.apollo.BrokerRegistry
import Response.Status._
import java.util.List
import org.apache.activemq.apollo.dto._
import java.{lang => jl}
import collection.JavaConversions
import org.fusesource.hawtdispatch.{ScalaDispatch, Future}
import ScalaDispatch._
import org.apache.activemq.apollo.broker._

/**
 * <p>
 * The BrokerStatus is the root container of the runtime status of a broker.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class BrokerStatus(parent:Broker) extends Resource {

  val broker:org.apache.activemq.apollo.broker.Broker = BrokerRegistry.get(parent.id)
  if( broker == null ) {
    println("not in regisitry: "+BrokerRegistry.brokers)
    result(NOT_FOUND)
  }

  @GET
  def get() = {
    println("get hit")
    Future[BrokerStatusDTO] { cb=>
      broker.dispatchQueue {
        println("building result...")
        val result = new BrokerStatusDTO

        result.id = broker.id
        result.currentTime = System.currentTimeMillis
        result.state = broker.serviceState.toString
        result.stateSince - broker.serviceState.since
        result.config = broker.config

        broker.connectors.foreach{ c=>
          result.connectors.add(c.id)
        }

        broker.virtualHosts.values.foreach{ host=>
          result.virtualHosts.add( host.id )
        }


        cb(result)
      }
    }
  }


  @Path("virtual-hosts")
  def virtualHosts :Array[jl.Long] = {
    val list: List[jl.Long] = get.virtualHosts
    list.toArray(new Array[jl.Long](list.size))
  }

  private def with_virtual_host[T](id:Long)(func: (VirtualHost, Option[T]=>Unit)=>Unit):T = {
    Future[Option[T]] { cb=>
      broker.virtualHosts.valuesIterator.find( _.id == id) match {
        case Some(virtualHost)=>
          virtualHost.dispatchQueue {
            func(virtualHost, cb)
          }
        case None=> cb(None)
      }
    }.getOrElse(result(NOT_FOUND))
  }

  @Path("virtual-hosts/{id}")
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

  @Path("virtual-hosts/{id}/destinations/{dest}")
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

  @Path("virtual-hosts/{id}/queues/{queue}")
  def queue(@PathParam("id") id : Long, @PathParam("queue") qid : Long):QueueStatusDTO = {
    with_virtual_host(id) { case (virtualHost,cb) =>
      import JavaConversions._
      virtualHost.queues.valuesIterator.find { _.id == qid } match {
        case Some(q:Queue)=>
          q.dispatchQueue {

            val result = new QueueStatusDTO
            result.id = q.id
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


            var cur = q.head_entry
            while( cur!=null ) {

              val e = new EntryStatusDTO
              e.seq = cur.seq
              e.count = cur.count
              e.size = cur.size
              e.consumers = cur.parked.size
              e.prefetched = cur.prefetched
              e.state = cur.label

              result.entries.add(e)

              cur = if( cur == q.tail_entry ) {
                null
              } else {
                cur.nextOrTail
              }
            }

            cb(Some(result))
          }
        case None=>
          cb(None)
      }
    }
  }


  @Path("connectors")
  def connectors :Array[jl.Long] = {
    val list: List[jl.Long] = get.connectors
    list.toArray(new Array[jl.Long](list.size))
  }

  @Path("connectors/{id}")
  def connector(@PathParam("id") id : Long):ConnectorStatusDTO = {

    Future[Option[ConnectorStatusDTO]] { cb=>
      broker.connectors.find(_.id == id) match {
        case Some(connector)=>
          connector.dispatchQueue {
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
        case None=> cb(None)
      }
    }.getOrElse(result(NOT_FOUND))

  }

}
