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
import org.apache.activemq.apollo.dto._
import java.{lang => jl}
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.broker._
import scala.collection.Iterable
import org.apache.activemq.apollo.util.{Failure, Success, Dispatched, Result}
import scala.Some

/**
 * <p>
 * The RuntimeResource resource manages access to the runtime state of a broker.  It is used
 * to see the status of the broker and to apply management operations against the broker.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@Path("/")
@Produces(Array("application/json", "application/xml","text/xml", "text/html;qs=5"))
case class BrokerResource() extends Resource() {

  type FutureResult[T] = Future[Result[T, Throwable]]

  def FutureResult[T]() = Future[Result[T, Throwable]]()

  private def sync[T](dispached:Dispatched)(func: =>FutureResult[T]):FutureResult[T] = {
    val rc = Future[Result[T, Throwable]]()
    dispached.dispatch_queue.apply {
      try {
        func.onComplete(x=> rc.apply(x))
      } catch {
        case e:Throwable => rc.apply(Failure(e))
      }
    }
    rc
  }


  def sync_all[T,D<:Dispatched](values:Iterable[D])(func: (D)=>FutureResult[T]) = {
    Future.all {
      values.map { value=>
        sync(value) {
          func(value)
        }
      }
    }
  }

  private implicit def to_local_router(host:VirtualHost):LocalRouter = {
    host.router.asInstanceOf[LocalRouter]
  }

  private implicit def wrap_future_result[T](value:T):FutureResult[T] = {
    val rc = FutureResult[T]()
    rc.apply(Success(value))
    rc
  }

  private implicit def unwrap_future_result[T](value:FutureResult[T]):T = {
    value.await() match {
      case Success(value) => value
      case Failure(value) => throw value
    }
  }

  private def with_broker[T](func: (org.apache.activemq.apollo.broker.Broker)=>FutureResult[T]):FutureResult[T] = {
    BrokerRegistry.list.headOption match {
      case Some(broker)=>
        sync(broker) {
          func(broker)
        }
      case None=>
        result(NOT_FOUND)
    }
  }

  private def with_virtual_host[T](id:String)(func: (VirtualHost)=>FutureResult[T]):FutureResult[T] = {
    with_broker { broker =>
      broker.virtual_hosts.valuesIterator.find( _.id == id) match {
        case Some(virtualHost)=>
          sync(virtualHost) {
            func(virtualHost)
          }
        case None=>
          result(NOT_FOUND)
      }
    }
  }

  private def with_connection[T](id:Long)(func: BrokerConnection=>FutureResult[T]):FutureResult[T] = {
    with_broker { broker =>
      broker.connectors.flatMap{ _.connections.get(id) }.headOption match {
        case Some(connection:BrokerConnection) =>
          sync(connection) {
            func(connection)
          }
        case None=>
          result(NOT_FOUND)
      }
    }
  }


  @Path("config")
  def config_resource:ConfigurationResource = {
    with_broker { broker =>
      ConfigurationResource(this, broker.config)
    }
  }

  @GET
  def get_broker():BrokerStatusDTO = {
    with_broker { broker =>
      val result = new BrokerStatusDTO

      result.id = broker.id
      result.current_time = System.currentTimeMillis
      result.state = broker.service_state.toString
      result.state_since = broker.service_state.since

      broker.virtual_hosts.values.foreach{ host=>
        // TODO: may need to sync /w virtual host's dispatch queue
        result.virtual_hosts.add( host.id )
      }

      broker.connectors.foreach{ c=>
        result.connectors.add( c.id )
      }

      broker.connectors.foreach{ connector=>
        connector.connections.foreach { case (id,connection) =>
          // TODO: may need to sync /w connection's dispatch queue
          result.connections.add( new LongIdLabeledDTO(id, connection.transport.getRemoteAddress ) )
        }
      }
      result
    }
  }

  @GET
  @Path("queue-metrics")
  def get_queue_metrics(): AggregateQueueMetricsDTO = {
    with_broker { broker =>
      get_queue_metrics(broker)
    }
  }

  @GET @Path("virtual-hosts")
  def virtualHosts = {
    val rc = new StringListDTO
    rc.items = get_broker.virtual_hosts
    rc
  }

  def aggregate_queue_metrics(queue_metrics:Iterable[QueueMetricsDTO]):AggregateQueueMetricsDTO = {
    queue_metrics.foldLeft(new AggregateQueueMetricsDTO){ (rc, q)=>
      rc.enqueue_item_counter += q.enqueue_item_counter
      rc.enqueue_size_counter += q.enqueue_size_counter
      rc.enqueue_ts = rc.enqueue_ts max q.enqueue_ts

      rc.dequeue_item_counter += q.dequeue_item_counter
      rc.dequeue_size_counter += q.dequeue_size_counter
      rc.dequeue_ts += rc.dequeue_ts max q.dequeue_ts

      rc.nack_item_counter += q.nack_item_counter
      rc.nack_size_counter += q.nack_size_counter
      rc.nack_ts = rc.nack_ts max q.nack_ts

      rc.queue_size += q.queue_size
      rc.queue_items += q.queue_items

      rc.swap_out_item_counter += q.swap_out_item_counter
      rc.swap_out_size_counter += q.swap_out_size_counter
      rc.swap_in_item_counter += q.swap_in_item_counter
      rc.swap_in_size_counter += q.swap_in_size_counter

      rc.swapping_in_size += q.swapping_in_size
      rc.swapping_out_size += q.swapping_out_size

      rc.swapped_in_items += q.swapped_in_items
      rc.swapped_in_size += q.swapped_in_size

      rc.swapped_in_size_max += q.swapped_in_size_max

      if( q.isInstanceOf[AggregateQueueMetricsDTO] ) {
        rc.queues += q.asInstanceOf[AggregateQueueMetricsDTO].queues
      } else {
        rc.queues += 1
      }
      rc
    }
  }

  def get_queue_metrics(broker:Broker):FutureResult[AggregateQueueMetricsDTO] = {
    val metrics = sync_all(broker.virtual_hosts.values) { host =>
      get_queue_metrics(host)
    }
    metrics.map( x=> Success(aggregate_queue_metrics(x.flatMap(_.success_option)) ))
  }

  def get_queue_metrics(host:VirtualHost):FutureResult[AggregateQueueMetricsDTO] = {
    val router:LocalRouter = host
    val queues: Iterable[Queue] = router.queues_by_id.values
    val metrics = sync_all(queues) { queue =>
      get_queue_metrics(queue)
    }
    metrics.map( x=> Success(aggregate_queue_metrics(x.flatMap(_.success_option))) )
  }


  @GET @Path("virtual-hosts/{id}")
  def virtual_host(@PathParam("id") id : String):VirtualHostStatusDTO = {
    with_virtual_host(id) { host =>
      val result = new VirtualHostStatusDTO
      result.id = host.id
      result.state = host.service_state.toString
      result.state_since = host.service_state.since
      result.store = host.store!=null

      val router:LocalRouter = host

      router.topic_domain.destinations.foreach { node=>
        result.topics.add(new LongIdLabeledDTO(node.id, node.name))
      }

      router.queue_domain.destinations.foreach { node=>
        result.queues.add(new LongIdLabeledDTO(node.id, node.binding.label))
      }

      result
    }
  }

  @GET @Path("virtual-hosts/{id}/queue-metrics")
  def virtual_host_queue_metrics(@PathParam("id") id : String): AggregateQueueMetricsDTO = {
    with_virtual_host(id) { virtualHost =>
      get_queue_metrics(virtualHost)
    }
  }

  @GET @Path("virtual-hosts/{id}/store")
  def store(@PathParam("id") id : String):StoreStatusDTO = {
    with_virtual_host(id) { virtualHost =>
      if(virtualHost.store!=null) {
        val rc = FutureResult[StoreStatusDTO]()
        virtualHost.store.get_store_status { status =>
          rc(Success(status))
        }
        rc
      } else {
        result(NOT_FOUND)
      }
    }
  }

  def link(connection:BrokerConnection) = {
    val link = new LinkDTO()
    link.kind = "connection"
    link.id = connection.id.toString
    link.label = connection.transport.getRemoteAddress
    link
  }

  def link(queue:Queue) = {
    val link = new LinkDTO()
    link.kind = "queue"
    link.id = queue.id.toString
    link.label = queue.binding.label
    link
  }

  @GET @Path("virtual-hosts/{id}/topics/{dest}")
  def destination(@PathParam("id") id : String, @PathParam("dest") dest : Long):TopicStatusDTO = {
    with_virtual_host(id) { host =>

      val router:LocalRouter = host
      val node = router.topic_domain.destination_by_id.get(dest).getOrElse(result(NOT_FOUND))
      val rc = new TopicStatusDTO
      rc.id = node.id
      rc.name = node.name
      rc.config = node.config

      node.durable_subscriptions.foreach { q=>
        rc.durable_subscriptions.add(new LongIdLabeledDTO(q.id, q.binding.label))
      }
      node.consumers.foreach { consumer=>
        consumer match {
          case queue:Queue =>
            rc.consumers.add(link(queue))
          case _ =>
            consumer.connection.foreach{c=>
              rc.consumers.add(link(c))
            }
        }
      }
      node.producers.flatMap( _.connection ).foreach { connection=>
        rc.producers.add(link(connection))
      }

      rc
    }
  }

  @GET @Path("virtual-hosts/{id}/all-queues/{queue}")
  def queue(@PathParam("id") id : String, @PathParam("queue") qid : Long, @QueryParam("entries") entries:Boolean):QueueStatusDTO = {
    with_virtual_host(id) { host =>
      val router:LocalRouter = host
      val queue = router.queues_by_id.get(qid)
      status(queue, entries)
    }
  }

  @GET @Path("virtual-hosts/{id}/queues/{queue}")
  def destination_queue(@PathParam("id") id : String, @PathParam("queue") qid : Long, @QueryParam("entries") entries:Boolean ):QueueStatusDTO = {
    with_virtual_host(id) { host =>
      val router:LocalRouter = host
      val queue = router.queue_domain.destination_by_id.get(qid)
      status(queue, entries)
    }
  }

  def get_queue_metrics(q:Queue):QueueMetricsDTO = {
    val rc = new QueueMetricsDTO

    rc.enqueue_item_counter = q.enqueue_item_counter
    rc.enqueue_size_counter = q.enqueue_size_counter
    rc.enqueue_ts = q.enqueue_ts

    rc.dequeue_item_counter = q.dequeue_item_counter
    rc.dequeue_size_counter = q.dequeue_size_counter
    rc.dequeue_ts = q.dequeue_ts

    rc.nack_item_counter = q.nack_item_counter
    rc.nack_size_counter = q.nack_size_counter
    rc.nack_ts = q.nack_ts

    rc.queue_size = q.queue_size
    rc.queue_items = q.queue_items

    rc.swap_out_item_counter = q.swap_out_item_counter
    rc.swap_out_size_counter = q.swap_out_size_counter
    rc.swap_in_item_counter = q.swap_in_item_counter
    rc.swap_in_size_counter = q.swap_in_size_counter

    rc.swapping_in_size = q.swapping_in_size
    rc.swapping_out_size = q.swapping_out_size

    rc.swapped_in_items = q.swapped_in_items
    rc.swapped_in_size = q.swapped_in_size

    rc.swapped_in_size_max = q.swapped_in_size_max

    rc
  }

  def status(qo:Option[Queue], entries:Boolean=false) = qo match {
    case None=> result(NOT_FOUND)
    case Some(q)=> sync(q) {

      val rc = new QueueStatusDTO
      rc.id = q.id
      rc.destination = q.binding.binding_dto
      rc.config = q.config
      rc.metrics = get_queue_metrics(q)

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
        status.position = sub.pos.seq
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
      rc
    }
  }


  @GET @Path("connectors")
  def connectors = {
    val rc = new StringListDTO
    rc.items = get_broker.connectors
    rc
  }

  @GET @Path("connectors/{id}")
  def connector(@PathParam("id") id : String):ConnectorStatusDTO = {
    with_broker { broker =>
      broker.connectors.find(_.id == id) match {
        case None=> result(NOT_FOUND)
        case Some(connector)=>

          val result = new ConnectorStatusDTO
          result.id = connector.id.toString
          result.state = connector.service_state.toString
          result.state_since = connector.service_state.since

          result.accepted = connector.connection_counter.get
          connector.connections.foreach { case (id,connection) =>
            // TODO: may need to sync /w connection's dispatch queue
            result.connections.add( new LongIdLabeledDTO(id, connection.transport.getRemoteAddress ) )
          }

          result
      }
    }
  }


  @GET @Path("connections")
  def connections:LongIdListDTO = {
    with_broker { broker =>
      val rc = new LongIdListDTO

      broker.connectors.foreach { connector=>
        connector.connections.foreach { case (id,connection) =>
          // TODO: may need to sync /w connection's dispatch queue
          rc.items.add(new LongIdLabeledDTO(id, connection.transport.getRemoteAddress ))
        }
      }
      
      rc
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

  @POST
  @Path("action/shutdown")
  def command_shutdown:Unit = {
    info("JVM shutdown requested via web interface")

    // do the the exit async so that we don't
    // kill the current request.
    Broker.BLOCKABLE_THREAD_POOL.apply {
      Thread.sleep(200)
      System.exit(0)
    }
  }

}
