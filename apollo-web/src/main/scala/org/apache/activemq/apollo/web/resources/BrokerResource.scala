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

import org.apache.activemq.apollo.dto._
import java.{lang => jl}
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.broker._
import scala.collection.Iterable
import scala.Some
import security.{SecurityContext, Authorizer}
import org.apache.activemq.apollo.util.path.PathParser
import org.apache.activemq.apollo.web.resources.Resource._
import org.josql.Query
import org.apache.activemq.apollo.util._
import collection.mutable.ListBuffer
import javax.ws.rs._
import core.Response
import Response.Status._
import org.josql.expressions.SelectItemExpression
import org.apache.activemq.apollo.util.BaseService._
import management.ManagementFactory
import javax.management.ObjectName
import javax.management.openmbean.CompositeData

/**
 * <p>
 * The RuntimeResource resource manages access to the runtime state of a broker.  It is used
 * to see the status of the broker and to apply management operations against the broker.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@Produces(Array("application/json", "application/xml","text/xml", "text/html;qs=5"))
case class BrokerResource() extends Resource {

  @Path("config")
  def config_resource:ConfigurationResource = {
    with_broker { broker =>
      configing(broker) {
        ConfigurationResource(this, broker.config)
      }
    }
  }

  @GET
  def get_broker():BrokerStatusDTO = {
    with_broker { broker =>
      monitoring(broker) {
        val result = new BrokerStatusDTO

        result.id = broker.id
        result.jvm_metrics = create_jvm_metrics
        result.current_time = System.currentTimeMillis
        result.state = broker.service_state.toString
        result.state_since = broker.service_state.since
        result.version = Broker.version

        broker.virtual_hosts.values.foreach{ host=>
          // TODO: may need to sync /w virtual host's dispatch queue
          result.virtual_hosts.add( host.id )
        }

        broker.connectors.values.foreach{ c=>
          result.connectors.add( c.id )
        }

        broker.connections.foreach { case (id,connection) =>
          // TODO: may need to sync /w connection's dispatch queue
          result.connections.add( new LongIdLabeledDTO(id, connection.transport.getRemoteAddress.toString ) )
        }
        result

      }
    }
  }


  private def create_jvm_metrics = {
    val rc = new JvmMetricsDTO
    rc.jvm_name = Broker.jvm

    implicit def to_object_name(value:String):ObjectName = new ObjectName(value)
    implicit def to_long(value:AnyRef):Long = value.asInstanceOf[java.lang.Long].longValue()
    implicit def to_int(value:AnyRef):Int = value.asInstanceOf[java.lang.Integer].intValue()
    implicit def to_double(value:AnyRef):Double = value.asInstanceOf[java.lang.Double].doubleValue()

    def attempt(func: => Unit) = {
      try {
        func
      } catch {
        case _ => // ignore
      }
    }

    val mbean_server = ManagementFactory.getPlatformMBeanServer()

    attempt( rc.uptime = mbean_server.getAttribute("java.lang:type=Runtime", "Uptime") )
    attempt( rc.start_time = mbean_server.getAttribute("java.lang:type=Runtime", "StartTime") )
    attempt( rc.runtime_name = mbean_server.getAttribute("java.lang:type=Runtime", "Name").toString )

    rc.os_name = Broker.os
    attempt( rc.os_arch = mbean_server.getAttribute("java.lang:type=OperatingSystem", "Arch").toString )

    attempt( rc.os_fd_open = mbean_server.getAttribute("java.lang:type=OperatingSystem", "OpenFileDescriptorCount"))
    rc.os_fd_max = Broker.max_fd_limit.getOrElse(0)

    attempt( rc.os_memory_total = mbean_server.getAttribute("java.lang:type=OperatingSystem", "TotalPhysicalMemorySize") )
    attempt( rc.os_memory_free = mbean_server.getAttribute("java.lang:type=OperatingSystem", "FreePhysicalMemorySize") )

    attempt( rc.os_swap_free = mbean_server.getAttribute("java.lang:type=OperatingSystem", "FreeSwapSpaceSize") )
    attempt( rc.os_swap_free = mbean_server.getAttribute("java.lang:type=OperatingSystem", "TotalSwapSpaceSize") )

    attempt( rc.os_load_average = mbean_server.getAttribute("java.lang:type=OperatingSystem", "SystemLoadAverage") )
    attempt( rc.os_cpu_time = mbean_server.getAttribute("java.lang:type=OperatingSystem", "ProcessCpuTime") )
    attempt( rc.os_processors = mbean_server.getAttribute("java.lang:type=OperatingSystem", "AvailableProcessors") )

    attempt( rc.classes_loaded = mbean_server.getAttribute("java.lang:type=ClassLoading", "LoadedClassCount") )
    attempt( rc.classes_unloaded = mbean_server.getAttribute("java.lang:type=ClassLoading", "UnloadedClassCount") )

    attempt( rc.threads_peak = mbean_server.getAttribute("java.lang:type=Threading", "PeakThreadCount") )
    attempt( rc.threads_current = mbean_server.getAttribute("java.lang:type=Threading", "ThreadCount") )

    def memory_metrics(data:CompositeData) = {
      val rc = new MemoryMetricsDTO
      rc.alloc =  data.get("committed").asInstanceOf[java.lang.Long].longValue()
      rc.used =  data.get("used").asInstanceOf[java.lang.Long].longValue()
      rc.max =  data.get("max").asInstanceOf[java.lang.Long].longValue()
      rc
    }

    attempt( rc.heap_memory = memory_metrics(mbean_server.getAttribute("java.lang:type=Memory", "HeapMemoryUsage").asInstanceOf[CompositeData]) )
    attempt( rc.non_heap_memory = memory_metrics(mbean_server.getAttribute("java.lang:type=Memory", "NonHeapMemoryUsage").asInstanceOf[CompositeData]) )

    rc
  }

  @GET
  @Path("queue-metrics")
  def get_queue_metrics(): AggregateQueueMetricsDTO = {
    with_broker { broker =>
      monitoring(broker) {
        get_queue_metrics(broker)
      }
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
      monitoring(host) {
        val result = new VirtualHostStatusDTO
        result.id = host.id
        result.state = host.service_state.toString
        result.state_since = host.service_state.since
        result.store = host.store!=null

        val router:LocalRouter = host

        router.queue_domain.destinations.foreach { node=>
          result.queues.add(node.id)
        }
        router.topic_domain.destinations.foreach { node=>
          result.topics.add(node.id)
        }
        router.topic_domain.durable_subscriptions_by_id.keys.foreach { id=>
          result.dsubs.add(id)
        }

        result
      }
    }
  }

  @GET @Path("virtual-hosts/{id}/queue-metrics")
  def virtual_host_queue_metrics(@PathParam("id") id : String): AggregateQueueMetricsDTO = {
    with_virtual_host(id) { host =>
      monitoring(host) {
        get_queue_metrics(host)
      }
    }
  }

  @GET @Path("virtual-hosts/{id}/store")
  def store(@PathParam("id") id : String):StoreStatusDTO = {
    with_virtual_host(id) { host =>
      monitoring(host) {
        if(host.store!=null) {
          val rc = FutureResult[StoreStatusDTO]()
          host.store.get_store_status { status =>
            rc(Success(status))
          }
          rc
        } else {
          result(NOT_FOUND)
        }
      }
    }
  }

  def link(connection:BrokerConnection) = {
    val link = new LinkDTO()
    link.kind = "connection"
    link.id = connection.id.toString
    link.label = connection.transport.getRemoteAddress.toString
    link
  }

  def link(queue:Queue) = {
    val link = new LinkDTO()
    link.kind = "queue"
    link.id = queue.id
    link.label = queue.id
    link
  }

  def narrow[T](kind:Class[T], x:Iterable[Result[T, Throwable]], f:java.util.List[String], q:String, p:java.lang.Integer, ps:java.lang.Integer) = {
    import collection.JavaConversions._
    try {
      var records = x.toSeq.flatMap(_.success_option)

      val page_size = if( ps !=null ) ps.intValue() else 100
      val page = if( p !=null ) p.intValue() else 0

      val query = new Query
      val fields = if (f.isEmpty) "*" else f.toList.mkString(",")
      val where_clause = if (q != null) q else "1=1"

      query.parse("SELECT " + fields + " FROM " + kind.getName + " WHERE "+ where_clause+" LIMIT "+((page_size*page)+1)+", "+page_size)
      val headers = if (f.isEmpty) seqAsJavaList(List("*")) else f

      val list = query.execute(records).getResults

      Success(seqAsJavaList( headers :: list.toList) )
    } catch {
      case e:Throwable => Failure(e)
    }
  }

  @GET @Path("virtual-hosts/{id}/topics")
  @Produces(Array("application/json"))
  def topics(@PathParam("id") id : String, @QueryParam("f") f:java.util.List[String],
            @QueryParam("q") q:String, @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer ):java.util.List[_] = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val records = Future.all {
        router.topic_domain.destination_by_id.values.map { value  =>
          status(value)
        }
      }
      val rc:FutureResult[java.util.List[_]] = records.map(narrow(classOf[TopicStatusDTO], _, f, q, p, ps))
      rc
    }
  }

  @GET @Path("virtual-hosts/{id}/topics/{name:.*}")
  def topic(@PathParam("id") id : String, @PathParam("name") name : String):TopicStatusDTO = {
    with_virtual_host(id) { host =>
      val router:LocalRouter = host
      val node = router.topic_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      status(node)
    }
  }

  @GET @Path("virtual-hosts/{id}/queues")
  @Produces(Array("application/json"))
  def queues(@PathParam("id") id : String, @QueryParam("f") f:java.util.List[String],
            @QueryParam("q") q:String, @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer ):java.util.List[_] = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val values: Iterable[Queue] = router.queue_domain.destination_by_id.values

      val records = sync_all(values) { value =>
        status(value, false)
      }

      val rc:FutureResult[java.util.List[_]] = records.map(narrow(classOf[QueueStatusDTO], _, f, q, p, ps))
      rc
    }
  }

  @GET @Path("virtual-hosts/{id}/queues/{name:.*}")
  def queue(@PathParam("id") id : String, @PathParam("name") name : String, @QueryParam("entries") entries:Boolean ):QueueStatusDTO = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val node = router.queue_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      sync(node) {
        status(node, entries)
      }
    }
  }

  @DELETE @Path("virtual-hosts/{id}/queues/{name:.*}")
  @Produces(Array("application/json", "application/xml","text/xml"))
  def queue_delete(@PathParam("id") id : String, @PathParam("name") name : String):Unit = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val node = router.queue_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      admining(node) {
        router._destroy_queue(node)
      }
    }
  }

  @POST @Path("virtual-hosts/{id}/queues/{name:.*}/action/delete")
  @Produces(Array("text/html;qs=5"))
  def post_queue_delete_and_redirect(@PathParam("id") id : String, @PathParam("name") name : String):Unit = {
    queue_delete(id, name)
    result(strip_resolve("../../.."))
  }

  @GET @Path("virtual-hosts/{id}/dsubs")
  @Produces(Array("application/json"))
  def durable_subscriptions(@PathParam("id") id : String, @QueryParam("f") f:java.util.List[String],
            @QueryParam("q") q:String, @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer ):java.util.List[_] = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val values: Iterable[Queue] = router.topic_domain.durable_subscriptions_by_id.values

      val records = sync_all(values) { value =>
        status(value, false)
      }

      val rc:FutureResult[java.util.List[_]] = records.map(narrow(classOf[QueueStatusDTO], _, f, q, p, ps))
      rc
    }
  }
  @GET @Path("virtual-hosts/{id}/dsubs/{name:.*}")
  def durable_subscription(@PathParam("id") id : String, @PathParam("name") name : String, @QueryParam("entries") entries:Boolean):QueueStatusDTO = {
    with_virtual_host(id) { host =>
      val router:LocalRouter = host
      val node = router.topic_domain.durable_subscriptions_by_id.get(name).getOrElse(result(NOT_FOUND))
      sync(node) {
        status(node, entries)
      }
    }
  }


  @DELETE @Path("virtual-hosts/{id}/dsubs/{name:.*}")
  @Produces(Array("application/json", "application/xml","text/xml"))
  def dsub_delete(@PathParam("id") id : String, @PathParam("name") name : String):Unit = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val node = router.topic_domain.durable_subscriptions_by_id.get(name).getOrElse(result(NOT_FOUND))
      admining(node) {
        router._destroy_queue(node)
      }
    }
  }

  @POST @Path("virtual-hosts/{id}/dsubs/{name:.*}/action/delete")
  @Produces(Array("text/html;qs=5"))
  def post_dsub_delete_and_redirect(@PathParam("id") id : String, @PathParam("name") name : String):Unit = {
    dsub_delete(id, name)
    result(strip_resolve("../../.."))
  }


  private def decode_path(name:String) = {
    try {
      LocalRouter.destination_parser.decode_path(name)
    } catch {
      case x:PathParser.PathException => result(NOT_FOUND)
    }
  }

  def status(node: Topic): FutureResult[TopicStatusDTO] = {
    monitoring(node) {
      val rc = new TopicStatusDTO
      rc.id = node.id
      rc.state = "STARTED"
      rc.state_since = node.created_at
      rc.config = node.config

      node.durable_subscriptions.foreach {
        q =>
          rc.dsubs.add(q.id)
      }
      node.consumers.foreach {
        consumer =>
          consumer match {
            case queue: Queue =>
              rc.consumers.add(link(queue))
            case _ =>
              consumer.connection.foreach {
                c =>
                  rc.consumers.add(link(c))
              }
          }
      }
      node.producers.flatMap(_.connection).foreach {
        connection =>
          rc.producers.add(link(connection))
      }

      rc
    }
  }

  def status(q:Queue, entries:Boolean=false) = monitoring(q) {
    val rc = new QueueStatusDTO
    rc.id = q.id
    rc.state = q.service_state.toString
    rc.state_since = q.service_state.since
    rc.binding = q.binding.binding_dto
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


  @GET @Path("connectors")
  @Produces(Array("application/json"))
  def connectors(@QueryParam("f") f:java.util.List[String], @QueryParam("q") q:String,
                  @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer ):java.util.List[_] = {

    with_broker { broker =>
      monitoring(broker) {
        val records = broker.connectors.values.map { value =>
          Success(status(value))
        }
        FutureResult(narrow(classOf[ConnectorStatusDTO], records, f, q, p, ps))
      }
    }
  }

  @GET @Path("connectors/{id}")
  def connector(@PathParam("id") id : String):ConnectorStatusDTO = {
    with_connector(id) { connector =>
      monitoring(connector.broker) {
        status(connector)
      }
    }
  }

  def status(connector: Connector): ConnectorStatusDTO = {
    val result = new ConnectorStatusDTO
    result.id = connector.id.toString
    result.state = connector.service_state.toString
    result.state_since = connector.service_state.since
    result.accepted = connector.accepted.get
    result.connected = connector.connected.get
    result
  }

  @POST @Path("connectors/{id}/action/stop")
  @Produces(Array("application/json", "application/xml","text/xml"))
  def post_connector_stop(@PathParam("id") id : String):Unit = {
    with_connector(id) { connector =>
      admining(connector.broker) {
        connector.stop
      }
    }
  }

  @POST @Path("connectors/{id}/action/stop")
  @Produces(Array("text/html;qs=5"))
  def post_connector_stop_and_redirect(@PathParam("id") id : String):Unit = {
    post_connector_stop(id)
    result(strip_resolve(".."))
  }

  @POST @Path("connectors/{id}/action/start")
  @Produces(Array("application/json", "application/xml","text/xml"))
  def post_connector_start(@PathParam("id") id : String):Unit = {
    with_connector(id) { connector =>
      admining(connector.broker) {
        connector.start
      }
    }
  }

  @POST @Path("connectors/{id}/action/start")
  @Produces(Array("text/html;qs=5"))
  def post_connector_start_and_redirect(@PathParam("id") id : String):Unit = {
    post_connector_start(id)
    result(strip_resolve(".."))
  }

  @GET @Path("connections")
  @Produces(Array("application/json"))
  def connections(@QueryParam("f") f:java.util.List[String], @QueryParam("q") q:String,
                  @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer ):java.util.List[_] = {

    with_broker { broker =>
      monitoring(broker) {

        val records = sync_all(broker.connections.values) { value =>
          value.get_connection_status
        }

        val rc:FutureResult[java.util.List[_]] = records.map(narrow(classOf[ConnectionStatusDTO], _, f, q, p, ps))
        rc
      }
    }
  }

  @GET @Path("connections/{id}")
  def connection(@PathParam("id") id : Long):ConnectionStatusDTO = {
    with_connection(id){ connection=>
      monitoring(connection.connector.broker) {
        connection.get_connection_status
      }
    }
  }

  @DELETE @Path("connections/{id}")
  @Produces(Array("application/json", "application/xml","text/xml"))
  def connection_delete(@PathParam("id") id : Long):Unit = {
    with_connection(id){ connection=>
      admining(connection.connector.broker) {
        connection.stop
      }
    }
  }


  @POST @Path("connections/{id}/action/delete")
  @Produces(Array("text/html;qs=5"))
  def post_connection_delete_and_redirect(@PathParam("id") id : Long):Unit = {
    connection_delete(id)
    result(strip_resolve("../../.."))
  }

  @POST
  @Path("action/shutdown")
  def command_shutdown:Unit = {
    info("JVM shutdown requested via web interface")
    with_broker { broker =>
      admining(broker) {
        // do the the exit async so that we don't
        // kill the current request.
        Broker.BLOCKABLE_THREAD_POOL.apply {
          Thread.sleep(200)
          System.exit(0)
        }
      }
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



}
