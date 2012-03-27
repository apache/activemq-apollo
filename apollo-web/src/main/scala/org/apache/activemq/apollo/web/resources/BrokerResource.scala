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

import java.{lang => jl}
import org.fusesource.hawtdispatch._
import scala.collection.Iterable
import org.apache.activemq.apollo.util.path.PathParser
import org.apache.activemq.apollo.util._
import javax.ws.rs._
import core.Response.Status
import core.{Response, Context}
import javax.ws.rs.core.Response.Status._
import management.ManagementFactory
import javax.management.ObjectName
import javax.management.openmbean.CompositeData
import org.josql.{QueryResults, Query}
import java.util.regex.Pattern
import javax.servlet.http.HttpServletResponse
import java.util.{Collections, ArrayList}
import org.apache.activemq.apollo.broker._
import java.security.Principal
import org.apache.activemq.apollo.dto._
import javax.ws.rs.core.MediaType._
import security.SecurityContext

/**
 * <p>
 * The RuntimeResource resource manages access to the runtime state of a broker.  It is used
 * to see the status of the broker and to apply management operations against the broker.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@Produces(Array(APPLICATION_JSON, APPLICATION_XML, TEXT_XML, "text/html;qs=5"))
case class BrokerResource() extends Resource {
  import Resource._

  @GET
  @Path("whoami")
  def whoami():java.util.List[PrincipalDTO] = {
    val rc: Set[Principal] = with_broker { broker =>
      val rc = FutureResult[Set[Principal]]()
      if(broker.authenticator!=null) {
        authenticate(broker.authenticator) { security_context =>
          if(security_context!=null) {
            rc.set(Success(security_context.principals))
          } else {
            rc.set(Success(Set[Principal]()))
          }
        }
      } else {
        rc.set(Success(Set[Principal]()))
      }
      rc
    }
    import collection.JavaConversions._
    new ArrayList[PrincipalDTO](rc.map(x=>new PrincipalDTO(x.getClass.getName, x.getName)))
  }

  @Produces(Array("text/html;qs=5"))
  @GET
  @Path("signin")
  def get_signin_html(@Context response:HttpServletResponse, @QueryParam("username") username:String, @QueryParam("password") password:String): ErrorDTO = {
    if(post_signin(response, username, password)) {
      result(strip_resolve("../.."))
    } else {
      var dto = new ErrorDTO()
      dto.code = "%d: %s".format(BAD_REQUEST.getStatusCode, BAD_REQUEST.getReasonPhrase)
      dto.message = "Invalid user id or password";
      result(BAD_REQUEST, dto)
    }
  }

//  @GET
//  @Path("signin")
//  def get_signin(@Context response:HttpServletResponse, @QueryParam("username") username:String, @QueryParam("password") password:String):Boolean = {
//    post_signin(response, username, password)
//  }

  @POST
  @Path("signin")
  def post_signin(@Context response:HttpServletResponse, @FormParam("username") username:String, @FormParam("password") password:String):Boolean =  {
    try {
      val user_info = UserInfo(username, password)
      http_request.setAttribute("user_info", user_info)
      unwrap_future_result[Boolean] {
        with_broker { broker =>
          monitoring(broker) {
            // Only create the session if he is a valid user.
            val session = http_request.getSession(true)
            user_info.security_context = http_request.getAttribute(SECURITY_CONTEXT_ATTRIBUTE).asInstanceOf[SecurityContext]
            session.setAttribute("user_info", user_info)
            true
          }
        }
      }
    } catch {
      case e:WebApplicationException => // this happens if user is not authorized
        e.printStackTrace()
        false
    }
  }

  @Produces(Array("text/html"))
  @GET @Path("signout")
  def signout_html():String = {
    signout()
    result(strip_resolve("../.."))
    ""
  }

  @Produces(Array(APPLICATION_JSON, APPLICATION_XML, TEXT_XML))
  @GET @Path("signout")
  def signout():String =  {
    val session = http_request.getSession(false)
    if( session !=null ) {
      session.invalidate();
    }
    ""
  }

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
        result.current_time = now
        result.state = broker.service_state.toString
        result.state_since = broker.service_state.since
        result.version = Broker.version
        result.connection_counter = broker.connection_id_counter.get()
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
  def get_queue_metrics(): AggregateDestMetricsDTO = {
    val rc:AggregateDestMetricsDTO = with_broker { broker =>
      monitoring(broker) {
        get_queue_metrics(broker)
      }
    }
    rc.current_time = now
    rc
  }

  @GET
  @Path("topic-metrics")
  def get_topic_metrics(): AggregateDestMetricsDTO = {
    val rc:AggregateDestMetricsDTO = with_broker { broker =>
      monitoring(broker) {
        get_topic_metrics(broker)
      }
    }
    rc.current_time = now
    rc
  }

  @GET
  @Path("dsub-metrics")
  def get_dsub_metrics(): AggregateDestMetricsDTO = {
    val rc:AggregateDestMetricsDTO = with_broker { broker =>
      monitoring(broker) {
        get_dsub_metrics(broker)
      }
    }
    rc.current_time = now
    rc
  }

  def aggregate(queue:AggregateDestMetricsDTO, topic:AggregateDestMetricsDTO, dsub:AggregateDestMetricsDTO):AggregateDestMetricsDTO = {
    // zero out the enqueue stats on the dsubs since they will already be accounted for in the topic
    // stats.
    dsub.enqueue_item_counter = 0
    dsub.enqueue_size_counter = 0
    dsub.enqueue_ts = 0
    val rc = aggregate_dest_metrics(List(queue, dsub))
    DestinationMetricsSupport.add_destination_metrics(rc, topic)
    rc.objects += topic.objects
    rc.current_time = now
    rc
  }

  @GET
  @Path("dest-metrics")
  def get_dest_metrics(): AggregateDestMetricsDTO = {
    aggregate(get_queue_metrics(), get_topic_metrics(), get_dsub_metrics())
  }

  def aggregate_dest_metrics(metrics:Iterable[DestMetricsDTO]):AggregateDestMetricsDTO = {
    metrics.foldLeft(new AggregateDestMetricsDTO){ (to, from)=>
      DestinationMetricsSupport.add_destination_metrics(to, from)
      if( from.isInstanceOf[AggregateDestMetricsDTO] ) {
        to.objects += from.asInstanceOf[AggregateDestMetricsDTO].objects
      } else {
        to.objects += 1
      }
      to
    }
  }

  def get_queue_metrics(broker:Broker):FutureResult[AggregateDestMetricsDTO] = {
    val metrics = sync_all(broker.virtual_hosts.values) { host =>
      get_queue_metrics(host)
    }
    metrics.map( x=> Success(aggregate_dest_metrics(x.flatMap(_.success_option)) ))
  }

  def get_queue_metrics(host:VirtualHost):FutureResult[AggregateDestMetricsDTO] = host.get_queue_metrics

  def get_topic_metrics(broker:Broker):FutureResult[AggregateDestMetricsDTO] = {
    val metrics = sync_all(broker.virtual_hosts.values) { host =>
      get_topic_metrics(host)
    }
    metrics.map( x=> Success(aggregate_dest_metrics(x.flatMap(_.success_option)) ))
  }

  def get_topic_metrics(host:VirtualHost):FutureResult[AggregateDestMetricsDTO] = host.get_topic_metrics

  def get_dsub_metrics(broker:Broker):FutureResult[AggregateDestMetricsDTO] = {
    val metrics = sync_all(broker.virtual_hosts.values) { host =>
      get_dsub_metrics(host)
    }
    metrics.map( x=> Success(aggregate_dest_metrics(x.flatMap(_.success_option)) ))
  }

  def get_dsub_metrics(host:VirtualHost):FutureResult[AggregateDestMetricsDTO] = host.get_dsub_metrics


  @GET @Path("virtual-hosts")
  @Produces(Array(APPLICATION_JSON))
  def virtual_host(@QueryParam("f") f:java.util.List[String], @QueryParam("q") q:String,
                  @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer, @QueryParam("o") o:java.util.List[String] ):DataPageDTO = {

    with_broker { broker =>
      monitoring(broker) {
        val records = broker.virtual_hosts.values.map { value =>
          Success(status(value))
        }
        FutureResult(narrow(classOf[VirtualHostStatusDTO], records, f, q, p, ps, o))
      }
    }
  }

  @GET @Path("virtual-hosts/{id}")
  def virtual_host(@PathParam("id") id : String):VirtualHostStatusDTO = {
    with_virtual_host(id) { host =>
      monitoring(host) {
        status(host)
      }
    }
  }

  def status(host: VirtualHost): VirtualHostStatusDTO = {
    val result = new VirtualHostStatusDTO
    result.id = host.id
    result.state = host.service_state.toString
    result.state_since = host.service_state.since
    result.store = host.store != null
    result.host_names = host.config.host_names

    val router: LocalRouter = host

    router.local_queue_domain.destinations.foreach { node =>
      result.queues.add(node.id)
    }
    router.local_topic_domain.destinations.foreach { node =>
      result.topics.add(node.id)
    }
    router.local_dsub_domain.destination_by_id.keys.foreach { id =>
      result.dsubs.add(id)
    }

    result
  }

  @GET @Path("virtual-hosts/{id}/queue-metrics")
  def virtual_host_queue_metrics(@PathParam("id") id : String): AggregateDestMetricsDTO = {
    val rc:AggregateDestMetricsDTO = with_virtual_host(id) { host =>
      monitoring(host) {
        get_queue_metrics(host)
      }
    }
    rc.current_time = now
    rc
  }

  @GET @Path("virtual-hosts/{id}/topic-metrics")
  def virtual_host_topic_metrics(@PathParam("id") id : String): AggregateDestMetricsDTO = {
    val rc:AggregateDestMetricsDTO = with_virtual_host(id) { host =>
      monitoring(host) {
        get_topic_metrics(host)
      }
    }
    rc.current_time = now
    rc
  }

  @GET @Path("virtual-hosts/{id}/dsub-metrics")
  def virtual_host_dsub_metrics(@PathParam("id") id : String): AggregateDestMetricsDTO = {
    val rc:AggregateDestMetricsDTO = with_virtual_host(id) { host =>
      monitoring(host) {
        get_dsub_metrics(host)
      }
    }
    rc.current_time = now
    rc
  }

  @GET @Path("virtual-hosts/{id}/dest-metrics")
  def virtual_host_dest_metrics(@PathParam("id") id : String): AggregateDestMetricsDTO = {
    aggregate(virtual_host_queue_metrics(id), virtual_host_topic_metrics(id), virtual_host_dsub_metrics(id))
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

  class JosqlHelper {

    def get(o:AnyRef, name:String):AnyRef = {

      def invoke(o:AnyRef, name:String):Option[AnyRef] = {
        try {
          if(name.endsWith("()")) {
            Option(o.getClass().getMethod(name.stripSuffix("()")).invoke(o))
          } else {
            Option(o.getClass().getField(name).get(o))
          }
        } catch {
          case e:Throwable =>
            None
        }
      }

      var parts = name.split(Pattern.quote("."))
      parts.foldLeft(Option(o)) { case(memo, field)=>
        memo.flatMap(invoke(_, field))
      }.getOrElse(null)
    }

    def NOT(o:AnyRef):AnyRef = not(o)
    def Not(o:AnyRef):AnyRef = not(o)
    def not(o:AnyRef):AnyRef = {
      o match {
        case java.lang.Boolean.TRUE => java.lang.Boolean.FALSE
        case java.lang.Boolean.FALSE => java.lang.Boolean.TRUE
        case null => java.lang.Boolean.TRUE
        case _ => java.lang.Boolean.FALSE
      }
    }
  }

  def narrow[T](kind:Class[T], x:Iterable[Result[T, Throwable]], f:java.util.List[String], q:String, p:java.lang.Integer, ps:java.lang.Integer, o:java.util.List[String]) = {
    import collection.JavaConversions._
    try {
      var records = x.toSeq.flatMap(_.success_option)

      val page_size = if( ps !=null ) ps.intValue() else 100
      val page = if( p !=null ) p.intValue() else 0

      val query = new Query
      query.addFunctionHandler(new JosqlHelper)
      val fields = if (f.isEmpty) "*" else f.toList.map("get(:_currobj, \""+_+"\")").mkString(",")
      val where_clause = if (q != null) q else "1=1"

      val orderby_clause = if (o.isEmpty) "" else " ORDER BY "+o.toList.mkString(",")

      query.parse("SELECT " + fields + " FROM " + kind.getName + " WHERE "+ where_clause+orderby_clause+" LIMIT "+((page_size*page)+1)+", "+page_size)
      val headers = if (f.isEmpty) seqAsJavaList(List("*")) else f

      val query_result: QueryResults = query.execute(records)
      val list = query_result.getResults

      val rc = new DataPageDTO
      rc.page = page
      rc.page_size = page_size



      def total_pages(x:Int,y:Int) = if(x==0) 1 else { x/y + (if ( x%y == 0 ) 0 else 1) }
      rc.total_pages = total_pages(query_result.getWhereResults.length, rc.page_size)
      rc.total_rows = query_result.getWhereResults.length
      rc.headers = headers
      rc.rows = list

      Success(rc)
    } catch {
      case e:Throwable =>
        Failure(create_result(BAD_REQUEST, e.getMessage))
    }
  }

  @GET @Path("virtual-hosts/{id}/topics")
  @Produces(Array(APPLICATION_JSON))
  def topics(@PathParam("id") id : String, @QueryParam("f") f:java.util.List[String],
            @QueryParam("q") q:String, @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer, @QueryParam("o") o:java.util.List[String] ):DataPageDTO = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val records = Future.all {
        router.local_topic_domain.destination_by_id.values.map { value  =>
          monitoring(value) {
            value.status
          }
        }
      }
      val rc:FutureResult[DataPageDTO] = records.map(narrow(classOf[TopicStatusDTO], _, f, q, p, ps, o))
      rc
    }
  }

  @GET @Path("virtual-hosts/{id}/topics/{name:.*}")
  def topic(@PathParam("id") id : String, @PathParam("name") name : String):TopicStatusDTO = {
    with_virtual_host(id) { host =>
      val router:LocalRouter = host
      val node = router.local_topic_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      monitoring(node) {
        node.status
      }
    }
  }

  @GET @Path("virtual-hosts/{id}/topic-queues/{name:.*}/{qid}")
  def topic(@PathParam("id") id : String,@PathParam("name") name : String,  @PathParam("qid") qid : Long, @QueryParam("entries") entries:Boolean):QueueStatusDTO = {
    with_virtual_host(id) { host =>
      val router:LocalRouter = host
      val node = router.local_topic_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      val queue =router.queues_by_store_id.get(qid).getOrElse(result(NOT_FOUND))
      monitoring(node) {
        sync(queue) {
          queue.status(entries)
        }
      }
    }
  }

  @GET @Path("virtual-hosts/{id}/queues")
  @Produces(Array(APPLICATION_JSON))
  def queues(@PathParam("id") id : String, @QueryParam("f") f:java.util.List[String],
            @QueryParam("q") q:String, @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer, @QueryParam("o") o:java.util.List[String] ):DataPageDTO = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val values: Iterable[Queue] = router.local_queue_domain.destination_by_id.values

      val records = sync_all(values) { value =>
        status(value, false)
      }

      val rc:FutureResult[DataPageDTO] = records.map(narrow(classOf[QueueStatusDTO], _, f, q, p, ps, o))
      rc
    }
  }

  @GET @Path("virtual-hosts/{id}/queues/{name:.*}")
  def queue(@PathParam("id") id : String, @PathParam("name") name : String, @QueryParam("entries") entries:Boolean ):QueueStatusDTO = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val node = router.local_queue_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      sync(node) {
        status(node, entries)
      }
    }
  }

  @DELETE @Path("virtual-hosts/{id}/queues/{name:.*}")
  @Produces(Array(APPLICATION_JSON, APPLICATION_XML,TEXT_XML))
  def queue_delete(@PathParam("id") id : String, @PathParam("name") name : String):Unit = unwrap_future_result {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val node = router.local_queue_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      admining(node) {
        router._destroy_queue(node)
      }
    }
  }

  @POST @Path("virtual-hosts/{id}/queues/{name:.*}/action/delete")
  @Produces(Array("text/html;qs=5"))
  def post_queue_delete_and_redirect(@PathParam("id") id : String, @PathParam("name") name : String):Unit = unwrap_future_result {
    queue_delete(id, name)
    result(strip_resolve("../../.."))
  }

  @GET @Path("virtual-hosts/{id}/dsubs")
  @Produces(Array(APPLICATION_JSON))
  def durable_subscriptions(@PathParam("id") id : String, @QueryParam("f") f:java.util.List[String],
            @QueryParam("q") q:String, @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer, @QueryParam("o") o:java.util.List[String] ):DataPageDTO = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val values: Iterable[Queue] = router.local_dsub_domain.destination_by_id.values

      val records = sync_all(values) { value =>
        status(value, false)
      }

      val rc:FutureResult[DataPageDTO] = records.map(narrow(classOf[QueueStatusDTO], _, f, q, p, ps, o))
      rc
    }
  }
  @GET @Path("virtual-hosts/{id}/dsubs/{name:.*}")
  def durable_subscription(@PathParam("id") id : String, @PathParam("name") name : String, @QueryParam("entries") entries:Boolean):QueueStatusDTO = {
    with_virtual_host(id) { host =>
      val router:LocalRouter = host
      val node = router.local_dsub_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      sync(node) {
        status(node, entries)
      }
    }
  }


  @DELETE @Path("virtual-hosts/{id}/dsubs/{name:.*}")
  @Produces(Array(APPLICATION_JSON, APPLICATION_XML,TEXT_XML))
  def dsub_delete(@PathParam("id") id : String, @PathParam("name") name : String):Unit = unwrap_future_result {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val node = router.local_dsub_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      admining(node) {
        router._destroy_queue(node)
      }
    }
  }

  @POST @Path("virtual-hosts/{id}/dsubs/{name:.*}/action/delete")
  @Produces(Array("text/html;qs=5"))
  def post_dsub_delete_and_redirect(@PathParam("id") id : String, @PathParam("name") name : String):Unit = unwrap_future_result {
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

  def status(q:Queue, entries:Boolean=false) = monitoring(q) {
    q.status(entries)
  }

  @GET @Path("connectors")
  @Produces(Array(APPLICATION_JSON))
  def connectors(@QueryParam("f") f:java.util.List[String], @QueryParam("q") q:String,
                  @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer, @QueryParam("o") o:java.util.List[String] ):DataPageDTO = {

    with_broker { broker =>
      monitoring(broker) {
        val records = broker.connectors.values.map { value =>
          Success(value.status)
        }
        FutureResult(narrow(classOf[ServiceStatusDTO], records, f, q, p, ps, o))
      }
    }
  }

  @GET @Path("connectors/{id}")
  def connector(@PathParam("id") id : String):ServiceStatusDTO = {
    with_connector(id) { connector =>
      monitoring(connector.broker) {
        connector.status
      }
    }
  }

  @POST @Path("connectors/{id}/action/stop")
  def post_connector_stop(@PathParam("id") id : String):Unit = unwrap_future_result {
    with_connector(id) { connector =>
      admining(connector.broker) {
        connector.stop(NOOP)
      }
    }
    result(strip_resolve(".."))
  }

  @POST @Path("connectors/{id}/action/start")
  def post_connector_start(@PathParam("id") id : String):Unit = unwrap_future_result {
    with_connector(id) { connector =>
      admining(connector.broker) {
        connector.start(NOOP)
      }
    }
    result(strip_resolve(".."))
  }

  @GET
  @Path("connection-metrics")
  def get_connection_metrics(): AggregateConnectionMetricsDTO = {
    val f = new ArrayList[String]()
    f.add("read_counter")
    f.add("write_counter")
    f.add("subscription_count")
    val rs = connections(f, null, null, Integer.MAX_VALUE, Collections.emptyList())
    val rc = new AggregateConnectionMetricsDTO
    import collection.JavaConversions._
    for( row <- rs.rows ) {
      val info = row.asInstanceOf[java.util.List[_]]
      def read(index:Int) = try {
        info.get(index).asInstanceOf[java.lang.Number].longValue()
      } catch {
        case e:Throwable => 0L
      }
      rc.read_counter += read(0)
      rc.write_counter += read(1)
      rc.subscription_count += read(2)
    }
    rc.objects = rs.rows.length
    rc.current_time = System.currentTimeMillis()
    rc
  }


  @GET @Path("connections")
  @Produces(Array(APPLICATION_JSON))
  def connections(@QueryParam("f") f:java.util.List[String], @QueryParam("q") q:String,
                  @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer, @QueryParam("o") o:java.util.List[String] ):DataPageDTO = {

    with_broker { broker =>
      monitoring(broker) {

        val records = sync_all(broker.connections.values) { value =>
          value.get_connection_status
        }

        val rc:FutureResult[DataPageDTO] = records.map(narrow(classOf[ConnectionStatusDTO], _, f, q, p, ps, o))
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
  @Produces(Array(APPLICATION_JSON, APPLICATION_XML,TEXT_XML))
  def connection_delete(@PathParam("id") id : Long):Unit = unwrap_future_result {
    with_connection(id){ connection=>
      admining(connection.connector.broker) {
        connection.stop(NOOP)
      }
    }
  }


  @POST @Path("connections/{id}/action/delete")
  @Produces(Array("text/html;qs=5"))
  def post_connection_delete_and_redirect(@PathParam("id") id : Long):Unit = unwrap_future_result {
    connection_delete(id)
    result(strip_resolve("../../.."))
  }

  @POST
  @Path("action/shutdown")
  def command_shutdown:Unit = unwrap_future_result {
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

}
