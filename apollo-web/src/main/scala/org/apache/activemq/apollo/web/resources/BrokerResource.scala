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
import core.{Response, Context}
import javax.ws.rs.core.Response.Status._
import management.ManagementFactory
import javax.management.ObjectName
import javax.management.openmbean.CompositeData
import org.josql.{QueryResults, Query}
import java.util.regex.Pattern
import java.util.{Collections, ArrayList}
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.dto._
import javax.ws.rs.core.MediaType._
import javax.servlet.http.HttpServletResponse
import FutureResult._
import com.wordnik.swagger.annotations.{ApiOperation, Api}
import language.implicitConversions
import org.fusesource.hawtbuf.Buffer
import org.apache.commons.codec.binary.Base64
;

@Path(          "/api/json/broker")
@Api(value =    "/api/json/broker",
  listingPath = "/api/docs/broker")
@Produces(Array("application/json"))
class BrokerResourceJSON extends BrokerResource

@Path(          "/api/docs/broker{ext:(\\.json)?}")
@Api(value =    "/api/json/broker",
  listingPath = "/api/docs/broker",
  listingClass = "org.apache.activemq.apollo.web.resources.BrokerResourceJSON")
class BrokerResourceHelp extends HelpResourceJSON

@Path("/broker")
@Produces(Array(APPLICATION_JSON, APPLICATION_XML, TEXT_XML, "text/html;qs=5"))
class BrokerResourceHTML extends BrokerResource


/**
 * <p>
 * The RuntimeResource resource manages access to the runtime state of a broker.  It is used
 * to see the status of the broker and to apply management operations against the broker.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class BrokerResource() extends Resource {
  import Resource._

  def SessionResource() = {
    val rc = new SessionResource()
    rc.setHttpRequest(http_request)
    rc
  }

  @Deprecated()
  @GET
  @Path("/whoami")
  def whoami() = SessionResource().whoami()

  @Deprecated()
  @Produces(Array("text/html;qs=5"))
  @GET
  @Path("/signin")
  def get_signin_html(@Context response:HttpServletResponse,
                      @QueryParam("username") username:String,
                      @QueryParam("password") password:String,
                      @QueryParam("target") target:String
                             ): ErrorDTO = SessionResource().get_signin_html(response, username, password, target)

  @Deprecated()
  @POST
  @Path("/signin")
  def post_signin(@Context response:HttpServletResponse,
                  @FormParam("username") username:String,
                  @FormParam("password") password:String):Boolean = SessionResource().post_signin(response, username, password)

  @Deprecated()
  @Produces(Array("text/html"))
  @GET @Path("/signout")
  def signout_html():String = SessionResource().signout_html()

  @Deprecated()
  @Produces(Array(APPLICATION_JSON, APPLICATION_XML, TEXT_XML))
  @GET @Path("/signout")
  def signout():String = SessionResource().signout()

  @GET
  @ApiOperation(value = "Returns a BrokerStatusDTO which contains summary information about the broker and the JVM")
  def get_broker(@QueryParam("connections") connections:Boolean):BrokerStatusDTO = {
    with_broker { broker =>
      monitoring(broker) {
        val result = new BrokerStatusDTO

        result.id = broker.id
        result.jvm_metrics = create_jvm_metrics
        result.current_time = now
        result.state = broker.service_state.toString
        result.state_since = broker.service_state.since
        result.version = Broker.version
        result.home_location = Option(System.getProperty("apollo.home")).getOrElse(null)
        result.base_location = Option(System.getProperty("apollo.base")).getOrElse(null)
        result.version = Broker.version
        result.connection_counter = broker.connection_id_counter.get()
        result.connected = broker.connections.size
        broker.virtual_hosts.values.foreach{ host=>
          result.virtual_hosts.add( host.id )
        }

        broker.connectors.values.foreach{ c=>
          result.connectors.add( c.id )
          val status = c.status
          status match {
            case status:ConnectorStatusDTO =>
              result.messages_sent += status.messages_sent
              result.messages_received += status.messages_received
              result.read_counter += status.read_counter
              result.write_counter += status.write_counter
          }
        }

        // only include the connection list if it was requested.
        if( !connections ) {
          result.connections = null;
        } else {
          broker.connections.foreach { case (id,connection) =>
            result.connections.add( new LongIdLabeledDTO(id, connection.transport.getRemoteAddress.toString ) )
          }
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
        case _:Throwable => // ignore
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
  @Path("/queue-metrics")
  @ApiOperation(value = "Returns an AggregateDestMetricsDTO holding the summary of all the queue metrics")
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
  @Path("/topic-metrics")
  @ApiOperation(value = "Returns an AggregateDestMetricsDTO holding the summary of all the topic metrics")
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
  @Path("/dsub-metrics")
  @ApiOperation(value = "Returns an AggregateDestMetricsDTO holding the summary of all the durable subscription metrics")
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
  @Path("/dest-metrics")
  @ApiOperation(value = "Returns an AggregateDestMetricsDTO holding the summary of all the destination metrics")
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


  @GET @Path("/virtual-hosts")
  @Produces(Array(APPLICATION_JSON))
  @ApiOperation(value = "Returns an DataPageDTO holding all the virtual hosts")
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

  @GET @Path("/virtual-hosts/{id}")
  @ApiOperation(value = "Returns a VirtualHostStatusDTO holding the status of the requested virtual host")
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

  @GET @Path("/virtual-hosts/{id}/queue-metrics")
  @ApiOperation(value = "Aggregates the messaging metrics for all the queue destinations")
  def virtual_host_queue_metrics(@PathParam("id") id : String): AggregateDestMetricsDTO = {
    val rc:AggregateDestMetricsDTO = with_virtual_host(id) { host =>
      monitoring(host) {
        get_queue_metrics(host)
      }
    }
    rc.current_time = now
    rc
  }

  @GET @Path("/virtual-hosts/{id}/topic-metrics")
  @ApiOperation(value = "Aggregates the messaging metrics for all the topic destinations")
  def virtual_host_topic_metrics(@PathParam("id") id : String): AggregateDestMetricsDTO = {
    val rc:AggregateDestMetricsDTO = with_virtual_host(id) { host =>
      monitoring(host) {
        get_topic_metrics(host)
      }
    }
    rc.current_time = now
    rc
  }

  @GET @Path("/virtual-hosts/{id}/dsub-metrics")
  @ApiOperation(value = "Aggregates the messaging metrics for all the durable subscription destinations")
  def virtual_host_dsub_metrics(@PathParam("id") id : String): AggregateDestMetricsDTO = {
    val rc:AggregateDestMetricsDTO = with_virtual_host(id) { host =>
      monitoring(host) {
        get_dsub_metrics(host)
      }
    }
    rc.current_time = now
    rc
  }

  @GET @Path("/virtual-hosts/{id}/dest-metrics")
  @ApiOperation(value = "Aggregates the messaging metrics for all the destinations")
  def virtual_host_dest_metrics(@PathParam("id") id : String): AggregateDestMetricsDTO = {
    with_virtual_host(id) { host =>
      monitoring(host) {
        host.get_dest_metrics
      }
    }
  }


  @GET @Path("/virtual-hosts/{id}/store")
  @ApiOperation(value = "Gets metrics about the status of the message store used by the {host} virtual host.")
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

  @POST @Path("/virtual-hosts/{id}/store/action/compact")
  @ApiOperation(value = "Compacts the store.")
  @Produces(Array("text/html;qs=5"))
  def store_compact(@PathParam("id") id : String):Unit = {
    with_virtual_host(id) { host =>
      admining(host) {
        if(host.store!=null) {
          val rc = FutureResult[Unit]()
          host.store.compact {
            rc(Success(()))
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

  @GET @Path("/virtual-hosts/{id}/topics")
  @ApiOperation(value = "Gets a list of all the topics that exist on the broker.")
  @Produces(Array(APPLICATION_JSON))
  def topics(@PathParam("id") id : String, @QueryParam("f") f:java.util.List[String],
            @QueryParam("q") q:String, @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer, @QueryParam("o") o:java.util.List[String] ):DataPageDTO = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val records = Future.all {
        router.local_topic_domain.destination_by_id.values.map { value  =>
          monitoring(value) {
            value.status(false, false)
          }
        }
      }
      val rc:FutureResult[DataPageDTO] = records.map(narrow(classOf[TopicStatusDTO], _, f, q, p, ps, o))
      rc
    }
  }

  @GET @Path("/virtual-hosts/{id}/topics/{name:.*}")
  @ApiOperation(value = "Gets the status of the named topic.")
  def topic(@PathParam("id") id : String, @PathParam("name") name : String,
            @QueryParam("producers") producers:Boolean,
            @QueryParam("consumers") consumers:Boolean):TopicStatusDTO = {
    with_virtual_host(id) { host =>
      val router:LocalRouter = host
      val node = router.local_topic_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      monitoring(node) {
        node.status(producers, consumers)
      }
    }
  }

  @PUT @Path("/virtual-hosts/{id}/topics/{name:.*}")
  @Produces(Array(APPLICATION_JSON, APPLICATION_XML,TEXT_XML))
  @ApiOperation(value = "Creates the named topic.")
  def topic_create(@PathParam("id") id : String, @PathParam("name") name : String) = ok {
    with_virtual_host(id) { host =>
      val rc = FutureResult[Null]()
      authenticate(host.broker.authenticator) { security_context =>
        val address = (new DestinationParser).decode_single_destination("topic:"+name, null)
        host.local_topic_domain.get_or_create_destination(address, security_context).failure_option match {
          case Some(x) => rc.set(Failure(new Exception(x)))
          case _ =>
        }
        rc.set(Success(null))
      }
      rc
    }
  }

  @DELETE @Path("/virtual-hosts/{id}/topics/{name:.*}")
  @Produces(Array(APPLICATION_JSON, APPLICATION_XML,TEXT_XML))
  @ApiOperation(value = "Deletes the named topic.")
  def topic_delete(@PathParam("id") id : String, @PathParam("name") name : String) = ok {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val node = router.local_topic_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      admining(node) {
        node.delete.map(result(NOT_MODIFIED, _))
      }
    }
  }

  @POST @Path("/virtual-hosts/{id}/topics/{name:.*}/action/delete")
  @Produces(Array("text/html;qs=5"))
  def post_topic_delete_and_redirect(@PathParam("id") id : String, @PathParam("name") name : String) = {
    if_ok(topic_delete(id, name)) {
      result(strip_resolve("../../.."))
    }
  }

  @GET @Path("/virtual-hosts/{id}/topic-queues/{name:.*}/{qid}")
    @ApiOperation(value = "Gets the status of a topic consumer queue.")
    def topic(@PathParam("id") id : String,@PathParam("name") name : String,  @PathParam("qid") qid : Long,
              @QueryParam("entries") entries:Boolean,
              @QueryParam("producers") producers:Boolean,
              @QueryParam("consumers") consumers:Boolean):QueueStatusDTO = {
      with_virtual_host(id) { host =>
        val router:LocalRouter = host
        val node = router.local_topic_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
        val queue =router.queues_by_store_id.get(qid).getOrElse(result(NOT_FOUND))
        monitoring(node) {
          sync(queue) {
            queue.status(entries, producers, consumers)
          }
        }
      }
    }


  @GET @Path("/virtual-hosts/{id}/topics/{name:.*}/messages")
  @ApiOperation(value = "Gets a list of recent messages sent to the topic.")
  def topic_messages(@PathParam("id") id : String, @PathParam("name") name : String,
                     @QueryParam("from") _from:java.lang.Long,
                     @QueryParam("max") _max:java.lang.Long,
                     @QueryParam("max_body") _max_body:java.lang.Integer):BrowsePageDTO = {
    var from = OptionSupport(_from).getOrElse(0L)
    var max = OptionSupport(_max).getOrElse(100L)
    var max_body = OptionSupport(_max_body).getOrElse(0)
    with_virtual_host(id) { host =>
      val rc = FutureResult[BrowsePageDTO]()
      val router: LocalRouter = host
      val node = router.local_topic_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      monitoring(node) {
        node.browse(from, None, max) { browse_result =>
          val page = new BrowsePageDTO()
          for( entry <- browse_result.entries ) {
            page.messages.add(message_convert(max_body, entry))
          }
          page.first_seq = browse_result.first_seq
          page.last_seq = browse_result.last_seq
          page.total_messages = browse_result.total_entries
          rc.set(Success(page))
        }
      }
      rc
    }
  }

  @GET @Path("/virtual-hosts/{id}/queues")
  @ApiOperation(value = "Gets a list of all the queues that exist on the broker.")
  @Produces(Array(APPLICATION_JSON))
  def queues(@PathParam("id") id : String, @QueryParam("f") f:java.util.List[String],
            @QueryParam("q") q:String, @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer, @QueryParam("o") o:java.util.List[String] ):DataPageDTO = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val values: Iterable[Queue] = router.local_queue_domain.destination_by_id.values

      val records = sync_all(values) { value =>
        status(value, false, false , false)
      }

      val rc:FutureResult[DataPageDTO] = records.map(narrow(classOf[QueueStatusDTO], _, f, q, p, ps, o))
      rc
    }
  }

  @GET @Path("/virtual-hosts/{id}/queues/{name:.*}")
  @ApiOperation(value = "Gets the status of the named queue.")
  def queue(@PathParam("id") id : String, @PathParam("name") name : String,
            @QueryParam("entries") entries:Boolean,
            @QueryParam("producers") producers:Boolean,
            @QueryParam("consumers") consumers:Boolean ):QueueStatusDTO = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val node = router.local_queue_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      sync(node) {
        status(node, entries, producers, consumers)
      }
    }
  }
  @GET @Path("/virtual-hosts/{id}/queues/{name:.*}/messages")
  @ApiOperation(value = "Gets a list of messages that exist on the queue.")
  def queue_messages(@PathParam("id") id : String, @PathParam("name") name : String,
                      @QueryParam("from") _from:java.lang.Long,
                      @QueryParam("to") _to:java.lang.Long,
                      @QueryParam("max") _max:java.lang.Long,
                      @QueryParam("max_body") _max_body:java.lang.Integer):BrowsePageDTO = {
    var from = OptionSupport(_from).getOrElse(0L)
    var to = OptionSupport(_to)
    var max = OptionSupport(_max).getOrElse(100L)
    var max_body = OptionSupport(_max_body).getOrElse(0)
    with_virtual_host(id) { host =>
      val rc = FutureResult[BrowsePageDTO]()
      val router: LocalRouter = host
      val node = router.local_queue_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      monitoring(node) {
        node.browse(from, to, max) { browe_result =>
          val page = new BrowsePageDTO()
          for( entry <- browe_result.entries ) {
            page.messages.add(message_convert(max_body, entry))
          }
          page.first_seq = browe_result.first_seq
          page.last_seq = browe_result.last_seq
          page.total_messages = browe_result.total_entries
          rc.set(Success(page))
        }
      }
      rc
    }
  }


  @DELETE @Path("/virtual-hosts/{id}/queues/{name:.*}")
  @Produces(Array(APPLICATION_JSON, APPLICATION_XML,TEXT_XML))
  @ApiOperation(value = "Deletes the named queue.")
  def queue_delete(@PathParam("id") id : String, @PathParam("name") name : String) = ok {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val node = router.local_queue_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      admining(node) {
        host.dispatch_queue {
          router._destroy_queue(node)
        }
      }
    }
  }

  @PUT @Path("/virtual-hosts/{id}/queues/{name:.*}")
  @Produces(Array(APPLICATION_JSON, APPLICATION_XML,TEXT_XML))
  @ApiOperation(value = "Creates the named queue.")
  def queue_create(@PathParam("id") id : String, @PathParam("name") name : String) = ok {
    with_virtual_host(id) { host =>
      val rc = FutureResult[Null]()
      authenticate(host.broker.authenticator) { security_context =>
        val address = (new DestinationParser).decode_single_destination("queue:"+name, null)
        host.local_queue_domain.get_or_create_destination(address, security_context).failure_option match {
          case Some(x) => rc.set(Failure(new Exception(x)))
          case _ =>
        }
        rc.set(Success(null))
      }
      rc
    }
  }

  @POST @Path("/virtual-hosts/{id}/queues/{name:.*}/action/delete")
  @Produces(Array("text/html;qs=5"))
  def post_queue_delete_and_redirect(@PathParam("id") id : String, @PathParam("name") name : String) = {
    if_ok(queue_delete(id, name)) {
      result(strip_resolve("../../.."))
    }
  }

  private def base64(buffer:Buffer) =
    new String(Base64.encodeBase64(buffer.toByteArray), "UTF-8");

  private def message_convert(max_body:Int, value:(EntryStatusDTO, Delivery)): MessageStatusDTO = {
    val (entry, delivery) = value
    val rc = new MessageStatusDTO
    rc.codec = delivery.message.codec.id()
    rc.headers = delivery.message.headers_as_json;
    rc.expiration = delivery.expiration
    rc.persistent = delivery.persistent
    rc.entry = entry

    if( max_body > 0 ) {
      val body = new Buffer(delivery.message.getBodyAs(classOf[Buffer]))
      if( body.length > max_body) {
        body.length = max_body
        rc.body_truncated = true
      }
      rc.base64_body = base64(body)
    }
    rc
  }

  @GET @Path("/virtual-hosts/{id}/dsubs")
  @ApiOperation(value = "Gets a list of all the durable subscriptions that exist on the broker.")
  @Produces(Array(APPLICATION_JSON))
  def durable_subscriptions(@PathParam("id") id : String, @QueryParam("f") f:java.util.List[String],
            @QueryParam("q") q:String, @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer, @QueryParam("o") o:java.util.List[String] ):DataPageDTO = {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val values: Iterable[Queue] = router.local_dsub_domain.destination_by_id.values

      val records = sync_all(values) { value =>
        status(value, false, false, false)
      }

      val rc:FutureResult[DataPageDTO] = records.map(narrow(classOf[QueueStatusDTO], _, f, q, p, ps, o))
      rc
    }
  }

  @GET @Path("/virtual-hosts/{id}/dsubs/{name:.*}")
  @ApiOperation(value = "Gets the status of the named durable subscription.")
  def durable_subscription(@PathParam("id") id : String, @PathParam("name") name : String,
                           @QueryParam("entries") entries:Boolean,
                           @QueryParam("producers") producers:Boolean,
                           @QueryParam("consumers") consumers:Boolean):QueueStatusDTO = {
    with_virtual_host(id) { host =>
      val router:LocalRouter = host
      val node = router.local_dsub_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      sync(node) {
        status(node, entries, producers, consumers)
      }
    }
  }


  @DELETE @Path("/virtual-hosts/{id}/dsubs/{name:.*}")
  @ApiOperation(value = "Deletes the named virtual host.")
  @Produces(Array(APPLICATION_JSON, APPLICATION_XML,TEXT_XML))
  def dsub_delete(@PathParam("id") id : String, @PathParam("name") name : String) = ok {
    with_virtual_host(id) { host =>
      val router: LocalRouter = host
      val node = router.local_dsub_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      admining(node) {
        host.dispatch_queue {
          router._destroy_queue(node)
        }
      }
    }
  }

  @POST @Path("/virtual-hosts/{id}/dsubs/{name:.*}/action/delete")
  @Produces(Array("text/html;qs=5"))
  def post_dsub_delete_and_redirect(@PathParam("id") id : String, @PathParam("name") name : String) = {
    if_ok(dsub_delete(id, name)) {
      result(strip_resolve("../../.."))
    }
  }

  @GET @Path("/virtual-hosts/{id}/dsubs/{name:.*}/messages")
  @ApiOperation(value = "Gets a list of the messages that exist on the durable sub.")
  def dsub_messages(@PathParam("id") id : String, @PathParam("name") name : String,
             @QueryParam("from") _from:java.lang.Long,
             @QueryParam("to") _to:java.lang.Long,
             @QueryParam("max") _max:java.lang.Long,
             @QueryParam("max_body") _max_body:java.lang.Integer):BrowsePageDTO = {
    var from = OptionSupport(_from).getOrElse(0L)
    var to = OptionSupport(_to)
    var max = OptionSupport(_max).getOrElse(100L)
    var max_body = OptionSupport(_max_body).getOrElse(0)

    with_virtual_host(id) { host =>
      val rc = FutureResult[BrowsePageDTO]()
      val router: LocalRouter = host
      val node = router.local_dsub_domain.destination_by_id.get(name).getOrElse(result(NOT_FOUND))
      monitoring(node) {
        node.browse(from, to, max) { browse_result =>
          val page = new BrowsePageDTO()
          for( entry <- browse_result.entries ) {
            page.messages.add(message_convert(max_body, entry))
          }
          page.first_seq = browse_result.first_seq
          page.last_seq = browse_result.last_seq
          page.total_messages = browse_result.total_entries
          rc.set(Success(page))
        }
      }
      rc
    }
  }

  private def decode_path(name:String) = {
    try {
      LocalRouter.destination_parser.decode_path(name)
    } catch {
      case x:PathParser.PathException => result(NOT_FOUND)
    }
  }

  def status(q:Queue, entries:Boolean=false, producers:Boolean, consumers:Boolean) = monitoring(q) {
    q.status(entries, producers, consumers)
  }

  @GET @Path("/connectors")
  @ApiOperation(value = "Gets a paginated view of all the the connectors .")
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

  @GET @Path("/connectors/{id}")
  @ApiOperation(value = "Gets the status of the specified connector.")
  def connector(@PathParam("id") id : String, @QueryParam("connections") connections:Boolean):ServiceStatusDTO = {
    with_connector(id) { connector =>
      monitoring(connector.broker) {
        val rc = connector.status

        // only include the connection list if it was requested.
        rc match {
          case rc:ConnectorStatusDTO=>
            if( !connections ) {
              rc.connections = null
            }
          case _ =>
        }
        rc
      }
    }
  }

  @POST @Path("/connectors/{id}/action/stop")
  @ApiOperation(value = "Stops a connector.")
  def post_connector_stop(@PathParam("id") id : String) = ok {
    with_connector(id) { connector =>
      admining(connector.broker) {
        connector.stop(NOOP)
      }
    }
    result(strip_resolve(".."))
  }

  @POST @Path("/connectors/{id}/action/start")
  @ApiOperation(value = "Starts a connector.")
  def post_connector_start(@PathParam("id") id : String) = ok {
    with_connector(id) { connector =>
      admining(connector.broker) {
        connector.start(NOOP)
      }
    }
    result(strip_resolve(".."))
  }

  @GET
  @Path("/connection-metrics")
  @ApiOperation(value = "Aggregates connection metreics for all the connectors.")
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


  @GET @Path("/connections")
  @ApiOperation(value = "A paged view of all the connections.")
  @Produces(Array(APPLICATION_JSON))
  def connections(@QueryParam("f") f:java.util.List[String], @QueryParam("q") q:String,
                  @QueryParam("p") p:java.lang.Integer, @QueryParam("ps") ps:java.lang.Integer, @QueryParam("o") o:java.util.List[String] ):DataPageDTO = {

    with_broker { broker =>
      monitoring(broker) {

        val records = sync_all(broker.connections.values) { value =>
          value.get_connection_status(false)
        }

        val rc:FutureResult[DataPageDTO] = records.map(narrow(classOf[ConnectionStatusDTO], _, f, q, p, ps, o))
        rc
      }
    }
  }

  @GET @Path("/connections/{id}")
  @ApiOperation(value = "Gets that status of a connection.")
  def connection(@PathParam("id") id : Long, @QueryParam("debug") debug:Boolean):ConnectionStatusDTO = {
    with_connection(id){ connection=>
      monitoring(connection.connector.broker) {
        sync(connection) {
          connection.get_connection_status(debug)
        }
      }
    }
  }

  @DELETE @Path("/connections/{id}")
  @ApiOperation(value = "Disconnect a connection from the broker.")
  @Produces(Array(APPLICATION_JSON, APPLICATION_XML,TEXT_XML))
  def connection_delete(@PathParam("id") id : Long) = ok {
    with_connection(id){ connection=>
      admining(connection.connector.broker) {
        connection.stop(NOOP)
      }
    }
  }


  @POST @Path("/connections/{id}/action/delete")
  @ApiOperation(value = "Disconnect a connection from the broker.")
  @Produces(Array("text/html;qs=5"))
  def post_connection_delete_and_redirect(@PathParam("id") id : Long) = {
    if_ok(connection_delete(id)) {
      result(strip_resolve("../../.."))
    }
  }

  @POST
  @Path("/action/shutdown")
  @ApiOperation(value = "Shutsdown the JVM")
  def command_shutdown = ok {
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

  @GET
  @Path("/hawtdispatch/profile")
  @ApiOperation(value="Enables or disables profiling")
  def hawtdispatch_profile(@QueryParam("enabled") enabled : Boolean):String = {
    with_broker { broker =>
      admining(broker) {
        Dispatch.profile(enabled)
        ""
      }
    }
  }

  @GET
  @Path("/hawtdispatch/metrics")
  @ApiOperation(value="Enables or disables profiling")
  def hawtdispatch_metrics():Array[DispatchQueueMetrics] = {
    with_broker { broker =>
      monitoring(broker) {
        val m = Dispatch.metrics()
        m.toArray(new Array[Metrics](m.size())).sortWith{ case (l,r)=> l.totalRunTimeNS > r.totalRunTimeNS }.map { x =>
          val rc = new DispatchQueueMetrics
          rc.queue = x.queue.getLabel
          rc.duration = x.durationNS

          rc.waiting = (x.enqueued - x.dequeued).max(0)
          rc.wait_time_max = x.maxWaitTimeNS
          rc.wait_time_total = x.totalWaitTimeNS

          rc.execute_time_max = x.maxRunTimeNS
          rc.execute_time_total = x.totalRunTimeNS
          rc.executed = x.dequeued
          rc
        }
      }
    }
  }

}
