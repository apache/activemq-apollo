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
package org.apache.activemq.apollo.broker.jmx

import org.apache.activemq.apollo.web.resources.Resource
import javax.ws.rs._
import scala.Array
import javax.ws.rs.core.MediaType._
import core.{Response, Context}
import javax.servlet.http.{HttpServletRequestWrapper, HttpServletResponse, HttpServletRequest}
import org.jolokia.http.AgentServlet
import org.apache.activemq.apollo.broker.Broker
import collection.mutable.HashMap
import org.apache.activemq.apollo.web.CustomServletConfig
import javax.servlet.ServletContext
import com.sun.jersey.server.impl.ThreadLocalInvoker
import org.apache.activemq.apollo.util._
import org.fusesource.hawtdispatch._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

@Path("/hawtio/jolokia")
@Produces(Array(APPLICATION_JSON, "text/json"))
class JolokiaResource extends JmxResource

@Path("/jmx/")
@Produces(Array(APPLICATION_JSON, "text/json"))
class JmxResource extends Resource {

  @GET
  def get(@Context ctx:ServletContext, @Context req:HttpServletRequest, @Context resp:HttpServletResponse) = invoke(ctx, req, resp, "")
  @POST
  def post(@Context ctx:ServletContext, @Context req:HttpServletRequest, @Context resp:HttpServletResponse) = invoke(ctx, req, resp, "")
  @PUT
  def put(@Context ctx:ServletContext, @Context req:HttpServletRequest, @Context resp:HttpServletResponse) = invoke(ctx, req, resp, "")
  @DELETE
  def delete(@Context ctx:ServletContext, @Context req:HttpServletRequest, @Context resp:HttpServletResponse) = invoke(ctx, req, resp, "")
  @OPTIONS
  def options(@Context ctx:ServletContext, @Context req:HttpServletRequest, @Context resp:HttpServletResponse) = invoke(ctx, req, resp, "")
  @HEAD
  def head(@Context ctx:ServletContext, @Context req:HttpServletRequest, @Context resp:HttpServletResponse) = invoke(ctx, req, resp, "")

  @GET @Path("{path:.*}")
  def get(@Context ctx:ServletContext, @Context req:HttpServletRequest, @Context resp:HttpServletResponse, @PathParam("path") path:String) = invoke(ctx, req, resp, path)
  @POST @Path("{path:.*}")
  def post(@Context ctx:ServletContext, @Context req:HttpServletRequest, @Context resp:HttpServletResponse, @PathParam("path") path:String) = invoke(ctx, req, resp, path)
  @PUT @Path("{path:.*}")
  def put(@Context ctx:ServletContext, @Context req:HttpServletRequest, @Context resp:HttpServletResponse, @PathParam("path") path:String) = invoke(ctx, req, resp, path)
  @DELETE @Path("{path:.*}")
  def delete(@Context ctx:ServletContext, @Context req:HttpServletRequest, @Context resp:HttpServletResponse, @PathParam("path") path:String) = invoke(ctx, req, resp, path)
  @OPTIONS @Path("{path:.*}")
  def options(@Context ctx:ServletContext, @Context req:HttpServletRequest, @Context resp:HttpServletResponse, @PathParam("path") path:String) = invoke(ctx, req, resp, path)
  @HEAD @Path("{path:.*}")
  def head(@Context ctx:ServletContext, @Context req:HttpServletRequest, @Context resp:HttpServletResponse, @PathParam("path") path:String) = invoke(ctx, req, resp, path)

  def create_servlet(ctx:ServletContext, broker:Broker):AgentServlet = {
    val settings = new HashMap[String, String]
    settings.put("mimeType", "application/json")
    val rc = new AgentServlet
    rc.init(CustomServletConfig("AgentServlet", ctx, settings))
    rc
  }

  def invoke(ctx:ServletContext, req:HttpServletRequest, resp:HttpServletResponse, path:String):String = {

    // Jersey's thread local wrapping messes /w us since we use the http_request
    // in a different threads.  Lets try to unwrap it..
    val unwrapped_req = try {
      java.lang.reflect.Proxy.getInvocationHandler(req).asInstanceOf[ThreadLocalInvoker[HttpServletRequest]].get()
    } catch {
      case e:Throwable =>
        req
    }

    val unwrapped_resp = try {
      java.lang.reflect.Proxy.getInvocationHandler(resp).asInstanceOf[ThreadLocalInvoker[HttpServletResponse]].get()
    } catch {
      case e:Throwable =>
        resp
    }

    val wrapped_req = new HttpServletRequestWrapper(unwrapped_req) {
      override def getPathInfo = path
    }


    FutureResult.unwrap_future_result[Void] {
      with_broker { broker =>
        admining(broker) {
          val rc = FutureResult[Void]()
          Broker.BLOCKABLE_THREAD_POOL {
            val servlet = broker.plugin_state(create_servlet(ctx, broker), classOf[AgentServlet])
            servlet.service(wrapped_req, unwrapped_resp)
            rc.set(Success(null))
          }
          rc
        }
      }
    }
    ""
  }

}