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
package org.apache.activemq.apollo.web.resources

import java.lang.String
import com.sun.jersey.api.NotFoundException
import javax.ws.rs._
import core.{UriInfo, Response, Context}
import org.fusesource.scalate.util.Logging
import reflect.{BeanProperty}
import com.sun.jersey.api.view.ImplicitProduces
import org.fusesource.hawtdispatch.Future
import Response._
import Response.Status._
import org.apache.activemq.apollo.dto.{IdListDTO, BrokerSummaryDTO, BrokerDTO}
import java.util.{Arrays, Collections}
import org.apache.activemq.apollo.web.ConfigStore
import org.apache.activemq.apollo.broker.BrokerRegistry
import collection.JavaConversions._
import com.sun.jersey.api.core.ResourceContext
import org.fusesource.scalate.RenderContext

/**
 * Defines the default representations to be used on resources
 */
@ImplicitProduces(Array("text/html;qs=5"))
@Produces(Array("application/json", "application/xml","text/xml"))
abstract class Resource(private val parent:Resource=null) extends Logging {

  @Context
  var uri_info:UriInfo = null

  if( parent!=null ) {
    this.uri_info = parent.uri_info
  }

  def result[T](value:Status, message:Any=null):T = {
    val response = Response.status(value)
    if( message!=null ) {
      response.entity(message)
    }
    throw new WebApplicationException(response.build)
  }

  def path(value:Any) = uri_info.getAbsolutePathBuilder().path(value.toString).build()

}

class ViewHelper {

  lazy val uri_info = RenderContext().attribute[UriInfo]("uri_info")

  def path(value:Any) = {
    uri_info.getAbsolutePathBuilder().path(value.toString).build()
  }

}

/**
 * Manages a collection of broker resources.
 */
@Path("/brokers")
class RootResource() extends Resource {

  @GET
  def brokers = {
    val rc = new IdListDTO
    val ids = Future[List[String]] { cb=>
      ConfigStore().listBrokers(cb)
    }.toArray[String]
    rc.ids.addAll(Arrays.asList(ids: _*))
    rc
  }

  @Path("{id}")
  def broker(@PathParam("id") id : String): BrokerResource = {
    new BrokerResource(this, id)
  }
}

/**
 * Resource that identifies a managed broker.
 */
case class BrokerResource(parent:RootResource, @BeanProperty id: String) extends Resource(parent) {

  @GET
  def get = {
    val rc = new BrokerSummaryDTO
    rc.id = id
    rc.manageable = BrokerRegistry.get(id)!=null
    rc.configurable = Future[Option[BrokerDTO]] { cb=>
        ConfigStore().getBroker(id, false)(cb)
      }.isDefined
    rc
  }

  @Path("config")
  def config = ConfigurationResource(this)

  @Path("runtime")
  def runtime = RuntimeResource(this)

}

