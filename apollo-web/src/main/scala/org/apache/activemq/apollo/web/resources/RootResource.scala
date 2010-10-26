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
import reflect.{BeanProperty}
import com.sun.jersey.api.view.ImplicitProduces
import org.fusesource.hawtdispatch.Future
import Response._
import Response.Status._
import org.apache.activemq.apollo.broker.ConfigStore
import org.apache.activemq.apollo.broker.BrokerRegistry
import collection.JavaConversions._
import com.sun.jersey.api.core.ResourceContext
import org.fusesource.scalate.RenderContext
import java.util.concurrent.TimeUnit
import org.apache.activemq.apollo.dto._
import java.util.{Arrays, Collections}
import org.apache.activemq.apollo.util.Logging

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

object ViewHelper {

  val KB: Long = 1024
  val MB: Long = KB * 1024
  val GB: Long = MB * 1024
  val TB: Long = GB * 1024

  val SECONDS: Long = TimeUnit.SECONDS.toMillis(1)
  val MINUTES: Long = TimeUnit.SECONDS.toMillis(60)
  val HOURS: Long = TimeUnit.SECONDS.toMillis(3600)
  val DAYS: Long = TimeUnit.SECONDS.toMillis(216000)
  val YEARS: Long = DAYS * 365


}
class ViewHelper {
  import ViewHelper._

  lazy val uri_info = RenderContext().attribute[UriInfo]("uri_info")

  def path(value:Any) = {
    uri_info.getAbsolutePathBuilder().path(value.toString).build()
  }

  def memory(value:Int):String = memory(value.toLong)
  def memory(value:Long):String = {

    if( value < KB ) {
      "%d bytes".format(value)
    } else if( value < MB ) {
       "%,.2f kb".format(value.toFloat/KB)
    } else if( value < GB ) {
      "%,.3f mb".format(value.toFloat/MB)
    } else if( value < TB ) {
      "%,.4f gb".format(value.toDouble/GB)
    } else {
      "%,.5f tb".format(value.toDouble/TB)
    }
  }

  def uptime(value:Long):String = {
    val duration = System.currentTimeMillis - value
    if( duration < SECONDS ) {
      "%d ms".format(duration)
    } else if (duration < MINUTES) {
      "%,.2f seconds".format(duration.toFloat / SECONDS)
    } else if (duration < HOURS) {
      "%,.2f minutes".format(duration.toFloat / MINUTES)
    } else if (duration < DAYS) {
      "%,.2f hours".format(duration.toFloat / HOURS)
    } else if (duration < YEARS) {
      "%,.2f days".format(duration.toDouble / DAYS)
    } else {
      "%,.2f years".format(duration.toDouble / YEARS)
    }
  }
}

/**
 * Index resource
 */
@Path("/")
class IndexResource() extends Resource {
}


/**
 * Manages a collection of broker resources.
 */
@Path("/brokers")
class RootResource() extends Resource {

  @GET
  def brokers = {
    val rc = new StringIdListDTO
    Future[List[String]] { cb=>
      ConfigStore().listBrokers(cb)
    }.foreach { x=> 
      rc.items.add( new StringIdLabeledDTO(x,x) )
    }
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

