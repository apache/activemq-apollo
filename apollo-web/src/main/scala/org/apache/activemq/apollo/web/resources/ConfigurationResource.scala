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

import javax.ws.rs._
import core.{UriInfo, Response, Context}
import org.fusesource.hawtdispatch._
import Response.Status._
import Response._
import java.net.URI
import java.io.ByteArrayInputStream
import org.apache.activemq.apollo.dto.{XmlCodec, BrokerDTO}
import org.apache.activemq.apollo.broker.ConfigStore

/**
 * A broker resource is used to represent the configuration of a broker.
 */
@Produces(Array("application/json", "application/xml","text/xml", "text/html;qs=5"))
case class ConfigurationResource(parent:BrokerResource) extends Resource(parent) {

  lazy val config = {
    ConfigStore.sync{ store=>
      store.getBroker(parent.id, false).getOrElse(result(NOT_FOUND))
    }
  }


  @GET
  def get(@Context uriInfo:UriInfo) = {
    val ub = uriInfo.getAbsolutePathBuilder()
    seeOther(path(config.rev)).build
  }

  @GET @Path("{rev}")
  def getConfig(@PathParam("rev") rev:Int):BrokerDTO = {
    // that rev may have gone away..
    config.rev==rev || result(NOT_FOUND)
    config
  }

  @POST @Path("{rev}")
  def post(@PathParam("rev") rev:Int, @FormParam("config") config:String) = {
    val dto = XmlCodec.unmarshalBrokerDTO(new ByteArrayInputStream(config.getBytes("UTF-8")))
    put(rev, dto)
    result(path("../"+dto.rev))
  }

  @PUT @Path("{rev}")
  def put(@PathParam("rev") rev:Int, config:BrokerDTO) = {
    config.id = parent.id;
    config.rev = rev
    ConfigStore.sync { store=>
      store.putBroker(config) || result(NOT_FOUND)
    }
  }

  @DELETE @Path("{rev}")
  def delete(@PathParam("rev") rev:Int) = {
    ConfigStore.sync { store=>
      store.removeBroker(parent.id, rev) || result(NOT_FOUND)
    }
  }

}

