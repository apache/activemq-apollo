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
import org.fusesource.hawtdispatch.Future
import Response.Status._
import Response._
import org.apache.activemq.apollo.dto.{BrokerDTO}
import org.apache.activemq.apollo.web.ConfigStore
import java.net.URI

/**
 * A broker resource is used to represent the configuration of a broker.
 */
case class ConfigurationResource(parent:Broker) extends Resource {


  def store = ConfigStore()

  lazy val config = {
    Future[Option[BrokerDTO]] { cb=>
      store.getBroker(parent.id, false)(cb)
    }.getOrElse(result(NOT_FOUND))
  }


  @GET
  def get(@Context uriInfo:UriInfo) = {
    val ub = uriInfo.getAbsolutePathBuilder()
    seeOther(ub.path(config.rev.toString).build()).build
  }

  @GET @Path("{rev}")
  def getConfig(@PathParam("rev") rev:Int):BrokerDTO = {
    // that rev may have gone away..
    config.rev==rev || result(NOT_FOUND)
    config
  }

  @PUT @Path("{rev}")
  def put(@PathParam("rev") rev:Int, config:BrokerDTO) = {
    config.id = parent.id;
    config.rev = rev
    Future[Boolean] { cb=>
      store.putBroker(config)(cb)
    } || result(NOT_FOUND)
  }

  @DELETE @Path("{rev}")
  def delete(@PathParam("rev") rev:Int) = {
    Future[Boolean] { cb=>
      store.removeBroker(parent.id, rev)(cb)
    } || result(NOT_FOUND)
  }

}

