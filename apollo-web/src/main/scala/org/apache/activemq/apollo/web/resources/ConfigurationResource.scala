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
import org.apache.activemq.apollo.broker.ConfigStore
import org.apache.activemq.apollo.dto.{ValueDTO, XmlCodec, BrokerDTO}

case class EditConfig(config:String)

/**
 * A broker resource is used to represent the configuration of a broker.
 */
@Produces(Array("application/json", "application/xml","text/xml", "text/html;qs=5"))
case class ConfigurationResource(parent:BrokerResource) extends Resource(parent) {

  lazy val store = {
    val rc = ConfigStore()
    if( rc.can_write ) {
      rc
    } else {
      None
    }.getOrElse(result(NOT_FOUND))
  }

  @GET
  def get() = store.load(false)

  @Produces(Array("text/html"))
  @GET
  @Path("edit")
  def edit_html() = {
    EditConfig(store.read)
  }

  @POST
  @Path("edit")
  def edit_post(@FormParam("config") config:String) = {
    val rc = new ValueDTO
    rc.value = config
    edit_put(rc)
    result(path("../.."))
  }

  @Produces(Array("application/json", "application/xml","text/xml"))
  @GET
  @Path("edit")
  def edit() = {
    val rc = new ValueDTO
    rc.value = store.read
    rc
  }

  @PUT
  @Path("edit")
  def edit_put(config:ValueDTO) = {
    store.write(config.value)
  }

}

