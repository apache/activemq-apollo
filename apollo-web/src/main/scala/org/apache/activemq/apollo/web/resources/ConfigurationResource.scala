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

import org.apache.activemq.apollo.dto.{XmlCodec, BrokerDTO}
import org.fusesource.hawtbuf._
import java.io.File
import org.apache.activemq.apollo.util.FileSupport._
import javax.ws.rs._
import javax.ws.rs.core.Response.Status._
import com.sun.jersey.api.view.ImplicitProduces

case class EditConfig(file:String, config:String, can_write:Boolean)
case class ListConfigs(files:Array[String])

/**
 * A broker resource is used to represent the configuration of a broker.
 */
case class ConfigurationResource(parent:BrokerResource, dto:BrokerDTO) extends Resource(parent) {

  lazy val etc_directory = {
    val apollo_base = Option(System.getProperty("apollo.base")).getOrElse(result(NOT_FOUND))
    val rc = new File(apollo_base) / "etc"
    if ( !rc.exists() || !rc.isDirectory ) {
      result(NOT_FOUND)
    }
    rc
  }

  @GET
  @Path("runtime")
  @Produces(Array("application/json", "application/xml","text/xml"))
  def runtime = {

    // Encode/Decode the runtime config so that we can get a copy that
    // we can modify..
    val baos = new ByteArrayOutputStream
    XmlCodec.encode(dto, baos, false)
    val copy = XmlCodec.decode(classOf[BrokerDTO], new ByteArrayInputStream(baos.toBuffer))

    // Scrub out the passwords to avoid a security leak.
    if( copy.key_storage !=null ) {
      copy.key_storage.password = null
      copy.key_storage.key_password = null
    }

    copy
  }

  @GET
  @Produces(Array("application/json"))
  @Path("files")
  def list() = {
    etc_directory.listFiles().flatMap { file =>
      if( file.canRead ) {
        Some(file.getName)
      } else {
        None
      }
    }
  }

  @GET
  @Produces(Array("text/html"))
  @Path("files")
  def list_html() = {
    ListConfigs(list())
  }

  @GET
  @Produces(Array("text/plain"))
  @Path("files/{name}")
  def get(@PathParam("name") name:String) = {
    val file = etc_directory / name
    if( !file.exists() || !file.canRead || file.getParentFile != etc_directory ) {
      result(NOT_FOUND)
    }
    file.read_bytes
  }

  @GET
  @Produces(Array("text/html"))
  @Path("files/{name}")
  def edit_html(@PathParam("name") name:String) = {
    val file = etc_directory / name
    if( !file.exists() || !file.canRead || file.getParentFile != etc_directory ) {
      result(NOT_FOUND)
    }
    EditConfig(name, file.read_text(), file.canWrite)
  }

  @PUT
  @Path("files/{name}")
  def put(@PathParam("name") name:String, config:Array[Byte]):Unit = {
    val file = etc_directory / name
    if( !file.exists() || !file.canWrite || file.getParentFile != etc_directory ) {
      result(NOT_FOUND)
    }
    file.write_bytes(config)
  }

  @POST
  @Path("files/{name}")
  @Produces(Array("application/json", "application/xml","text/xml", "text/html"))
  def edit_post(@PathParam("name") name:String, @FormParam("config") config:String):Unit = {
    put(name, config.getBytes("UTF-8"))
    result(strip_resolve("."))
  }



}

