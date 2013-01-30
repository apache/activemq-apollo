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
import javax.ws.rs.core.MediaType._
import org.apache.activemq.apollo.util.FutureResult
import FutureResult._
import com.wordnik.swagger.annotations.{ApiOperation, Api}
import org.apache.activemq.apollo.util._

case class EditConfig(file:String, config:String, can_write:Boolean)
case class ListConfigs(files:Array[String])

@Path(          "/api/json/broker/config")
@Api(value =    "/api/json/broker/config",
  listingPath = "/api/docs/broker/config")
@Produces(Array("application/json"))
class ConfigurationResourceJSON extends ConfigurationResource

@Path(          "/api/docs/broker/config{ext:(\\.json)?}")
@Api(value =    "/api/json/broker/config",
  listingPath = "/api/docs/broker/config",
  listingClass = "org.apache.activemq.apollo.web.resources.ConfigurationResourceJSON")
class ConfigurationResourceHelp extends HelpResourceJSON

@Path("/broker/config")
@Produces(Array(APPLICATION_JSON, APPLICATION_XML, TEXT_XML, "text/html;qs=5"))
class ConfigurationResourceHTML extends ConfigurationResource

/**
 * A broker resource is used to represent the configuration of a broker.
 */
class ConfigurationResource extends Resource {

  lazy val etc_directory = {
    val apollo_base = Option(System.getProperty("apollo.base")).getOrElse(result(NOT_FOUND))
    val rc = new File(apollo_base) / "etc"
    if ( !rc.exists() || !rc.isDirectory ) {
      result(NOT_FOUND)
    }
    rc
  }


  lazy val dto:BrokerDTO = with_broker { broker =>
    configing(broker) {
      broker.config
    }
  }

  def if_allowed[T](func: =>T):T = {
    with_broker[T]{ broker =>
      configing[T](broker) {
        func
      }
    }
  }

  @GET
  @Path("runtime")
//  @Produces(Array(APPLICATION_JSON, APPLICATION_XML, TEXT_XML))
//  @ApiOperation(value = "Returns a BrokerDTO object with runtime configuraiton of the broker.")
  def runtime = if_allowed {

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
  @Path("/files")
  @ApiOperation(value = "Returns the list of configuration files.")
  @Produces(Array(APPLICATION_JSON))
  def list() = if_allowed {
    etc_directory.listFiles().flatMap { file =>
      if( file.canRead ) {
        Some(file.getName)
      } else {
        None
      }
    }
  }

  @GET
  @Produces(Array(TEXT_HTML))
  @Path("/files")
  @ApiOperation(value = "Returns the list of configuration files.")
  def list_html() = if_allowed {
    ListConfigs(list())
  }

  @GET
  @Produces(Array(APPLICATION_OCTET_STREAM))
  @Path("/files/{name}")
  @ApiOperation(value = "Returns the contents of the configuration file.")
  def get(@PathParam("name") name:String) = if_allowed {
    val file = etc_directory / name
    if( !file.exists() || !file.canRead || file.getParentFile != etc_directory ) {
      result(NOT_FOUND)
    }
    file.read_bytes
  }

  @GET
  @Produces(Array(TEXT_HTML))
  @Path("/files/{name}")
  def edit_html(@PathParam("name") name:String) = if_allowed {
    val file = etc_directory / name
    if( !file.exists() || !file.canRead || file.getParentFile != etc_directory ) {
      result(NOT_FOUND)
    }
    EditConfig(name, file.read_text(), file.canWrite)
  }

  @POST
  @Produces(Array(APPLICATION_OCTET_STREAM))
  @Consumes(Array(APPLICATION_OCTET_STREAM))
  @Path("/files/{name}")
  @ApiOperation(value = "Updates the contents of the configuration file.")
  def put(@PathParam("name") name:String, config:Array[Byte]):Unit = if_allowed {
    val file = etc_directory / name
    if( !file.exists() || !file.canWrite || file.getParentFile != etc_directory ) {
      result(NOT_FOUND)
    }
    file.write_bytes(config)
    result(OK)
  }

  @POST
  @Path("/files/{name}")
  @Consumes(Array(APPLICATION_FORM_URLENCODED))
  @Produces(Array(APPLICATION_JSON, APPLICATION_XML,TEXT_XML, TEXT_HTML))
  def edit_post(@PathParam("name") name:String, @FormParam("config") config:String):Unit = if_allowed {
    put(name, config.getBytes("UTF-8"))
    result(strip_resolve("."))
  }



}

