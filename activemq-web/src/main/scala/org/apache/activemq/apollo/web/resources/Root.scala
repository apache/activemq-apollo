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
import org.fusesource.scalate.rest.{Container, ContainerResource}
import com.sun.jersey.api.NotFoundException
import javax.ws.rs._
import core.MediaType
import org.fusesource.scalate.util.Logging
import reflect.{BeanProperty, BeanInfo}
import org.codehaus.jackson.annotate.JsonProperty
import com.sun.jersey.api.view.ImplicitProduces
import org.apache.activemq.apollo.jaxb.BrokerConfig

/**
 * Defines the default representations to be used on resources
 */
@ImplicitProduces(Array("text/html;qs=5"))
@Produces(Array("application/json", "application/xml","text/xml"))
trait Resource extends Logging {
}

@Path("/")
class Root() extends Resource {

  @GET
  def get() = this

  @BeanProperty
  var brokers: Array[Broker] = Array(
    new Broker(this, "default"),
    new Broker(this, "example")
  )

  @Path("{id}")
  def broker(@PathParam("id") id : String): Broker = {
    if( id == "default" || id=="example" ) {
      new Broker(this, id)
    } else {
      throw new NotFoundException("Broker " + id + " not found")
    }
  }
}

case class Broker(parent:Root, @BeanProperty @BeanProperty id: String) extends Resource {
  @GET
  def get():AnyRef = {
    if( id == "default") {
      val config = new BrokerConfig();
      config.setName(id);
      return config
    } else {
      return null;
    }
  }

  @Path("status")
  def status = BrokerStatus(this, id)
}

case class BrokerStatus(parent:Broker, @BeanProperty id:String) extends Resource {
  @GET
  def get() = this

  @Path("virtual-hosts")
  def virtualHosts :Array[VirtualHostStatus] = null
  @Path("virtual-hosts/{id}")
  def virtualHost(@PathParam("id") id : String):VirtualHostStatus = null

  @Path("connectors")
  def connectors :Array[ConnectorStatus] = null
  @Path("connectors/{id}")
  def connector(@PathParam("id") id : String):ConnectorStatus = null

  @Path("connections")
  def connections :Array[ConnectionStatus] = null
  @Path("connections/{id}")
  def connection(@PathParam("id") id : String):ConnectionStatus = null
}

case class VirtualHostStatus(parent:BrokerStatus, @BeanProperty id: String) extends Resource {
  var state:String = null
  @GET
  def get() = this
}

case class ConnectorStatus(parent:BrokerStatus, @BeanProperty id: String) extends Resource {
  var state:String = "unknown";
  var accepted:Long = 0;
  @GET
  def get() = this
}

case class ConnectionStatus(parent:BrokerStatus, @BeanProperty id:String) extends Resource {
  var state:String = "unknown";
  var readCounter:Long = 0;
  var writeCounter:Long = 0;
  @GET
  def get() = this
}
