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

/**
 * Defines the default representations to be used on resources
 */
//@ImplicitProduces(Array("text/html;qs=5"))
//@Produces(Array("application/json", "application/xml","text/xml"))
@Consumes(Array(MediaType.APPLICATION_JSON))
@Produces(Array(MediaType.APPLICATION_JSON))
trait Resource extends Logging

@Path("/")
class Root() extends Resource {

  @BeanProperty
  var brokers: Array[Broker] = List(
    new Broker(this, "default"),
    new Broker(this, "example")
  ).toArray[Broker]

  @Path("{id}")
  def broker(@PathParam("id") id : String): Broker = {
    if( id == "default" ) {
      new Broker(this, id)
    } else {
      throw new NotFoundException("Broker " + id + " not found")
    }
  }
}

case class Broker(parent:Root, @BeanProperty id: String) extends Resource {
  @Path("config")
  def config =  BrokerConfig(this, id)
  @Path("status")
  def status = BrokerStatus(this, id)
}

case class BrokerConfig(parent:Broker, @BeanProperty id:String) extends Resource {

  @Path("virtual-hosts")
  def virtualHosts :List[VirtualHostConfig] = List(
    new VirtualHostConfig(this, "host1"),
    new VirtualHostConfig(this, "host2")
  )
  @Path("virtual-hosts/{id}")
  def virtualHost(@PathParam("id") id : String):VirtualHostConfig = null

  @Path("connectors")
  def connectors :List[ConnectorConfig] = null
  @Path("connectors/{id}")
  def connector(@PathParam("id") id : String):ConnectorConfig = null
}

case class BrokerStatus(parent:Broker, @BeanProperty id:String) extends Resource {
  @Path("virtual-hosts")
  def virtualHosts :List[VirtualHostStatus] = null
  @Path("virtual-host/{id}")
  def virtualHost(@PathParam("id") id : String):VirtualHostStatus = null

  @Path("connectors")
  def connectors :List[ConnectorStatus] = null
  @Path("connectors/{id}")
  def connector(@PathParam("id") id : String):ConnectorStatus = null

  @Path("connections")
  def connections :List[ConnectionStatus] = null
  @Path("connections/{id}")
  def connection(@PathParam("id") id : String):ConnectionStatus = null
}

case class VirtualHostConfig(parent:BrokerConfig, @BeanProperty id: String) extends Resource {
  var names:List[String] = null
}
case class VirtualHostStatus(parent:BrokerStatus, @BeanProperty id: String) extends Resource {
  var state:String = null
}

case class ConnectorConfig(parent:BrokerConfig, @BeanProperty id:String) extends Resource {
  var uri:String = null;
  var connect:String = null;
}
case class ConnectorStatus(parent:BrokerStatus, @BeanProperty id: String) extends Resource {
  var state:String = "unknown";
  var accepted:Long = 0;
}

case class ConnectionStatus(parent:BrokerStatus, @BeanProperty id:String) extends Resource {
  var state:String = "unknown";
  var readCounter:Long = 0;
  var writeCounter:Long = 0;
}
