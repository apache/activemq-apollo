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
package org.apache.activemq.apollo.web.resources;

import java.lang.String
import javax.ws.rs._
import reflect.{BeanProperty}
import org.apache.activemq.apollo.dto.{ConnectionStatusDTO, ConnectorStatusDTO, VirtualHostStatusDTO, BrokerStatusDTO}

/**
 * <p>
 * The BrokerStatus is the root container of the runtime status of a broker.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class BrokerStatus(parent:Broker, @BeanProperty id:String) extends Resource {
  @GET
  def get() = {
    val rc = new BrokerStatusDTO
    rc.id = id
    rc
  }

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
  @GET
  def get() = {
    val rc = new VirtualHostStatusDTO
    rc.id = id
    rc
  }
}

case class ConnectorStatus(parent:BrokerStatus, @BeanProperty id: String) extends Resource {

  @GET
  def get() = {
    val rc = new ConnectorStatusDTO
    rc.id = id
    rc
  }
}

case class ConnectionStatus(parent:BrokerStatus, @BeanProperty id:String) extends Resource {

  @GET
  def get() = {
    val rc = new ConnectionStatusDTO
    rc.id = id
    rc
  }
}
