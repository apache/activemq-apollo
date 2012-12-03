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
import core._
import org.apache.activemq.apollo.broker._
import javax.servlet.ServletConfig
import com.wordnik.swagger.jaxrs.{Help, ApiListing}
import com.sun.jersey.spi.resource.Singleton
import com.wordnik.swagger.core.Documentation
import com.wordnik.swagger.annotations.ApiOperation

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@Path(       "/api{ext:(\\.json)?}")
@Produces(Array("application/json"))
class ApolloApiListing extends ApiListing {

  @ApiOperation(value = "Returns list of all available api endpoints", responseClass = "DocumentationEndPoint", multiValueResponse = true)
  @GET
  override def getAllApis(
    @Context sc: ServletConfig,
    @Context app: Application,
    @Context headers: HttpHeaders,
    @Context uriInfo: UriInfo): Response = {
    val response = super.getAllApis(sc, app, headers, uriInfo)
    var doc = response.getEntity.asInstanceOf[Documentation]
    doc.setApiVersion(Broker.version)
    doc.setBasePath(uriInfo.getAbsolutePath.resolve(".").toString.stripSuffix("/"))
    response
  }
}

/**
 * Jersey seems to have trouble getting annotations from traits.. so lets
 * make a concrete class out of the Help trait..
 */
@Singleton
@Produces(Array("application/json"))
class HelpResourceJSON extends Help {
  @GET
  @ApiOperation(value = "Returns information about API parameters",
    responseClass = "com.wordnik.swagger.core.Documentation")
  override def getHelp(@Context sc: ServletConfig,
    @Context headers: HttpHeaders,
    @Context uriInfo: UriInfo) = super.getHelp(sc, headers, uriInfo)
}