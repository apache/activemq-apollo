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

import javax.ws.rs.core._
import javax.ws.rs.ext._
import javax.ws.rs._
import core.MediaType._
import core.Response.Status
import core.Response.Status._
import org.apache.activemq.apollo.dto.ErrorDTO
import javax.servlet.http.HttpServletRequest

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@Provider
class JaxrsExceptionMapper extends ExceptionMapper[Throwable] {

  @Context
  var http_request: HttpServletRequest = null

  def requested_uri = {
    val query = http_request.getQueryString
    http_request.getRequestURI + Option(query).map("?"+_).getOrElse("")
  }

  @Produces(Array(APPLICATION_JSON, APPLICATION_XML, TEXT_XML))
  def toResponse(error: Throwable): Response = {
    def response(status: Response.Status, msg: String=null) = {
      val response = Response.status(status)
      var dto = new ErrorDTO()
      dto.code = "%d: %s".format(status.getStatusCode, status.getReasonPhrase)
      dto.message = msg;
      dto.resource = requested_uri;
      response.entity(dto)
      response.build
    }

    error match {
      case ex:WebApplicationException =>
        if ( ! http_request.getServletPath.startsWith("/broker") ) {
          ex.getResponse
        } else {
          var code = ex.getResponse.getStatus
          if(code >= 400 && code != 401) {
            if(ex.getResponse.getStatus >= 500) {
              Resource.warn(ex, "HTTP request from '%s' for %s '%s' caused internal server error: %s", http_request.getRemoteAddr, http_request.getMethod, requested_uri, ex.toString);
            }
            var status = Status.fromStatusCode(ex.getResponse.getStatus)
            ex.getResponse.getEntity match {
              case null => response(status)
              case x:String => response(status, x)
              case _ => ex.getResponse
            }
          } else {
            ex.getResponse
          }
        }

      case ex:Throwable =>
        Resource.warn(ex, "HTTP request from '%s' for %s '%s' caused internal server error: %s", http_request.getRemoteAddr, http_request.getMethod, requested_uri, ex.toString);
        response(INTERNAL_SERVER_ERROR, ex.toString)
    }
  }
}