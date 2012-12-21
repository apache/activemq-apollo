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

import java.{lang => jl}
import org.apache.activemq.apollo.util._
import javax.ws.rs._
import core.Context
import javax.ws.rs.core.Response.Status._
import javax.servlet.http.HttpServletResponse
import java.util.ArrayList
import java.security.Principal
import org.apache.activemq.apollo.dto._
import javax.ws.rs.core.MediaType._
import org.apache.activemq.apollo.broker.security.SecurityContext
import FutureResult._
import com.wordnik.swagger.annotations._
import org.apache.activemq.apollo.util.Success

@Path(          "/api/json/session")
@Api(value =    "/api/json/session",
  listingPath = "/api/docs/session")
@Produces(Array("application/json"))
class SessionResourceJSON extends SessionResource

@Path(          "/api/docs/session{ext:(\\.json)?}")
@Api(value =    "/api/json/session",
  listingPath = "/api/docs/session",
  listingClass = "org.apache.activemq.apollo.web.resources.SessionResourceJSON")
class SessionResourceHelp extends HelpResourceJSON

@Path("/session")
@Produces(Array("text/html"))
class SessionResourceHTML extends SessionResource

/**
 * <p>
 * Used to manage user web sessions.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class SessionResource extends Resource {
  import Resource._

  @GET
  @Path("/whoami")
  @ApiOperation(value = "Returns the Signed-in user's pricipals")
  def whoami():java.util.List[PrincipalDTO] = {
    val rc: Set[Principal] = with_broker { broker =>
      val rc = FutureResult[Set[Principal]]()
      if(broker.authenticator!=null) {
        authenticate(broker.authenticator) { security_context =>
          if(security_context!=null) {
            rc.set(Success(security_context.principals))
          } else {
            rc.set(Success(Set[Principal]()))
          }
        }
      } else {
        rc.set(Success(Set[Principal]()))
      }
      rc
    }
    import collection.JavaConversions._
    new ArrayList[PrincipalDTO](rc.map(x=>new PrincipalDTO(x.getClass.getName, x.getName)))
  }

  @Produces(Array("text/html;qs=5"))
  @GET
  @Path("/signin")
  @ApiOperation(value = "Signin a user")
  @ApiErrors(Array(
    new ApiError(code = 400, reason = "Invalid user id or password")
  ))
  def get_signin_html(@Context response:HttpServletResponse,
                      @ApiParam(value = "User Name", defaultValue="admin")
                      @QueryParam("username") username:String,
                      @ApiParam(value = "User Password", defaultValue="password")
                      @QueryParam("password") password:String,
                      @ApiParam(value = "On success, redirect to the specified URL", defaultValue = "/")
                      @QueryParam("target") target:String
                             ): ErrorDTO = {
    if(post_signin(response, username, password)) {
      result(strip_resolve(Option(target).getOrElse("/")))
    } else {
      var dto = new ErrorDTO()
      dto.code = "%d: %s".format(BAD_REQUEST.getStatusCode, BAD_REQUEST.getReasonPhrase)
      dto.message = "Invalid user id or password";
      result(BAD_REQUEST, dto)
    }
  }

  @POST
  @Path("/signin")
  @ApiOperation(value = "Signin a user")
  def post_signin(@Context response:HttpServletResponse,
                  @ApiParam(value = "User Name")
                  @FormParam("username") username:String,
                  @ApiParam(value = "User Password")
                  @FormParam("password") password:String):Boolean =  {
    try {
      val user_info = UserInfo(username, password)
      http_request.setAttribute("user_info", user_info)
      unwrap_future_result[Boolean] {
        with_broker { broker =>
          monitoring(broker) {
            // Only create the session if he is a valid user.
            val session = http_request.getSession(true)
            user_info.security_context = http_request.getAttribute(SECURITY_CONTEXT_ATTRIBUTE).asInstanceOf[SecurityContext]
            session.setAttribute("user_info", user_info)
            true
          }
        }
      }
    } catch {
      case e:WebApplicationException => // this happens if user is not authorized
        false
    }
  }

  @Produces(Array("text/html"))
  @GET @Path("/signout")
  @ApiOperation(value = "Signout a user")
  def signout_html():String = {
    signout()
    result(strip_resolve("../.."))
    ""
  }

  @Produces(Array(APPLICATION_JSON, APPLICATION_XML, TEXT_XML))
  @GET @Path("/signout")
  @ApiOperation(value = "Signout a user")
  def signout():String =  {
    val session = http_request.getSession(false)
    if( session !=null ) {
      session.invalidate();
    }
    ""
  }

}
