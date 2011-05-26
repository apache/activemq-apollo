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
import com.sun.jersey.api.NotFoundException
import javax.ws.rs._
import core.{UriInfo, Response, Context}
import reflect.{BeanProperty}
import com.sun.jersey.api.view.ImplicitProduces
import Response._
import Response.Status._
import collection.JavaConversions._
import com.sun.jersey.api.core.ResourceContext
import java.util.concurrent.TimeUnit
import org.apache.activemq.apollo.dto._
import java.util.{Arrays, Collections}
import org.fusesource.hawtdispatch._
import java.net.URI
import org.fusesource.scalate.{NoValueSetException, RenderContext}
import com.sun.jersey.core.util.Base64
import javax.servlet.http.{HttpServletResponse, HttpServletRequest}
import java.io.{IOException, UnsupportedEncodingException}
import org.apache.activemq.apollo.broker.security.{Authorizer, SecurityContext, Authenticator}
import org.apache.activemq.apollo.broker._
import java.lang.reflect.Proxy
import com.sun.jersey.server.impl.ThreadLocalInvoker
import util.continuations._
import org.apache.activemq.apollo.util.Success._
import org.apache.activemq.apollo.util.Failure._
import org.apache.activemq.apollo.util._
import javax.management.remote.rmi._RMIConnection_Stub

object Resource {

  val SECURITY_CONTEXT_ATTRIBUTE: String = classOf[SecurityContext].getName
  val HEADER_WWW_AUTHENTICATE: String = "WWW-Authenticate"
  val HEADER_AUTHORIZATION: String = "Authorization"
  val AUTHENTICATION_SCHEME_BASIC: String = "Basic"

  private def decode_base64(value: String): String = {
    var transformed: Array[Byte] = Base64.decode(value)
    try {
      return new String(transformed, "ISO-8859-1")
    } catch {
      case uee: UnsupportedEncodingException => {
        return new String(transformed)
      }
    }
  }

}

/**
 * Defines the default representations to be used on resources
 */
@ImplicitProduces(Array("text/html;qs=5"))
@Produces(Array("application/json", "application/xml","text/xml"))
abstract class Resource(parent:Resource=null) extends Logging {
  import Resource._

  @Context
  var uri_info:UriInfo = null
  @Context
  var http_request: HttpServletRequest = null

  if( parent!=null ) {
    copy(parent)
  }

  def copy(other:Resource) = {
    this.uri_info = other.uri_info
    this.http_request = other.http_request
  }

  def result(value:Status, message:Any=null):Nothing = {
    val response = Response.status(value)
    if( message!=null ) {
      response.entity(message)
    }
    throw new WebApplicationException(response.build)
  }

  def result[T](uri:URI):T = {
    throw new WebApplicationException(seeOther(uri).build)
  }

  def path(value:Any) = uri_info.getAbsolutePathBuilder().path(value.toString).build()

  def strip_resolve(value:String) = {
    new URI(uri_info.getAbsolutePath.resolve(value).toString.stripSuffix("/"))
  }


  protected def authorize[T](authenticator:Authenticator, authorizer:Authorizer, block: =>FutureResult[T])(func: (Authorizer, SecurityContext)=>Boolean):FutureResult[T] = {
    if ( authenticator != null ) {
      val rc = FutureResult[T]()
      authenticate(authenticator) { security_context =>
        try {
          if (security_context != null) {
            if (authorizer == null) {
              block.onComplete(rc)
            } else {
              if (func(authorizer, security_context)) {
                block.onComplete(rc)
              } else {
                unauthroized
              }
            }
          } else {
            unauthroized
          }
        } catch {
          case e:Throwable =>
            rc.apply(Failure(e))
        }
      }
      rc
    } else {
      block
    }
  }

  protected def monitoring[T](broker:Broker)(func: =>FutureResult[T]):FutureResult[T] = {
    authorize(broker.authenticator, broker.authorizer, func) {  _.can_monitor(_, broker) }
  }

  protected def admining[T](broker:Broker)(func: =>FutureResult[T]):FutureResult[T] = {
    authorize(broker.authenticator, broker.authorizer, func) {  _.can_admin(_, broker) }
  }

  protected def admining[T](host:VirtualHost)(func: =>FutureResult[T]):FutureResult[T] = {
    authorize(host.authenticator, host.authorizer, func) {  _.can_admin(_, host) }
  }
  protected def monitoring[T](host:VirtualHost)(func: =>FutureResult[T]):FutureResult[T] = {
    authorize(host.authenticator, host.authorizer, func) {  _.can_monitor(_, host) }
  }

  protected def admining[T](dest:Queue)(func: =>FutureResult[T]):FutureResult[T] = {
    authorize(dest.virtual_host.authenticator, dest.virtual_host.authorizer, func) {  _.can_admin(_, dest.virtual_host, dest.config) }
  }
  protected def monitoring[T](dest:Queue)(func: =>FutureResult[T]):FutureResult[T] = {
    authorize(dest.virtual_host.authenticator, dest.virtual_host.authorizer, func) {  _.can_monitor(_, dest.virtual_host, dest.config) }
  }

  protected def admining[T](dest:Topic)(func: =>FutureResult[T]):FutureResult[T] = {
    authorize(dest.virtual_host.authenticator, dest.virtual_host.authorizer, func) {  _.can_admin(_, dest.virtual_host, dest.config) }
  }
  protected def monitoring[T](dest:Topic)(func: =>FutureResult[T]):FutureResult[T] = {
    authorize(dest.virtual_host.authenticator, dest.virtual_host.authorizer, func) {  _.can_monitor(_, dest.virtual_host, dest.config) }
  }

  protected def authenticate[T](authenticator:Authenticator)(func: (SecurityContext)=>Unit): Unit = {

    var security_context = http_request.getAttribute(SECURITY_CONTEXT_ATTRIBUTE).asInstanceOf[SecurityContext]
    if( security_context!=null ) {
      func(security_context)
    } else {
      security_context = new SecurityContext
      security_context.local_address = http_request.getLocalAddr+":"+http_request.getLocalPort
      security_context.remote_address = http_request.getRemoteAddr+":"+http_request.getRemotePort

      var auth_header = http_request.getHeader(HEADER_AUTHORIZATION)
      if (auth_header != null && auth_header.length > 0) {
        auth_header = auth_header.trim
        var blank = auth_header.indexOf(' ')
        if (blank > 0) {
          var auth_type = auth_header.substring(0, blank)
          var auth_info = auth_header.substring(blank).trim
          if (auth_type.equalsIgnoreCase(AUTHENTICATION_SCHEME_BASIC)) {
            try {
              var srcString = decode_base64(auth_info)
              var i = srcString.indexOf(':')
              var username: String = srcString.substring(0, i)
              var password: String = srcString.substring(i + 1)


//            connection.transport match {
//              case t:SslTransport=>
//                security_context.certificates = Option(t.getPeerX509Certificates).getOrElse(Array[X509Certificate]())
//              case _ => None
//            }
              security_context.user = username
              security_context.password = password

            } catch {
              case e: Exception =>
            }
          }
        }
      }

      reset {
        if( authenticator.authenticate(security_context) ) {
          http_request.setAttribute(SECURITY_CONTEXT_ATTRIBUTE, security_context)
          func(security_context)
        } else {
          func(null)
        }
      }
    }
  }

  protected def unauthroized = {
    // TODO: perhaps get the realm from the authenticator
    var http_realm = "Apollo"
    throw new WebApplicationException(Response.
      status(HttpServletResponse.SC_UNAUTHORIZED).
      header(HEADER_WWW_AUTHENTICATE, AUTHENTICATION_SCHEME_BASIC + " realm=\"" + http_realm + "\"").
      build())
  }

  type FutureResult[T] = Future[Result[T, Throwable]]

  protected def FutureResult[T]() = Future[Result[T, Throwable]]()

  protected def sync[T](dispached:Dispatched)(func: =>FutureResult[T]):FutureResult[T] = {
    val rc = Future[Result[T, Throwable]]()
    dispached.dispatch_queue.apply {
      try {
        func.onComplete(x=> rc.apply(x))
      } catch {
        case e:Throwable => rc.apply(Failure(e))
      }
    }
    rc
  }


  protected def sync_all[T,D<:Dispatched](values:Iterable[D])(func: (D)=>FutureResult[T]) = {
    Future.all {
      values.map { value=>
        sync(value) {
          func(value)
        }
      }
    }
  }

  protected implicit def to_local_router(host:VirtualHost):LocalRouter = {
    host.router.asInstanceOf[LocalRouter]
  }

  protected implicit def wrap_future_result[T](value:T):FutureResult[T] = {
    val rc = FutureResult[T]()
    rc.apply(Success(value))
    rc
  }

  protected implicit def unwrap_future_result[T](value:FutureResult[T]):T = {
    value.await() match {
      case Success(value) => value
      case Failure(value) => throw value
    }
  }

  protected def with_broker[T](func: (org.apache.activemq.apollo.broker.Broker)=>FutureResult[T]):FutureResult[T] = {
    BrokerRegistry.list.headOption match {
      case Some(broker)=>
        sync(broker) {
          func(broker)
        }
      case None=>
        result(NOT_FOUND)
    }
  }

  protected def with_virtual_host[T](id:String)(func: (VirtualHost)=>FutureResult[T]):FutureResult[T] = {
    with_broker { broker =>
      broker.virtual_hosts.valuesIterator.find( _.id == id) match {
        case Some(virtualHost)=>
          sync(virtualHost) {
            func(virtualHost)
          }
        case None=>
          result(NOT_FOUND)
      }
    }
  }

  protected def with_connection[T](id:Long)(func: BrokerConnection=>FutureResult[T]):FutureResult[T] = {
    with_broker { broker =>
      broker.connectors.flatMap{ _.connections.get(id) }.headOption match {
        case Some(connection:BrokerConnection) =>
          sync(connection) {
            func(connection)
          }
        case None=>
          result(NOT_FOUND)
      }
    }
  }

}

object ViewHelper {

  val KB: Long = 1024
  val MB: Long = KB * 1024
  val GB: Long = MB * 1024
  val TB: Long = GB * 1024

  val SECONDS: Long = TimeUnit.SECONDS.toMillis(1)
  val MINUTES: Long = TimeUnit.MINUTES.toMillis(1)
  val HOURS: Long = TimeUnit.HOURS.toMillis(1)
  val DAYS: Long = TimeUnit.DAYS.toMillis(1)
  val YEARS: Long = DAYS * 365


}
class ViewHelper {
  import ViewHelper._

  lazy val uri_info = {
    try {
      RenderContext().attribute[UriInfo]("uri_info")
    } catch {
      case x:NoValueSetException =>
        RenderContext().attribute[Resource]("it").uri_info
    }
  }

  def path(value:Any) = {
    uri_info.getAbsolutePathBuilder().path(value.toString).build()
  }

  def strip_resolve(value:String) = {
    uri_info.getAbsolutePath.resolve(value).toString.stripSuffix("/")
  }


  def memory(value:Int):String = memory(value.toLong)
  def memory(value:Long):String = {

    if( value < KB ) {
      "%d bytes".format(value)
    } else if( value < MB ) {
       "%,.2f kb".format(value.toFloat/KB)
    } else if( value < GB ) {
      "%,.3f mb".format(value.toFloat/MB)
    } else if( value < TB ) {
      "%,.4f gb".format(value.toDouble/GB)
    } else {
      "%,.5f tb".format(value.toDouble/TB)
    }
  }

  def uptime(value:Long):String = {
    def friendly(duration:Long):String = {
      if( duration < SECONDS ) {
        "%d ms".format(duration)
      } else if (duration < MINUTES) {
        "%d seconds".format(duration / SECONDS)
      } else if (duration < HOURS) {
        "%d minutes".format(duration / MINUTES)
      } else if (duration < DAYS) {
        "%d hours %s".format(duration / HOURS, friendly(duration%HOURS))
      } else if (duration < YEARS) {
        "%d days %s".format(duration / DAYS, friendly(duration%DAYS))
      } else {
        "%,d years %s".format(duration / YEARS, friendly(duration%YEARS))
      }
    }
    friendly(System.currentTimeMillis - value)
  }
}

