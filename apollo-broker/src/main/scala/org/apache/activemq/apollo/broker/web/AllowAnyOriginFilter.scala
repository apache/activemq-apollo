/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.broker.web

import javax.servlet._
import http.{HttpServletRequest, HttpServletResponse}
import java.util.concurrent.TimeUnit._

/**
 * Servlet filter which adds a 'Access-Control-Allow-Origin: *' HTTP header
 * to allow CORS.
 */
class AllowAnyOriginFilter(val allowed:Set[String]) extends javax.servlet.Filter {

  val allow_any = allowed.contains("*")

  override def init(filterConfig: FilterConfig) {}
  override def destroy() = {}

  override def doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain):Unit = {
    response match {
      case response: HttpServletResponse =>
        val req = request.asInstanceOf[HttpServletRequest]
        val origin = request.asInstanceOf[HttpServletRequest].getHeader("Origin")

        if( allow_any || allowed.contains(origin) ) {

          if( origin !=null ) {
            response.addHeader("Access-Control-Allow-Origin", origin);
          } else {
            response.addHeader("Access-Control-Allow-Origin", "*");
          }

          if ( req.getMethod == "OPTIONS" ) {
            response.addHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE");
            req.getHeader("Access-Control-Request-Headers") match {
              case headers:String=> response.addHeader("Access-Control-Allow-Headers", headers);
              case _ =>
            }
            response.addHeader("Access-Control-Max-Age", ""+DAYS.toSeconds(1));
          }
        }

      case _ =>
    }
    chain.doFilter(request, response)
  }

}