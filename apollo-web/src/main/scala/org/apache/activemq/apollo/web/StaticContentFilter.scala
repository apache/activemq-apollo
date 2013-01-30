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
package org.apache.activemq.apollo.web

import javax.servlet._
import http.{HttpServletResponse, HttpServletRequest}
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.server.Dispatcher
import org.eclipse.jetty.util.URIUtil
import java.io.File
import org.eclipse.jetty.util.resource.Resource
import org.apache.activemq.apollo.util.FileSupport._

object StaticContentFilter {
  val external_resource_dirs = Option(System.getProperty("apollo.web.resources")).map { x=>
    x.split(""",""").toList.map{ x=>
      new File(x.trim).getCanonicalFile
    }
  }.getOrElse(List[File]())
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class StaticContentFilter extends Filter {
  import StaticContentFilter._

  val static_content_servlet = new DefaultServlet {
    override def getResource(path: String):Resource = {
      val rc = super.getResource(path);
      if ( rc ==null && !external_resource_dirs.isEmpty ) {
        for( dir <- external_resource_dirs ) {
          val file = (dir / path).getCanonicalFile
          if ( file.isFile && file.getPath.startsWith(dir.getPath) ) {
            return Resource.newResource(file)
          }
        }
      }
      rc
    }
  }

  def init(config: FilterConfig) {
    static_content_servlet.init(new ServletConfig {
      def getServletName = "default"
      def getServletContext = config.getServletContext
      def getInitParameterNames = new java.util.Vector().elements()
      def getInitParameter(p1: String) = null
    })
  }

  def destroy() {
    static_content_servlet.destroy()
  }

  def doFilter(req: ServletRequest, res: ServletResponse, chain: FilterChain) = {
    req match {
      case req: HttpServletRequest =>
        res match {
          case res: HttpServletResponse =>
            if (static_content_exists(req)) {
              static_content_servlet.service(req, res)
            } else {
              // not static content? then it must be dynamic, lets default
              // headers so that results are not cached by the browser.
              res.setHeader("Cache-Control", "max-age=0, no-cache, must-revalidate, proxy-revalidate, private")
              res.setHeader("Pragma", "no-cache")
              chain.doFilter(req, res)
            }
          case _ => chain.doFilter(req, res)
        }
      case _ => chain.doFilter(req, res)
    }
  }

  def static_content_exists(request: HttpServletRequest) = {

    var servletPath: String = null;
    var pathInfo: String = null;

    if (request.getAttribute(RequestDispatcher.INCLUDE_REQUEST_URI) != null) {
      servletPath = request.getAttribute(RequestDispatcher.INCLUDE_SERVLET_PATH).asInstanceOf[String]
      pathInfo = request.getAttribute(RequestDispatcher.INCLUDE_PATH_INFO).asInstanceOf[String]
      if (servletPath == null) {
        servletPath = request.getServletPath();
        pathInfo = request.getPathInfo();
      }
    } else {
      servletPath = request.getServletPath() // or we could do "/"
      pathInfo = request.getPathInfo();
    }

    val resourcePath = URIUtil.addPaths(servletPath, pathInfo);
    val resource = static_content_servlet.getResource(resourcePath)
    resource!=null && resource.exists() && !resource.isDirectory
  }
}
