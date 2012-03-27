package org.apache.activemq.apollo.web

import javax.servlet._
import http.{HttpServletResponse, HttpServletRequest}
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.server.Dispatcher
import org.eclipse.jetty.util.URIUtil


/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class StaticContentFilter extends Filter {

  val static_content_servlet = new DefaultServlet

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

    if (request.getAttribute(Dispatcher.INCLUDE_REQUEST_URI) != null) {
      servletPath = request.getAttribute(Dispatcher.INCLUDE_SERVLET_PATH).asInstanceOf[String]
      pathInfo = request.getAttribute(Dispatcher.INCLUDE_PATH_INFO).asInstanceOf[String]
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
