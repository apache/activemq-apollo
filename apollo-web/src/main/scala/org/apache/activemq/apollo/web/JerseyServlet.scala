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
package org.apache.activemq.apollo.web

import com.sun.jersey.spi.container.servlet.ServletContainer
import javax.servlet.ServletConfig
import javax.servlet.ServletContext
import java.util.Enumeration
import collection.mutable.HashMap

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class JerseyServlet extends ServletContainer {

  override def init(config: ServletConfig): Unit = {
    com.wordnik.swagger.jaxrs.JaxrsApiReader.setFormatString("")

    val settings = new HashMap[String, String]
    settings.put("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.ClassNamesResourceConfig")
    settings.put("com.sun.jersey.config.property.classnames", WebModule.web_resources.map(_.getName).mkString(" "))
    settings.put("com.sun.jersey.config.feature.Trace", System.getProperty("com.sun.jersey.config.feature.Trace", "false"))
    settings.put("com.sun.jersey.spi.container.ContainerRequestFilters", "com.sun.jersey.api.container.filter.PostReplaceFilter")
    settings.put("com.sun.jersey.config.feature.Redirect", "true")
    settings.put("com.sun.jersey.config.feature.FilterForwardOn404", "true")
    settings.put("com.sun.jersey.config.feature.ImplicitViewables", "true")
    settings.put("com.sun.jersey.config.property.MediaTypeMappings", """
        html : text/html,
        json : application/json
        """)

    super.init(CustomServletConfig(config.getServletName, config.getServletContext, settings))
  }

}

case class CustomServletConfig(name:String, context:ServletContext, custom_config_map:HashMap[String, String]) extends ServletConfig {
  def getServletName: String = name
  def getServletContext: ServletContext = context
  def getInitParameterNames: Enumeration[String] = {
    import collection.JavaConversions._
    return new java.util.Vector(custom_config_map.keys).elements();
  }
  def getInitParameter(s: String): String = {
    return custom_config_map.get(s).getOrElse(null)
  }
}

