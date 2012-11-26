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

  var original_config: ServletConfig = _
  var custom_config_map = HashMap[String, String]()
  val custom_config: ServletConfig = new ServletConfig {

    def getServletName: String = {
      return original_config.getServletName
    }

    def getServletContext: ServletContext = {
      return original_config.getServletContext
    }

    def getInitParameterNames: Enumeration[String] = {
      import collection.JavaConversions._
      return new java.util.Vector(custom_config_map.keys).elements();
    }

    def getInitParameter(s: String): String = {
      return custom_config_map.get(s).getOrElse(null)
    }

  }

  override def init(config: ServletConfig): Unit = {
    com.wordnik.swagger.jaxrs.JaxrsApiReader.setFormatString("")

    original_config = config
    custom_config_map.put("com.sun.jersey.config.property.resourceConfigClass", "com.sun.jersey.api.core.ClassNamesResourceConfig")
    custom_config_map.put("com.sun.jersey.config.property.classnames", WebModule.web_resources.map(_.getName).mkString(" "))
    custom_config_map.put("com.sun.jersey.config.feature.Trace", System.getProperty("com.sun.jersey.config.feature.Trace", "false"))
    custom_config_map.put("com.sun.jersey.spi.container.ContainerRequestFilters", "com.sun.jersey.api.container.filter.PostReplaceFilter")
    custom_config_map.put("com.sun.jersey.config.feature.Redirect", "true")
    custom_config_map.put("com.sun.jersey.config.feature.FilterForwardOn404", "true")
    custom_config_map.put("com.sun.jersey.config.feature.ImplicitViewables", "true")
    custom_config_map.put("com.sun.jersey.config.property.MediaTypeMappings", """
        html : text/html,
        xml : application/xml,
        json : application/json
        """)

    super.init(custom_config)
  }

}