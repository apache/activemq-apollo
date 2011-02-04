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

import com.sun.jersey.spi.container.servlet.ServletContainer
import javax.servlet._
import org.apache.activemq.apollo.broker.Broker

/**
 * Sets the broker's extension class loader as the context class loader
 * and then delegates to the jersey filter.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class JerseyServletContainer extends ServletContainer {
  override def init(filterConfig: FilterConfig): Unit = {
    println("jersey init: "+Thread.currentThread.getContextClassLoader)
/*    Thread.currentThread.setContextClassLoader(getClass.getClassLoader)*/
    super.init(filterConfig)
  }

  override def doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain): Unit = {
    super.doFilter(request, response, chain)
  }
}