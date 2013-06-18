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

import org.apache.activemq.apollo.util.ClassFinder
import collection.immutable.TreeMap
import scala.collection.mutable.{ListBuffer, LinkedHashMap}
import resources._
import org.fusesource.scalate.jersey._
import com.wordnik.swagger.jaxrs.ApiHelpMessageBodyWriter

trait WebModule {
  def priority = 50
  def web_resources: Set[Class[_]]
  def root_redirect:String = null
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object WebModule {

  val finder = new ClassFinder[WebModule]("META-INF/services/org.apache.activemq.apollo/web-module.index",classOf[WebModule])

  val (root_redirect, web_resources) = {
    // sort by priority.  Highest priority wins.
    var sorted = finder.singletons.sortBy( _.priority )
    val web_resources = LinkedHashMap[Class[_], Class[_]]()
    for( provider <- sorted; resource <- provider.web_resources ) {
      web_resources.put(resource,resource)
    }
    val last_root_redirect = (sorted.flatMap{x=> Option(x.root_redirect)}).last
    (last_root_redirect, web_resources.keySet)
  }

}

object DefaultWebModule extends WebModule {

  override def priority: Int = 100

  override def web_resources = {
    var rc = ListBuffer[Class[_]]()

    // HTML is representation only available if Scalate is enabled in the webapp.
    if( Boot.booted ) {
      rc += classOf[BrokerResourceHTML]
      rc += classOf[SessionResourceHTML]
      rc += classOf[ConfigurationResourceHTML]
      rc += classOf[ScalateTemplateProvider]
      rc += classOf[ScalateTemplateProcessor]
    }
    rc += classOf[RootResource]
    rc += classOf[ApolloApiListing]
    rc += classOf[ApiHelpMessageBodyWriter]
    rc += classOf[BrokerResourceJSON]
    rc += classOf[BrokerResourceHelp]
    rc += classOf[SessionResourceJSON]
    rc += classOf[SessionResourceHelp]
    rc += classOf[ConfigurationResourceJSON]
    rc += classOf[ConfigurationResourceHelp]
    rc += classOf[JacksonJsonProvider]
    rc += classOf[JaxrsExceptionMapper]
    rc.toSet
  }

  override def root_redirect: String = {

    "console/index.html"
  }

}