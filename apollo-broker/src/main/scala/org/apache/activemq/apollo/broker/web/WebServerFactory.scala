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
package org.apache.activemq.apollo.broker.web

import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.broker.Broker
import org.fusesource.hawtdispatch.Task
import java.net.URI

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait WebServer extends Service {
  def update(on_complete:Task):Unit
  def uris():Array[URI]
}

trait WebServerFactory {
  def create(broker:Broker):WebServer
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object WebServerFactory {

  val finder = new ClassFinder[WebServerFactory]("META-INF/services/org.apache.activemq.apollo/web-server-factory.index",classOf[WebServerFactory])

  def create(broker:Broker):WebServer = {
    if( broker == null ) {
      return null
    }
    finder.singletons.foreach { provider=>
      val rc = provider.create(broker)
      if( rc!=null ) {
        return rc
      }
    }
    null
  }

}
