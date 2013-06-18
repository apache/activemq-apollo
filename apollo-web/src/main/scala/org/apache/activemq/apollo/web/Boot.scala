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

import org.fusesource.scalate.TemplateEngine
import org.apache.activemq.apollo.broker.Broker

object Boot {
  @volatile
  var booted = false
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 *
 */
class Boot(engine: TemplateEngine) {


  // Put some references to the jersey classes in our code so that the osgi 
  // metadata creates the proper imports.
  val we_are_using = Array(
    classOf[com.sun.jersey.spi.container.servlet.ServletContainer]
  )
  
  def run: Unit = {
    Boot.booted = true
    engine.packagePrefix = "org.apache.activemq.apollo.web.templates"
  }
}