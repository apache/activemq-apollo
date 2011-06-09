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
package org.apache.activemq.apollo.util

import java.lang.String
import scala.collection.mutable.ListBuffer

object Module {
  val MODULE_INDEX_RESOURCE: String = "META-INF/services/org.apache.activemq.apollo/modules.index"
}
import Module._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract class Module {
  def xml_packages:Array[String] = Array()
  def web_resources:Map[String, ()=>AnyRef] = Map()
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ModuleRegistry {

  val finder = new ClassFinder[Module](MODULE_INDEX_RESOURCE,classOf[Module])

  def singletons = finder.singletons
  def jsingletons = finder.jsingletons

  private val listeners = ListBuffer[Runnable]()

  finder.on_change = ()=> {
    val copy = this.synchronized {
      listeners.toArray
    }
    copy.foreach { listener=>
      listener.run
    }
  }
}

