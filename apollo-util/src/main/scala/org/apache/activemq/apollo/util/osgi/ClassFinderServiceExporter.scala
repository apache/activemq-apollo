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
package org.apache.activemq.apollo.util.osgi

import org.osgi.framework._
import java.lang.String
import collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import java.util.Properties
import scala.reflect.BeanProperty
import org.apache.activemq.apollo.util._

object ClassFinderServiceExporter extends Log

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ClassFinderServiceExporter {

  import ClassFinderServiceExporter._

  @BeanProperty
  var context: BundleContext = _

  @BeanProperty
  var service_path_base = "META-INF/services/org.apache.activemq.apollo/"

  def start: Unit = this.synchronized{
    val bundle = context.getBundle
    val paths = bundle.getEntryPaths(service_path_base)

    if( paths!=null ){
      paths.foreach { case path:String =>
        if(!path.endsWith("/xml-packages.index")) {
          val url = bundle.getEntry(path)
          val classNames = ListBuffer[String]()
          val p = ClassFinder.loadProperties(url.openStream)
          p.keys.foreach { next=>
            classNames += next.asInstanceOf[String]
          }

          classNames.distinct.foreach { name=>
            try {
              ClassFinder.instantiate(classOf[Object], bundle.loadClass(name)).foreach { service =>
                val p = new Properties
                p.put("finder_resource", path)
                context.registerService(service.getClass.getName, service, p)
              }
            } catch {
              case e:Throwable =>
                debug(e, "Could not load class %s", name)
            }
          }
        }
      }
    }
  }

  def stop: Unit = this.synchronized {
    // TODO: find out if OSGi, auto un-registers our services.
  }
}