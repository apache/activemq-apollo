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

import org.osgi.framework._
import java.lang.String
import java.lang.ref._
import java.lang.ref.WeakReference
import collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import java.util.Properties

/**
 * An OSGi bundle activator for Apollo which tracks Apollo extension
 * modules.
 */
object ClassFinderImportingActivator extends Log with ClassFinder.Loader {

  private var bundleContext: BundleContext = null

  case class DiscoverRequest[T](path: String, clazz: Class[T], callback: (List[T]) => Unit)

  //
  // Used the track the GC's of the DiscoverRequests
  //
  val queue = new ReferenceQueue[DiscoverRequest[_]]()

  //
  // A service listener which un-registers when the DiscoverRequest gets GCed.
  //
  class DiscoverServiceListener[T](request:DiscoverRequest[T]) extends WeakReference[DiscoverRequest[T]](request, queue) with org.osgi.framework.ServiceListener {
    val path = request.path
    var classes = Set[T]()

    def serviceChanged(event: ServiceEvent): Unit = this.synchronized {
      val request = get
      if( request==null ) {
        dain_queue
        return;
      }

      val found = cast(request, event.getServiceReference).getOrElse(return)

      val original_size = classes.size
      if( event.getType == ServiceEvent.UNREGISTERING ) {
        classes -= found
      } else {
        classes += found
      }

      // if the set changed, then notify the callback.
      if( original_size!= classes.size) {
        request.callback(classes.toList)
      }
    }

    this.synchronized {
      bundleContext.addServiceListener(this, filter)
      classes = Option(bundleContext.getServiceReferences(null, filter)).getOrElse(Array()).flatMap { ref =>
        cast(request, ref)
      }.toSet
      request.callback(classes.toList)
    }


    def filter: String = "(finder_resource=" + path + ")"

    def cast(request:DiscoverRequest[T], ref:ServiceReference):Option[T] = {
      try {
        Some(request.clazz.cast(bundleContext.getService(ref)))
      } catch {
        case _ => None
      }
    }

  }


  def dain_queue:Unit = {
    var rc = queue.poll
    while( rc!=null ) {
      val dsl: DiscoverServiceListener[_] = rc.asInstanceOf[DiscoverServiceListener[_]]
      bundleContext.removeServiceListener(dsl)
      rc = queue.poll
    }
  }


  def discover[T](path: String, clazz: Class[T])(callback: (List[T]) => Unit) = {
    dain_queue
    new DiscoverServiceListener(DiscoverRequest(path, clazz, callback))
  }

  // Change the default loaders used by the ClassFinder so
  // that it can find classes using osgi
  ClassFinder.default_loader = ClassFinderImportingActivator

}

class ClassFinderImportingActivator extends BundleActivator {

  import ClassFinderImportingActivator._

  def start(bc: BundleContext): Unit = this.synchronized{
    debug("activating")
    bundleContext = bc
    debug("activated")
  }

  def stop(bc: BundleContext): Unit = this.synchronized{
    debug("deactivating")

    bundleContext = null
    debug("deactivated")
  }

}
object ClassFinderExportingActivator extends Log

class ClassFinderExportingActivator extends BundleActivator {

  import ClassFinderExportingActivator._

  var service_path_base = "META-INF/services/org.apache.activemq.apollo/"

  def start(bc: BundleContext): Unit = this.synchronized{
    val bundle = bc.getBundle
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
                bc.registerService(service.getClass.getName, service, p)
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

  def stop(bc: BundleContext): Unit = this.synchronized{
  }
}