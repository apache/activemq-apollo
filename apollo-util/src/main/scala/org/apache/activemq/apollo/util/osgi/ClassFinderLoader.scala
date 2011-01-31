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

import java.lang.Class
import org.osgi.framework._
import java.lang.String
import java.lang.ref._
import java.lang.ref.WeakReference
import org.yaml.snakeyaml.Yaml
import java.io.{FileWriter, FileInputStream, File}
import java.util.concurrent.CountDownLatch
import org.apache.activemq.apollo.util._
import FileSupport._
import collection.JavaConversions._

/**
 * An OSGi aware ClassFinder.Loader.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ClassFinderLoader extends Log with ClassFinder.Loader {

  var context: BundleContext = _
  val init_complete_latch = new CountDownLatch(1);

  val yaml = new Yaml

  def extensions_file:File = context.getDataFile("extensions.yaml")

  var extension_bundles = Set[Long]()

  def init = this.synchronized {

    ClassFinder.default_loader = ClassFinderLoader

    if( extensions_file.exists ) {

      extension_bundles = using(new FileInputStream(extensions_file)) { is =>
        yaml.load(is).asInstanceOf[java.util.List[Any]].flatMap{ _ match {
          case x:Int=> Some(x.toLong)
          case x:Long=> Some(x)
          case _ => None
          }
        }.toSet

      }

      def pending_bundles = {
        extension_bundles.flatMap { x =>
          context.getBundle(x) match {
            case null => // must have been un-installed.
              extension_bundles -= x
              write_extensions_file
              None
            case bundle =>
              if( bundle.getState == Bundle.ACTIVE ) {
                None
              } else {
                Some(bundle.getSymbolicName)
              }
          }
        }
      }

      // wait for all the pending bundles to start up..
      debug("Will wait for extension bundles to start: %s", extension_bundles)
      var p = pending_bundles
      while(!p.isEmpty) {
        debug("waiting on extension bundles to start: %s", p)
        this.wait(100)
        p = pending_bundles
      }
      debug("All extension bundles to started")

    } else {
      debug("First start, wait a bit to discover the installed extension bundles..")
      // first time we boot up sleep for a bit to allow extensions
      // to register..
      this.wait(2000)
      write_extensions_file
    }
    init_complete_latch.countDown

  }

  def write_extensions_file = this.synchronized {
    using(new FileWriter(extensions_file)) { os =>
      debug("storing extension bundles: %s", extension_bundles)
      yaml.dump(asJavaList(extension_bundles.toSeq), os)
    }
  }

  def register_extension_bundle(bundle:Bundle) = this.synchronized {
    if( !extension_bundles.contains(bundle.getBundleId) ) {
      debug("New extension bundle registered: "+bundle.getBundleId)
      extension_bundles += bundle.getBundleId
      write_extensions_file
    }
  }

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
        register_extension_bundle(event.getServiceReference.getBundle)
      }

      // if the set changed, then notify the callback.
      if( original_size!= classes.size) {
        request.callback(classes.toList)
      }
    }

    this.synchronized {
      context.addServiceListener(this, filter)
      val refs: Array[ServiceReference] = context.getServiceReferences(null, filter)
      classes = Option(refs).getOrElse(Array()).flatMap { ref =>
        val rc = cast(request, ref)
        rc.foreach(x=> register_extension_bundle(ref.getBundle) )
        rc
      }.toSet

      request.callback(classes.toList)
    }


    def filter: String = "(finder_resource=" + path + ")"

    def cast(request:DiscoverRequest[T], ref:ServiceReference):Option[T] = {
      try {
        Some(request.clazz.cast(context.getService(ref)))
      } catch {
        case _ => None
      }
    }

  }


  def dain_queue:Unit = {
    var rc = queue.poll
    while( rc!=null ) {
      val dsl: DiscoverServiceListener[_] = rc.asInstanceOf[DiscoverServiceListener[_]]
      context.removeServiceListener(dsl)
      rc = queue.poll
    }
  }


  def discover[T](path: String, clazz: Class[T])(callback: (List[T]) => Unit) = {
    // block requests until init completes.
    init_complete_latch.await
    dain_queue
    new DiscoverServiceListener(DiscoverRequest(path, clazz, callback))
  }

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ClassFinderLoader {

  def setContext(value:BundleContext):Unit = ClassFinderLoader.context = value

  def start() = ClassFinderLoader.init
  def stop() = {}

}