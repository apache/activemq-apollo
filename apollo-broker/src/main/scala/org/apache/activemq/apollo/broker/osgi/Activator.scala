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
package org.apache.activemq.apollo.osgi

import java.util.concurrent.ConcurrentHashMap
import org.osgi.framework._
import org.apache.activemq.apollo.broker.Broker
import org.apache.activemq.apollo.broker.store.Store
import org.apache.activemq.apollo.broker.protocol.Protocol
import org.apache.activemq.apollo.util.{ClassFinder, Log}
import java.net.URL
import collection.mutable.ListBuffer

/**
 * An OSGi bundle activator for Apollo which tracks Apollo extension
 * modules.
 */
object ApolloActivator extends Log {

  import ClassFinder._

  val bundles = new ConcurrentHashMap[Long, Bundle]

  case class BundleLoader(bundle:Bundle) extends Loader {
    def getResources(path:String) = bundle.getResources(path).asInstanceOf[java.util.Enumeration[URL]]
    def loadClass(name:String) = bundle.loadClass(name)
  }

  def osgi_loader():Array[Loader] = {
    val builder = ListBuffer[Loader]()
    import collection.JavaConversions._
    bundles.values.foreach { bundle=>
      builder += BundleLoader(bundle)
    }
    builder += ClassLoaderLoader(Thread.currentThread.getContextClassLoader)
    builder.toArray
  }


  // Change the default loaders used by the ClassFinder so
  // that it can find classes in extension osgi modules.
  ClassFinder.default_loaders = ()=> osgi_loader

}

class ApolloActivator extends BundleActivator with SynchronousBundleListener {

  import ApolloActivator._

  def start(bundleContext: BundleContext): Unit = this.synchronized {
    debug("activating")
    this.bundleContext = bundleContext
    debug("checking existing bundles")
    bundleContext.addBundleListener(this)
    for (bundle <- bundleContext.getBundles) {
      if (bundle.getState == Bundle.RESOLVED ||
          bundle.getState == Bundle.STARTING ||
          bundle.getState == Bundle.ACTIVE ||
          bundle.getState == Bundle.STOPPING) {
        register(bundle)
      }
    }
    debug("activated")
  }

  def stop(bundleContext: BundleContext): Unit = this.synchronized {
    debug("deactivating")
    bundleContext.removeBundleListener(this)
    while (!bundles.isEmpty) {
      unregister(bundles.keySet.iterator.next)
    }
    debug("deactivated")
    this.bundleContext = null
  }

  def bundleChanged(event: BundleEvent): Unit = {
    if (event.getType == BundleEvent.RESOLVED) {
      register(event.getBundle)
    }
    else if (event.getType == BundleEvent.UNRESOLVED || event.getType == BundleEvent.UNINSTALLED) {
      unregister(event.getBundle.getBundleId)
    }
  }

  protected def register(bundle: Bundle): Unit = {
    debug("checking bundle " + bundle.getBundleId)
    if (!isImportingUs(bundle)) {
      debug("The bundle does not import us: " + bundle.getBundleId)
    } else {
      bundles.put(bundle.getBundleId, bundle)
    }
  }

  /**
   * @param bundleId
   */
  protected def unregister(bundleId: Long): Unit = {
    bundles.remove(bundleId)
  }

  private def isImportingUs(bundle: Bundle): Boolean = {
    isImportingClass(bundle, classOf[Broker]) ||
    isImportingClass(bundle, classOf[Store]) ||
    isImportingClass(bundle, classOf[Protocol])
  }

  private def isImportingClass(bundle: Bundle, clazz: Class[_]): Boolean = {
    try {
      bundle.loadClass(clazz.getName) == clazz
    } catch {
      case e: ClassNotFoundException => {
        false
      }
    }
  }

  private var bundleContext: BundleContext = null
}