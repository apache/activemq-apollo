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

import java.io.InputStream
import java.util.Properties
import scala.collection.mutable.ListBuffer
import collection.JavaConversions._


object ClassFinder extends Log {

  var class_loader:ClassLoader = Option(ClassFinder.getClass.getClassLoader).getOrElse(ClassLoader.getSystemClassLoader)

  trait Loader {
    def discover[T](path:String, clazz: Class[T])( callback: List[T]=>Unit )
    def load[T](name: String, clazz: Class[T]): T
  }

  case class ClassLoaderLoader(loaders: Seq[ClassLoader]) extends Loader {
    def discover[T](path: String, clazz: Class[T])(callback: (List[T]) => Unit) = {
      val classes = ListBuffer[Class[_]]()
      loaders.foreach { loader=>
        val resources = loader.getResources(path)
        val classNames =  ListBuffer[String]()
        while(resources.hasMoreElements) {
          val p = loadProperties(resources.nextElement.openStream)
          p.keys.foreach { next =>
            classNames += next.asInstanceOf[String]
          }
        }
        classNames.distinct.foreach { name=>
          try {
            classes += loader.loadClass(name)
          } catch {
            case e:Throwable =>
              debug(e, "Could not load class %s using class loader: %s", name, loader)
          }
        }
      }
      val singltons = classes.flatMap(x=> instantiate(clazz, x) ).distinct
      callback( singltons.toList )
    }

    def load[T](name: String, clazz: Class[T]): T = {
      loaders.foreach { loader=>
        instantiate(clazz, loader.loadClass(name)) match {
          case Some(rc)=> return rc
          case None =>
        }
      }
      throw new ClassNotFoundException(name)
    }
  }

  def instantiate[T](target:Class[T], clazz:Class[_]) = {
    try {
      Some(target.cast(clazz.newInstance))
    } catch {
      case e: Throwable =>
        // It may be a scala object.. check for a module class
        try {
          val moduleField = clazz.getClassLoader.loadClass(clazz.getName + "$").getDeclaredField("MODULE$")
          Some(moduleField.get(null).asInstanceOf[T])
        } catch {
          case e2: Throwable =>
            debug(e, "Could not create an instance of '%s' using classloader %s", clazz.getName, clazz.getClassLoader)
            None
        }
    }
  }

  var default_loader:Loader = ClassLoaderLoader(Array(class_loader))

  def loadProperties(is:InputStream):Properties = {
    if( is==null ) {
      return null;
    }
    try {
      val p = new Properties()
      p.load(is);
      return p
    } catch {
      case e:Exception =>
      return null
    } finally {
      try {
        is.close()
      } catch {
        case _:Throwable =>
      }
    }
  }
}



/**
 * <p>
 * Used to discover classes using the META-INF discovery trick.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ClassFinder[T](val path:String, val clazz: Class[T]) {

  import ClassFinder._

  @volatile
  var singletons = List[T]()
  var on_change = ()=>{}

  var loader:Loader=default_loader

  loader.discover(path, clazz) { x=>
    singletons = x
    on_change()
  }

  def jsingletons = {
    import collection.JavaConversions._
    seqAsJavaList(singletons)
  }

}