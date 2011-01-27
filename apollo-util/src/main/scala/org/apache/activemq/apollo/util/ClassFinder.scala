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
import java.net.URL


object ClassFinder extends Log {

  trait Loader {
    def getResources(path:String):java.util.Enumeration[URL]
    def loadClass(name:String):Class[_]
  }

  case class ClassLoaderLoader(cl:ClassLoader) extends Loader {
    def getResources(path:String) = cl.getResources(path)
    def loadClass(name:String) = cl.loadClass(name)
  }

  def standalone_loader():Array[Loader] = {
    Array(ClassLoaderLoader(Thread.currentThread.getContextClassLoader))
  }

  var default_loaders = ()=>standalone_loader
}

import ClassFinder._


/**
 * <p>
 * Used to discover classes using the META-INF discovery trick.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ClassFinder[T](val path:String, val loaders: ()=>Array[Loader]) {

  def this(path:String) = this(path, default_loaders)
//  def this(path:String, loaders:Array[ClassLoader]) = this(path, ()=>{loaders.map(ClassLoaderLoader _) })

  def findArray(): Array[Class[T]] = find.toArray

  def find(): List[Class[T]] = {
    var classes = List[Class[T]]()
    loaders().foreach { loader=>

      val resources = loader.getResources(path)
      var classNames: List[String] = Nil
      while(resources.hasMoreElements) {
        val url = resources.nextElement;
        val p = loadProperties(url.openStream)
        val enum = p.keys
        while (enum.hasMoreElements) {
          classNames = classNames ::: enum.nextElement.asInstanceOf[String] :: Nil
        }
      }
      classNames = classNames.distinct

      classes :::= classNames.flatMap { name=>
        try {
          Some(loader.loadClass(name).asInstanceOf[Class[T]])
        } catch {
          case e:Throwable =>
            debug(e, "Could not load class %s", name)
            None
        }
      }
    }

    return classes.distinct
  }

  def new_instances() = {
    val t = ListBuffer[T]()
    find.foreach {clazz =>
      try {
        t += clazz.newInstance.asInstanceOf[T]
      } catch {
        case e: Throwable =>
          // It may be a scala object.. check for a module class
          try {
            val moduleField = clazz.getClassLoader.loadClass(clazz.getName + "$").getDeclaredField("MODULE$")
            val instance = moduleField.get(null).asInstanceOf[T]
            t += instance
          } catch {
            case e2: Throwable =>
              debug(e, "Could not load the class")
          }
      }
    }
    t.toList
  }

  private def loadProperties(is:InputStream):Properties = {
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
        case _ =>
      }
    }
  }
}