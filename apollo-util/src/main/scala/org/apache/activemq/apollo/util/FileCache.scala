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

import java.io.File
import java.lang.Long
import org.fusesource.hawtdispatch._
import scala.Some
import java.util.concurrent.{TimeUnit, ConcurrentHashMap}
import java.util.Arrays
import org.apache.activemq.apollo.util.FileSupport._

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class FileMonitor(file:File, change_listener: =>Unit) extends BaseService {

  val dispatch_queue = createQueue("monitor: "+file)

  var last_data = Array[Byte]()
  var last_modified = 0L
  var state_ver = 0

  protected def _stop(on_completed: Task) = {
    state_ver+=1
    on_completed.run()
  }

  protected def _start(on_completed: Task) = {
    last_data = file.read_bytes
    last_modified = file.lastModified()
    state_ver+=1
    update_check(state_ver)
    on_completed.run()
  }

  private def update_check(ver:Int):Unit = {
    if( ver == state_ver ) {
      try {
        val modified = file.lastModified
        if( modified != last_modified ) {
          val new_data = file.read_bytes
          if ( !Arrays.equals(last_data, new_data) ) {
            change_listener
          }
          last_modified = modified
          last_data = new_data
        }
      } catch {
        case e:Exception =>
        // error reading the file..  could be that someone is
        // in the middle of updating the file.
      }
      dispatch_queue.after(1, TimeUnit.SECONDS) {
        update_check(ver)
      }
    }
  }
}

/**
 * <p>
 * Class used to maintain a cache of loaded files which gets
 * evicted periodically via async time stamp checks.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class FileCache[T](mapper: (File)=>Option[T], evict_after:Long=1000*60*5) {

  class Entry(val file:File, val modified:Long, @volatile var last_accessed:Long, val value:Option[T])

  private val cache = new ConcurrentHashMap[File, Entry]()
  private var eviction_ver = 0

  def get(file:File):Option[T] = {
    var rc = cache.get(file)
    val now: Long = System.currentTimeMillis()
    if( rc == null ) {
      rc = if ( !file.exists() ) {
        new Entry(file, 0, now, None)
      } else {
        new Entry(file, file.lastModified(), now, mapper(file))
      }
      this.synchronized {
        cache.put(file, rc)
        if( cache.size() == 1) {
          eviction_ver += 1;
          val ver = eviction_ver
          globalQueue.after(1, TimeUnit.SECONDS)(eviction_check(ver))
        }
      }
    }
    rc.last_accessed = now
    rc.value
  }

  def eviction_check(ver:Int):Unit = {
    if (ver == eviction_ver) {
      import collection.JavaConversions._
      val evict_point = System.currentTimeMillis() - evict_after
      val evictions = cache.values().flatMap { entry =>
        if(
          entry.value == None ||
          !entry.file.exists() ||
          entry.file.lastModified() != entry.modified ||
          entry.last_accessed < evict_point
        ) {
          Some(entry.file)
        } else {
          None
        }
      }
      evictions.foreach(f => cache.remove(f))
      this.synchronized {
        if( cache.size() == 0) {
          eviction_ver += 1;
        } else {
          globalQueue.after(1, TimeUnit.SECONDS)(eviction_check(ver))
        }
      }
    }
  }

}