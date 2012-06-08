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

import collection.mutable.ArrayBuffer

/**
 * <p>A circular buffer</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class CircularBuffer[T](max:Int) extends ArrayBuffer[T](max) {
  
  def max_size = max
  private var pos = 0
  
  override def +=(elem: T): this.type = {
    if( size < initialSize ) {
      super.+=(elem)
    } else {
      evicted(this(pos))
      this.update(pos, elem)
      pos += 1
      if( pos >= initialSize ) {
        pos = 0
      }
    }
    this
  }

  /**
   * Sub classes can override this method to so they can be
   * notified when an element is being evicted from the circular
   * buffer.
   */
  protected def evicted(elem:T) = {}
  
}