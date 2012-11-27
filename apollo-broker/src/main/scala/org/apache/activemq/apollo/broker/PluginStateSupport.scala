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
package org.apache.activemq.apollo.broker

import java.util.concurrent.ConcurrentHashMap

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait PluginStateSupport {

  private val _plugin_state = new ConcurrentHashMap[Class[_],  Any]()

  /**
   * Plugins can associate state data with the virtual host instance
   * using this method.  The factory will be used to create the state
   * if it does not yet exist.
   */
  def plugin_state[T](factory: =>T, clazz:Class[T]):T = {
    var state = _plugin_state.get(clazz).asInstanceOf[T]
    if( state == null ) {
      state = factory
      if( state != null ) {
        _plugin_state.put(clazz, state)
      }
    }
    state
  }

  /**
   * Used to clear out previously set plugin state.
   */
  def clear_plugin_state[T](clazz:Class[T]):T = {
    _plugin_state.remove(clazz).asInstanceOf[T]
  }

}