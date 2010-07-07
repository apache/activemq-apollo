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
package org.apache.activemq.apollo

import java.util.HashMap
import org.apache.activemq.apollo.broker.Broker

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object BrokerRegistry {

  private val _brokers = new HashMap[String, Broker]()

  def get(id:String) = _brokers.synchronized {
    _brokers.get(id)
  }

  def add(broker:Broker) = _brokers.synchronized {
    _brokers.put(broker.name, broker)
  }

  def remove(id:String) = _brokers.synchronized {
    _brokers.remove(id)
  }
}