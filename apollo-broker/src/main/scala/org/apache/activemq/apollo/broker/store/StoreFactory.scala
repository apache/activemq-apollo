package org.apache.activemq.apollo.broker.store

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
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.dto.{NullStoreDTO, StoreDTO}

trait StoreFactory {
  def create(config:StoreDTO):Store
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object StoreFactory {

  val finder = new ClassFinder[StoreFactory]("META-INF/services/org.apache.activemq.apollo/store-factory.index", classOf[StoreFactory])

  def create(config:StoreDTO):Store = config match {
    case null => null
    case config:NullStoreDTO => null
    case _ =>
      finder.singletons.foreach { provider=>
        val rc = provider.create(config)
        if( rc!=null ) {
          return rc
        }
      }
      throw new IllegalArgumentException("Uknonwn store type: "+config.getClass)
  }

}
