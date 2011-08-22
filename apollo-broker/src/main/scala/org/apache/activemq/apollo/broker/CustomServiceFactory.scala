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

import org.apache.activemq.apollo.dto.CustomServiceDTO
import org.apache.activemq.apollo.util.{Log, Service, ClassFinder}

trait CustomServiceFactory {
  def create(broker:Broker, dto:CustomServiceDTO):Service
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object CustomServiceFactory {

  val finder = new ClassFinder[CustomServiceFactory]("META-INF/services/org.apache.activemq.apollo/custom-service-factory.index",classOf[CustomServiceFactory])

  def create(broker:Broker, dto:CustomServiceDTO):Service = {
    if( dto == null ) {
      return null
    }
    finder.singletons.foreach { provider=>
      val service = provider.create(broker, dto)
      if( service!=null ) {
        return service;
      }
    }
    return null
  }

}

object ReflectiveCustomServiceFactory extends CustomServiceFactory with Log {

  def create(broker: Broker, dto: CustomServiceDTO): Service = {
    if( dto.getClass != classOf[CustomServiceDTO] ) {
      // don't process sub classes of CustomServiceDTO
      return null;
    }

    val service = try {
      Broker.class_loader.loadClass(dto.kind).newInstance().asInstanceOf[Service]
    } catch {
      case e:Throwable =>
        debug(e, "could not create instance of %d for service %s", dto.kind, dto.id)
        return null;
    }

    // Try to inject the broker via reflection..
    try {
      type BrokerAware = {var broker: Broker}
      service.asInstanceOf[BrokerAware].broker = broker
    } catch {
      case _ =>
    }

    // Try to inject the config via reflection..
    try {
      type ConfigAware = {var config: CustomServiceDTO}
      service.asInstanceOf[ConfigAware].config = dto
    } catch {
      case _ =>
    }

    service

  }
}