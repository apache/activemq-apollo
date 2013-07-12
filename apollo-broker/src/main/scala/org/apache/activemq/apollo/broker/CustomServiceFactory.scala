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

import org.apache.activemq.apollo.util._
import org.fusesource.hawtdispatch._
import java.util.concurrent.TimeUnit
import org.apache.activemq.apollo.dto.{SimpleCustomServiceDTO, AutoGCServiceDTO, CustomServiceDTO}

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

object SimpleCustomServiceFactory extends CustomServiceFactory with Log {

  def create(broker: Broker, dto: CustomServiceDTO): Service = dto match {
    case dto:SimpleCustomServiceDTO =>
      if( dto.getClass != classOf[SimpleCustomServiceDTO] ) {
        // don't process sub classes of SimpleCustomServiceDTO
        return null;
      }

      val service = try {
        Broker.class_loader.loadClass(dto.kind).newInstance().asInstanceOf[Service]
      } catch {
        case e:Throwable =>
          debug(e, "could not create instance of %d for service %s", dto.kind, Option(dto.id).getOrElse("<not set>"))
          return null;
      }

      import language.reflectiveCalls
      type ServiceDuckType = {
        var broker: Broker
        var config: CustomServiceDTO
      }

      // Try to inject the broker via reflection..
      try {
        service.asInstanceOf[ServiceDuckType].broker = broker
      } catch {
        case _:Throwable =>
      }

      // Try to inject the config via reflection..
      try {
        service.asInstanceOf[ServiceDuckType].config = dto
      } catch {
        case _:Throwable =>
      }

      service
    case _ => null
  }
}

object AutoGCServiceFactory extends CustomServiceFactory with Log {
  def create(broker: Broker, dto: CustomServiceDTO): Service = dto match {

    case dto:AutoGCServiceDTO => new BaseService {

      val dispatch_queue = createQueue("auto gc service")

      def interval = OptionSupport(dto.interval).getOrElse(30)
      var run_counter = 0

      protected def _start(on_completed: Task) = {
        schedule_gc(run_counter)
        on_completed.run
      }

      protected def _stop(on_completed: Task) = {
        run_counter += 1
        on_completed.run
      }

      def schedule_gc(counter:Int):Unit = {
        if(counter == run_counter) {
          dispatch_queue.after(interval, TimeUnit.SECONDS) {
            Runtime.getRuntime.gc()
            schedule_gc(counter)
          }
        }
      }

    }
    case _ => null
  }
}