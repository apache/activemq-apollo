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
package org.apache.activemq.apollo.store

import org.apache.activemq.apollo.util.ClassFinder
import org.apache.activemq.broker.store.Store
import org.apache.activemq.apollo.dto.StoreDTO
import org.apache.activemq.apollo.broker.{ReporterLevel, Reporter}
import ReporterLevel._

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class StoreFactory

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object StoreFactory {

  val finder =  ClassFinder[SPI]("META-INF/services/org.apache.activemq.apollo/stores")
  var storesSPI = List[SPI]()

  trait SPI {
    def create(config:StoreDTO):Store
    def validate(config: StoreDTO, reporter:Reporter):ReporterLevel
  }

  finder.find.foreach{ clazz =>
    try {
      val SPI = clazz.newInstance.asInstanceOf[SPI]
      storesSPI ::= SPI
    } catch {
      case e:Throwable =>
        e.printStackTrace
    }
  }

  def create(config:StoreDTO):Store = {
    if( config == null ) {
      return null
    }
    storesSPI.foreach { spi=>
      val rc = spi.create(config)
      if( rc!=null ) {
        return rc
      }
    }
    throw new IllegalArgumentException("Uknonwn store configuration type: "+config.getClass)
  }


  def validate(config: StoreDTO, reporter:Reporter):ReporterLevel = {
    if( config == null ) {
      return INFO
    } else {
      storesSPI.foreach { spi=>
        val rc = spi.validate(config, reporter)
        if( rc!=null ) {
          return rc
        }
      }
    }
    reporter.report(ERROR, "Uknonwn store configuration type: "+config.getClass)
    ERROR
  }

}