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

import org.apache.activemq.apollo.util.ClassFinder

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object DirectBufferPoolFactory {

  val finder = ClassFinder[SPI]("META-INF/services/org.apache.activemq.apollo/direct-buffer-pools")
  var spis = List[SPI]()

  trait SPI {
    def create(config:String):DirectBufferPool
    def validate(config: String):Boolean
  }

  finder.find.foreach{ clazz =>
    try {
      spis ::= clazz.newInstance.asInstanceOf[SPI]
    } catch {
      case e:Throwable => e.printStackTrace
    }
  }

  def create(config:String):DirectBufferPool = {
    if( config == null ) {
      return null
    }
    spis.foreach { spi=>
      val rc = spi.create(config)
      if( rc!=null ) {
        return rc
      }
    }
    throw new IllegalArgumentException("Uknonwn direct buffer pool type: "+config)
  }


  def validate(config: String):Boolean = {
    if( config == null ) {
      return true
    } else {
      spis.foreach { spi=>
        if( spi.validate(config) ) {
          return true
        }
      }
    }
    false
  }

}