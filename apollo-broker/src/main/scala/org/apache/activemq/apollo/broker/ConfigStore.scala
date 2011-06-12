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

import org.fusesource.hawtbuf.ByteArrayInputStream
import security.EncryptionSupport
import org.apache.activemq.apollo.util._
import FileSupport._
import java.util.Properties
import java.io.{FileInputStream, File}
import org.apache.activemq.apollo.dto.{XmlCodec, BrokerDTO}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ConfigStore {

  def load(file:File) = {
    XmlCodec.decode(classOf[BrokerDTO], new FileInputStream(file), config_properties(file))
  }

  def load_xml(in:Array[Byte]) = {
    XmlCodec.decode(classOf[BrokerDTO], new ByteArrayInputStream(in), config_properties(null))
  }

  def config_properties(file:File): Properties = {
    val props = new Properties()
    props.putAll(System.getProperties)
    if( file!=null ) {
      val prop_file = file.getParentFile / (file.getName + ".properties")
      if (prop_file.exists()) {
        FileSupport.using(new FileInputStream(prop_file)) {
          is =>
            val p = new Properties
            p.load(new FileInputStream(prop_file))
            props.putAll(EncryptionSupport.decrypt(p))
        }
      }
    }
    props
  }
}
