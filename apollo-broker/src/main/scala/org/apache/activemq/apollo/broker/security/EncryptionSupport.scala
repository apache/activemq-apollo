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
package org.apache.activemq.apollo.broker.security

import java.util.Properties
import org.jasypt.properties.PropertyValueEncryptionUtils
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor
import org.jasypt.encryption.pbe.config.EnvironmentStringPBEConfig

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object EncryptionSupport {

  val encryptor = new StandardPBEStringEncryptor
  encryptor.setConfig({
    val config = new EnvironmentStringPBEConfig
    config.setAlgorithm("PBEWithMD5AndDES")
    config.setPasswordEnvName("APOLLO_ENCRYPTION_PASSWORD")
    config
  })

  def decrypt(props:Properties):Properties = {

    import collection.JavaConversions._
    props.keySet.toArray.foreach{ k=>
      val key = k.asInstanceOf[String]
      var value = props.getProperty(key)
      if (PropertyValueEncryptionUtils.isEncryptedValue(value)) {
        value = PropertyValueEncryptionUtils.decrypt(value, encryptor);
        props.setProperty(key, value)
      }
    }
    props

  }

}