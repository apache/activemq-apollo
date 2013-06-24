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

import org.apache.activemq.apollo.dto.KeyStorageDTO
import javax.net.ssl._
import java.io.FileInputStream
import java.security.{Principal, KeyStore}
import java.net.Socket

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class KeyStorage(val config:KeyStorageDTO) {

  var key_store:KeyStore = _
  var trust_managers:Array[TrustManager] = _
  var key_managers:Array[KeyManager] = _

  // a little helper for dealing /w null values.
  private def opt[T](value:T):Option[T] = value match {
    case null => None
    case x => Some(x)
  }

  def create_key_store = {
    if( key_store == null ) {
      key_store = {
        val store = KeyStore.getInstance(opt(config.store_type).getOrElse("JKS"))
        store.load(new FileInputStream(config.file), opt(config.password).getOrElse("").toCharArray())
        store
      }
    }
    key_store
  }

  def create_trust_managers = {
    if( trust_managers==null ) {
      val factory = TrustManagerFactory.getInstance(opt(config.trust_algorithm).getOrElse("SunX509"))
      factory.init(create_key_store)
      trust_managers = factory.getTrustManagers
    }
    trust_managers
  }

  def create_key_managers = {
    if( key_managers==null ) {
      val factory = KeyManagerFactory.getInstance(opt(config.key_algorithm).getOrElse("SunX509"))
      factory.init(create_key_store, opt(config.key_password).getOrElse("").toCharArray())
      key_managers = factory.getKeyManagers

      if( config.key_alias!=null ) {
        key_managers = key_managers.map  { m =>
          m match {
            case m:X509ExtendedKeyManager => AliasFilteringKeyManager(config.key_alias, m)
            case _ => m
          }
        }
      }
    }
    key_managers
  }

}

case class AliasFilteringKeyManager(alias: String, next:X509ExtendedKeyManager) extends X509ExtendedKeyManager {
  override def chooseEngineClientAlias(keyType: Array[String], issuers: Array[Principal], engine: SSLEngine) = alias
  override def chooseEngineServerAlias(keyType: String, issuers: Array[Principal], engine: SSLEngine) = alias
  def chooseClientAlias(keyType: Array[String], issuers: Array[Principal], socket: Socket) = alias
  def chooseServerAlias(keyType: String, issuers: Array[Principal], socket: Socket) = alias
  def getClientAliases(keyType: String, issuers: Array[Principal]) = next.getClientAliases(keyType, issuers)
  def getServerAliases(keyType: String, issuers: Array[Principal]) = next.getServerAliases(keyType, issuers)
  def getCertificateChain(alias: String) = next.getCertificateChain(alias)
  def getPrivateKey(alias: String) = next.getPrivateKey(alias)
}