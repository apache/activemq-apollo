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

import javax.security.auth.login.LoginContext

import javax.security.auth.callback.Callback
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.callback.UnsupportedCallbackException

import org.apache.activemq.jaas._
import org.apache.activemq.apollo.util.OptionSupport._
import org.apache.activemq.apollo.broker.Broker.BLOCKABLE_THREAD_POOL
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.dto.{PrincipalDTO, AuthenticationDTO}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

class JaasAuthenticator(val config: AuthenticationDTO) extends Authenticator {

  val jass_realm = config.domain.getOrElse("apollo")
  val user_principal_kinds = config.user_principal_kinds()

  /*
   * The 'BLOCKABLE_THREAD_POOL ! { ... }' magic makes the code block
   * execute on the global thread pool since JAAS requests could
   * potentially perform a blocking wait (e.g. LDAP request).
   */
  def authenticate(security_ctx: SecurityContext) = BLOCKABLE_THREAD_POOL ! {
    _authenticate(security_ctx)
  }

  /**
   * Extracts the user name of the logged in user.
   */
  def user_name(ctx:SecurityContext):Option[String] = {
    if( ctx.subject!=null ) {
      import collection.JavaConversions._
      ctx.subject.getPrincipals.find( x=> user_principal_kinds.contains( x.getClass.getName ) ).map(_.getName)
    } else {
      None
    }
  }

  def _authenticate(security_ctx: SecurityContext): Boolean = {
    val original = Thread.currentThread().getContextClassLoader()
    Thread.currentThread().setContextClassLoader(getClass.getClassLoader())
    try {

      security_ctx.login_context = new LoginContext(jass_realm, new CallbackHandler {
        def handle(callbacks: Array[Callback]) = {
          callbacks.foreach{
            callback =>
              callback match {
                case x: NameCallback => x.setName(security_ctx.user)
                case x: PasswordCallback => x.setPassword(security_ctx.password.getOrElse("").toCharArray)
                case x: CertificateCallback => x.setCertificates(security_ctx.certificates)
                case _ => throw new UnsupportedCallbackException(callback)
              }
          }
        }
      })

      security_ctx.login_context.login()
      security_ctx.subject = security_ctx.login_context.getSubject()
      true
    } catch {
      case x: Exception =>
        false
    } finally {
      Thread.currentThread().setContextClassLoader(original)
    }
  }

}