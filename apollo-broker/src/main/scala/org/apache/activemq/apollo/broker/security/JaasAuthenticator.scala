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


import javax.security.auth.callback.Callback
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.callback.UnsupportedCallbackException

import org.apache.activemq.jaas._
import org.apache.activemq.apollo.broker.Broker.BLOCKABLE_THREAD_POOL
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.dto.AuthenticationDTO
import org.apache.activemq.apollo.util.Log
import collection.JavaConversions._
import javax.security.auth.login._
import javax.security.auth.message.AuthException

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object JaasAuthenticator extends Log {

  val _log = new ThreadLocal[Log]()
  def broker_log = Option(_log.get())

}


/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class JaasAuthenticator(val config: AuthenticationDTO, val log:Log) extends Authenticator {

  val jass_realm = Option(config.domain).getOrElse("apollo")
  val user_principal_kinds = config.user_principal_kinds()
  val acl_principal_kinds = config.acl_principal_kinds().toSet

  /*
   * The 'BLOCKABLE_THREAD_POOL ! { ... }' magic makes the code block
   * execute on the global thread pool since JAAS requests could
   * potentially perform a blocking wait (e.g. LDAP request).
   */
  def authenticate(security_ctx: SecurityContext)(cb:(String)=>Unit) = BLOCKABLE_THREAD_POOL {
    cb(_authenticate(security_ctx))
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

  def _authenticate(security_ctx: SecurityContext): String = {
    val original = Thread.currentThread().getContextClassLoader()
    Thread.currentThread().setContextClassLoader(getClass.getClassLoader())
    JaasAuthenticator._log.set(log)
    try {

      security_ctx.login_context = new LoginContext(jass_realm, new CallbackHandler {
        def handle(callbacks: Array[Callback]) = {
          callbacks.foreach{
            callback =>
              callback match {
                case x: NameCallback => x.setName(security_ctx.user)
                case x: PasswordCallback => x.setPassword(Option(security_ctx.password).map(_.toCharArray).getOrElse(null))
                case x: CertificateCallback => x.setCertificates(security_ctx.certificates)
                case x: SocketAddressCallback =>
                  x.local = security_ctx.local_address
                  x.remote = security_ctx.remote_address
                case _ => throw new UnsupportedCallbackException(callback)
              }
          }
        }
      })

      security_ctx.login_context.login()
      security_ctx.subject = security_ctx.login_context.getSubject()
      null

    } catch {
      case x: Exception =>
        val (reported, actual) = x match {
          case x:AccountLockedException =>
            ("Account locked", "Account locked: "+x.getMessage)
          case x:AccountExpiredException  =>
            ("Account expired", "Account expired: "+x.getMessage)
          case x:CredentialExpiredException  =>
            ("Creditial expired", "Creditial expired: "+x.getMessage)
          case x:FailedLoginException  =>
            ("Authentication failed", "Failed login: "+x.getMessage)
          case x:AccountNotFoundException  =>
            ("Authentication failed", "Account not found: "+x.getMessage)
          case _ =>
            ("Authentication failed", x.getMessage)
        }
        security_ctx.login_context = null
        log.info("authentication failed: local:%s, remote:%s, reason:%s ", security_ctx.local_address, security_ctx.remote_address, actual)
        reported
    } finally {
      JaasAuthenticator._log.remove
      Thread.currentThread().setContextClassLoader(original)
    }
  }

}