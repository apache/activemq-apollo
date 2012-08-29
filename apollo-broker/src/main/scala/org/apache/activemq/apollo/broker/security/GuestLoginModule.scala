package org.apache.activemq.apollo.broker.security

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

import java.io.IOException
import java.security.Principal
import javax.security.auth.Subject
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.LoginException
import javax.security.auth.spi.LoginModule

import java.{util => ju}
import org.apache.activemq.apollo.util.Log
import org.apache.activemq.jaas.{GroupPrincipal, UserPrincipal}

object GuestLoginModule {
  val USER_OPTION = "user"
  val GROUP_OPTION = "group"
  val DEFAULT_LOG = Log(getClass)
}

/**
 * <p>
 * A login module which only succeeds if no id/password credentials
 * were given.  It can be configured to add a guest UserPrincipal
 * and GroupPrincipal.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class GuestLoginModule extends LoginModule {

  import GuestLoginModule._
  val log = JaasAuthenticator.broker_log.getOrElse(DEFAULT_LOG)
  import log._

  private var subject: Subject = _
  private var callback_handler: CallbackHandler = _

  private var user: String = _
  private var group: String = _
  private val principals = new ju.HashSet[Principal]()

  def initialize(subject: Subject, callback_handler: CallbackHandler, shared_state: ju.Map[String, _], options: ju.Map[String, _]): Unit = {
    this.subject = subject
    this.callback_handler = callback_handler

    user = options.get(USER_OPTION).asInstanceOf[String]
    group = options.get(GROUP_OPTION).asInstanceOf[String]
    debug("Initialized user=%s, group=%s", user, group)
  }

  def login: Boolean = {

    try {
      val callback = new NameCallback("Username: ")
      callback_handler.handle(Array(callback))
      if( callback.getName!=null && callback.getName.size>=0 ) {
        return false;
      }
    } catch {
      case ioe: IOException =>
        throw new LoginException(ioe.getMessage())
      case uce: UnsupportedCallbackException =>
    }

    try {
      val callback = new PasswordCallback("Password: ", false)
      callback_handler.handle(Array(callback))
      if( callback.getPassword!=null && callback.getPassword.size>=0 ) {
        return false;
      }
    } catch {
      case ioe: IOException =>
        throw new LoginException(ioe.getMessage())
      case uce: UnsupportedCallbackException =>
    }

    if( user!=null ) {
      principals.add(new UserPrincipal(user))
    }
    if( group!=null ) {
      principals.add(new GroupPrincipal(group))
    }
    debug("guest login: principals %s", principals)
    true
  }

  def commit: Boolean = {
    val p = subject.getPrincipals()
    if( p.isEmpty || (p.size()==1 && p.iterator().next().isInstanceOf[SourceAddressPrincipal]) ) {
      subject.getPrincipals().addAll(principals)
    }
    debug("commit")
    return true
  }

  def abort: Boolean = {
    principals.clear
    debug("abort")
    return true
  }

  def logout: Boolean = {
    subject.getPrincipals().removeAll(principals)
    principals.clear
    debug("logout")
    return true
  }


}