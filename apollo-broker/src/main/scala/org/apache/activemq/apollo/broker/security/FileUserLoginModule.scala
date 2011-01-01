/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.security.Principal
import java.util.Properties
import javax.security.auth.Subject
import javax.security.auth.callback.Callback
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.NameCallback
import javax.security.auth.callback.PasswordCallback
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.FailedLoginException
import javax.security.auth.login.LoginException
import javax.security.auth.spi.LoginModule

import org.apache.activemq.jaas.UserPrincipal
import java.{util => ju}
import org.apache.activemq.apollo.util.{FileSupport, Log}
import FileSupport._

object FileUserLoginModule extends Log {
  val LOGIN_CONFIG = "java.security.auth.login.config"
  val FILE_OPTION = "file"
}

/**
 * <p>
 * Uses a userid=password property file to control who can
 * login.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class FileUserLoginModule extends LoginModule {

  import FileUserLoginModule._

  private var subject: Subject = _
  private var callback_handler: CallbackHandler = _

  private var file: File = _
  private val users = new Properties()
  private var user: String = _
  private val principals = new ju.HashSet[Principal]()

  def initialize(subject: Subject, callback_handler: CallbackHandler, shared_state: ju.Map[String, _], options: ju.Map[String, _]): Unit = {

    this.subject = subject
    this.callback_handler = callback_handler

    val base_dir = if (System.getProperty(LOGIN_CONFIG) != null) {
      new File(System.getProperty(LOGIN_CONFIG)).getParentFile()
    } else {
      new File(".")
    }

    file = new File(base_dir, options.get(FILE_OPTION).asInstanceOf[String])

    debug("Initialized file=%s", file)
  }

  def login: Boolean = {
    try {
      users.clear()
      using( new FileInputStream(file) ) { in=>
        users.load(in)
      }
      EncryptionSupport.decrypt(users)
    } catch {
      case ioe: IOException => throw new LoginException("Unable to load user properties file " + file)
    }

    val callbacks = new Array[Callback](2)
    callbacks(0) = new NameCallback("Username: ")
    callbacks(1) = new PasswordCallback("Password: ", false)
    try {
      callback_handler.handle(callbacks)
    } catch {
      case ioe: IOException =>
        throw new LoginException(ioe.getMessage())
      case uce: UnsupportedCallbackException =>
        throw new LoginException(uce.getMessage() + " not available to obtain information from user")
    }

    user = callbacks(0).asInstanceOf[NameCallback].getName()
    var tmpPassword = callbacks(1).asInstanceOf[PasswordCallback].getPassword()
    if (tmpPassword == null) {
      tmpPassword = new Array[Char](0)
    }
    val password = users.getProperty(user)

    if (password == null || !password.equals(new String(tmpPassword))) {
      throw new FailedLoginException("Invalid user id or password")
    }
    debug("login %s", user)
    true
  }

  def commit: Boolean = {
    principals.add(new UserPrincipal(user))
    subject.getPrincipals().addAll(principals)

    user = null
    debug("commit")
    return true
  }

  def abort: Boolean = {
    user = null
    debug("abort")
    return true
  }

  def logout: Boolean = {
    subject.getPrincipals().removeAll(principals)
    principals.clear
    user = null
    debug("logout")
    return true
  }


}
