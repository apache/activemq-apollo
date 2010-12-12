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

import org.apache.activemq.jaas.GroupPrincipal
import org.apache.activemq.jaas.UserPrincipal
import java.{util => ju}
import org.apache.activemq.apollo.util.{FileSupport, Log}
import FileSupport._

object FileLoginModule extends Log {
  val LOGIN_CONFIG = "java.security.auth.login.config"
  val USERS_FILE = "users_file"
  val GROUPS_FILE = "groups_file"
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class FileLoginModule extends LoginModule {

  import FileLoginModule._

  private var subject: Subject = _
  private var callback_handler: CallbackHandler = _

  private var user_file: File = _
  private var group_file: File = _

  private val users = new Properties()
  private val groups = new Properties()

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

    user_file = new File(base_dir, options.get(USERS_FILE).asInstanceOf[String])
    group_file = new File(base_dir, options.get(GROUPS_FILE).asInstanceOf[String])

    debug("Initialized user_file=%s group_file=%s", user_file, group_file)
  }

  def login: Boolean = {
    try {
      users.clear()
      using( new FileInputStream(user_file) ) { in=>
        users.load(in)
      }
      EncryptionSupport.decrypt(users)
    } catch {
      case ioe: IOException => throw new LoginException("Unable to load user properties file " + user_file)
    }

    try {
      groups.clear
      using( new FileInputStream(group_file) ) { in=>
        groups.load(in)
      }
    } catch {
      case ioe: IOException => throw new LoginException("Unable to load group properties file " + group_file)
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
    val en = groups.keys()
    while (en.hasMoreElements()) {
      val group_name = en.nextElement().asInstanceOf[String]
      val users = groups.getProperty(group_name).split(",").map(_.trim)
      users.foreach { x =>
        if (user == x) {
          principals.add(new GroupPrincipal(group_name))
        }
      }
    }

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
