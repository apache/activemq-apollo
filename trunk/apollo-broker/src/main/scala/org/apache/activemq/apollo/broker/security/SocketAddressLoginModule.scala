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

import javax.security.auth.Subject
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.LoginException
import java.{util => ju}
import java.io.{File, IOException}
import org.apache.activemq.apollo.util.{FileSupport, Log}
import java.lang.String
import javax.security.auth.spi.LoginModule
import java.net.{InetSocketAddress, SocketAddress, InetAddress}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object SocketAddressLoginModule {
  val LOGIN_CONFIG = "java.security.auth.login.config"
  val WHITE_LIST_OPTION = "white_list_file"
  val BLACK_LIST_OPTION = "black_list_file"
  val DEFAULT_LOG = Log(getClass)
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class SocketAddressLoginModule extends LoginModule {

  import SocketAddressLoginModule._

  val log = JaasAuthenticator.broker_log.getOrElse(DEFAULT_LOG)
  import log._

  var callback_handler: CallbackHandler = _

  var white_list_file: Option[File] = None
  var black_list_file: Option[File] = None

  /**
   * Overriding to allow for proper initialization. Standard JAAS.
   */
  def initialize(subject: Subject, callback_handler: CallbackHandler, shared_state: ju.Map[String, _], options: ju.Map[String, _]): Unit = {
    this.callback_handler = callback_handler

    val base_dir = if (System.getProperty(LOGIN_CONFIG) != null) {
      new File(System.getProperty(LOGIN_CONFIG)).getParentFile()
    } else {
      new File(".")
    }

    white_list_file = Option(options.get(WHITE_LIST_OPTION)).map(x=> new File(base_dir,x.asInstanceOf[String]))
    black_list_file = Option(options.get(BLACK_LIST_OPTION)).map(x=> new File(base_dir,x.asInstanceOf[String]))

    debug("Initialized white_list_file=%s, black_list_file=%s", white_list_file, black_list_file)
  }

  def login: Boolean = {
    val address_callback = new SocketAddressCallback()
    try {
      callback_handler.handle(Array(address_callback))
    } catch {
      case ioe: IOException =>
        throw new LoginException(ioe.getMessage())
      case uce: UnsupportedCallbackException =>
        return false;
    }

    if( address_callback.remote==null ) {
      return false;
    }

    white_list_file match {
      case None =>
      case Some(file)=>
        if( !matches(file, address_callback.remote) ) {
          throw new LoginException("Remote address is not whitelisted.")
        }
    }

    black_list_file match {
      case None =>
      case Some(file)=>
        if( matches(file, address_callback.remote) ) {
          throw new LoginException("Remote address blacklisted.")
        }
    }

    return true
  }

  def matches(file:File, address:SocketAddress):Boolean = {

    val needle = address match {
      case address:InetSocketAddress =>
        address.getAddress.getHostAddress
      case _ => return false
    }

    import FileSupport._
    file.read_text().split("\n").find( _.trim() == needle ).isDefined
  }

  def commit: Boolean = {
    return true
  }

  def abort: Boolean = {
    return true
  }

  def logout: Boolean = {
    return true
  }

}