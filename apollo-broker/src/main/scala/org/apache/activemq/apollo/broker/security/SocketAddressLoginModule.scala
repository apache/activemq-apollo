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

import javax.security.auth.Subject
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.LoginException
import java.{util => ju}
import java.lang.String
import javax.security.auth.spi.LoginModule
import java.net.{InetSocketAddress, SocketAddress}
import java.io.{File, IOException}
import org.apache.activemq.apollo.util.{FileCache, FileSupport, Log}
import java.security.Principal

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

  def load_line_set(file:File):Option[Set[String]] = {
    try {
      import FileSupport._
      val rc: Set[String] = file.read_text().split("\n").map(_.trim()).toSet
      Some(rc.flatMap { line =>
        if(line.isEmpty || line.startsWith("#")) {
          None
        } else {
          Some(line)
        }
      })
    } catch {
      case e: Throwable =>
        DEFAULT_LOG.warn(e, "Unable to load file: " + file)
        None
    }
  }

  val file_cache = new FileCache[Set[String]](load_line_set)
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
  private val principals = new ju.HashSet[Principal]()
  private var subject:Subject = _

  /**
   * Overriding to allow for proper initialization. Standard JAAS.
   */
  def initialize(subject: Subject, callback_handler: CallbackHandler, shared_state: ju.Map[String, _], options: ju.Map[String, _]): Unit = {
    this.subject = subject
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

    val address = address_callback.remote match {
      case address:InetSocketAddress =>
        address.getAddress.getHostAddress
      case _ => null
    }

    white_list_file match {
      case None =>
      case Some(file)=>
        if( !matches(file, address) ) {
          throw new LoginException("Remote address is not whitelisted.")
        }
    }

    black_list_file match {
      case None =>
      case Some(file)=>
        if( matches(file, address) ) {
          throw new LoginException("Remote address blacklisted.")
        }
    }

    if( address!=null ) {
      principals.add(SourceAddressPrincipal(address))
    }

    return false
  }

  def matches(file:File, address:String):Boolean = {
    if( address == null )
      return false

    file_cache.get(file) match {
      case None => false
      case Some(haystack) =>
        haystack.contains(address)
    }
  }

  def commit: Boolean = {
    subject.getPrincipals().addAll(principals)
    return true
  }

  def abort: Boolean = {
    principals.clear
    return true
  }

  def logout: Boolean = {
    subject.getPrincipals().removeAll(principals)
    principals.clear
    return true
  }

}