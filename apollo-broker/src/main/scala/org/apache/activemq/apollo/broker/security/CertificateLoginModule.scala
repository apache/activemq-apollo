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

import java.security.Principal
import javax.security.auth.Subject
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.FailedLoginException
import javax.security.auth.login.LoginException
import java.security.cert.X509Certificate
import java.{util => ju}
import java.io.{FileInputStream, File, IOException}
import org.yaml.snakeyaml.Yaml
import java.lang.String
import org.apache.activemq.jaas.{UserPrincipal, CertificateCallback}
import javax.security.auth.spi.LoginModule
import java.util.{Properties, LinkedList}
import org.apache.activemq.apollo.util.FileSupport._
import org.apache.activemq.apollo.util.{FileCache, FileSupport, Log}
import org.apache.activemq.apollo.util.Log._

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object CertificateLoginModule {
  val LOGIN_CONFIG = "java.security.auth.login.config"
  val FILE_OPTION = "dn_file"
  val DEFAULT_LOG = Log(getClass)

  def load_dns(file:File):Option[java.util.Map[String, AnyRef]] = {
    try {
      import FileSupport._
      using( new FileInputStream(file) ) { in=>
        Some((new Yaml().load(in)).asInstanceOf[java.util.Map[String, AnyRef]])
      }
    } catch {
      case e: Throwable =>
        DEFAULT_LOG.warn(e, "Unable to load the distinguished name file: " + file)
        None
    }
  }

  val file_cache = new FileCache[java.util.Map[String, AnyRef]](load_dns)
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class CertificateLoginModule extends LoginModule {

  import CertificateLoginModule._

  val log = JaasAuthenticator.broker_log.getOrElse(DEFAULT_LOG)
  import log._

  var callback_handler: CallbackHandler = _
  var subject: Subject = _

  var certificates: Array[X509Certificate] = _
  var principals = new LinkedList[Principal]()

  var file: Option[File] = None

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

    file = Option(options.get(FILE_OPTION)).map(x=> new File(base_dir,x.asInstanceOf[String]))
    debug("Initialized file=%s", file)
  }

  def login: Boolean = {
    val cert_callback = new CertificateCallback()
    try {
      callback_handler.handle(Array(cert_callback))
    } catch {
      case ioe: IOException =>
        throw new LoginException(ioe.getMessage())
      case uce: UnsupportedCallbackException =>
        return false;
    }

    certificates = cert_callback.getCertificates()
    if( certificates==null ) {
      return false;
    }

    if (certificates.isEmpty) {
      throw new FailedLoginException("No associated certificates")
    }

    // Are we restricting the logins to known DNs?
    file match {
      case None =>
        for (cert <- certificates) {
          debug("Adding certificiate principal: '%s'", cert.getSubjectX500Principal.getName)
          principals.add(cert.getSubjectX500Principal)
        }

      case Some(file)=>

        val users = file_cache.get(file) match {
          case None =>
            throw new LoginException("Invalid login module configuration")
          case Some(x) => x
        }

        for (cert <- certificates) {
          val dn: String = cert.getSubjectX500Principal.getName
          if( users.containsKey(dn) ) {
            val alias = users.get(dn)
            if( alias!=null ) {
              principals.add(new UserPrincipal(alias.toString))
              debug("Adding user principal: '%s'", alias.toString)
            }
            principals.add(cert.getSubjectX500Principal)
            debug("Adding certificiate principal: '%s'", dn)
          } else {
            debug("Distinguished name: '%s' not found in dn file", dn)
          }
        }
    }

    if (principals.isEmpty) {
      throw new FailedLoginException("Unknown distinguished names: ["+certificates.map(_.getSubjectX500Principal.getName).mkString(";")+"]")
    }
    return true
  }

  def commit: Boolean = {
    subject.getPrincipals().addAll(principals)
    certificates = null;
    debug("commit")
    return true
  }

  def abort: Boolean = {
    principals.clear
    certificates = null;
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