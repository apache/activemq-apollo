package org.apache.activemq.apollo.broker.security

import java.io.IOException
import java.security.Principal
import javax.security.auth.Subject
import javax.security.auth.callback.CallbackHandler
import javax.security.auth.callback.UnsupportedCallbackException
import javax.security.auth.login.FailedLoginException
import javax.security.auth.login.LoginException
import java.security.cert.X509Certificate
import java.util.HashSet


import java.{util => ju}
import org.apache.activemq.apollo.util.Log
import org.apache.activemq.jaas.CertificateCallback

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object CertificateLoginModule extends Log

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class CertificateLoginModule {

  import CertificateLoginModule._

  var callback_handler: CallbackHandler = _
  var subject: Subject = _

  var certificates: Array[X509Certificate] = _
  var principals = new HashSet[Principal]()

  /**
   * Overriding to allow for proper initialization. Standard JAAS.
   */
  def initialize(subject: Subject, callback_handler: CallbackHandler, shared_state: ju.Map[String, _], options: ju.Map[String, _]): Unit = {
    this.subject = subject
    this.callback_handler = callback_handler
  }

  def login: Boolean = {
    val cert_callback = new CertificateCallback()
    try {
      callback_handler.handle(Array(cert_callback))
    } catch {
      case ioe: IOException =>
        throw new LoginException(ioe.getMessage())
      case uce: UnsupportedCallbackException =>
        throw new LoginException(uce.getMessage() + " Unable to obtain client certificates.")
    }

    certificates = cert_callback.getCertificates()
    if (certificates == null || certificates.isEmpty) {
      throw new FailedLoginException("No associated certificates")
    }
    return true
  }

  def commit: Boolean = {
    for (cert <- certificates) {
      principals.add(cert.getSubjectX500Principal)
    }
    subject.getPrincipals().addAll(principals)
    certificates = null;
    debug("commit")
    return true
  }

  def abort: Boolean = {
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