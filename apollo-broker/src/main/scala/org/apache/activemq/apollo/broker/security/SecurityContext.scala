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
import java.security.cert.X509Certificate
import java.net.SocketAddress
import org.apache.activemq.apollo.broker.Broker.BLOCKABLE_THREAD_POOL
import org.fusesource.hawtdispatch._
import javax.security.auth.login.LoginContext
import scala.collection.mutable.ListBuffer

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class SecurityContext {

  var user:String = _
  var password:String = _
  var sso_token:String = _
  var certificates:Array[X509Certificate] = _
  var connector_id:String = _
  var local_address:SocketAddress = _
  var remote_address:SocketAddress = _
  var login_context:LoginContext = _
  var session_id:String = null
  var remote_application:String = null

  case class Key(user:String,
    password:String,
    sso_token:String,
    certificates:ListBuffer[X509Certificate],
    connector_id:String,
    local_address:SocketAddress,
    remote_address:SocketAddress)

  def to_key = Key(user, password, sso_token, if(certificates==null) ListBuffer() else ListBuffer(certificates : _*),
    connector_id, local_address, remote_address)

  def credential_dump = {
    var rc = List[String]()
    if(certificates!=null) {
      for(cert<-certificates) {
        rc ::= "certdn="+cert.getSubjectX500Principal.getName
      }
    }
    if(user!=null) {
      rc ::= "user="+user
    }
    "["+rc.mkString(", ")+"]"
  }

  def principal_dump = {
    var rc = List[String]()
    if(_principals!=null) {
      for(principal<-_principals) {
        rc ::= principal.getClass.getName+"("+principal.getName+")"
      }
    }
    "["+rc.mkString(", ")+"]"
  }

  private var _subject:Subject = _
  def subject = _subject

  private var _principals = Set[Principal]()
  def principals = _principals

  def subject_= (value:Subject) {
    _subject = value
    _principals = Set()
    if( value!=null ) {
      import collection.JavaConversions._
      _principals = value.getPrincipals.toSet
    }
  }

  def principals(kind:String):Set[Principal] = {
    kind match {
      case "+"=>
        principals
      case "*"=>
        principals
      case kind=>
        principals.filter(_.getClass.getName == kind)
    }
  }

  /**
   * Logs the user off, called func when completed. Pass
   * any errors that occurred during the log off process
   * to the function or null.
   */
  def logout(func: (Throwable)=>Unit) = {
    if(login_context==null) {
      func(null)
    } else {
      val lc = login_context
      login_context = null
      BLOCKABLE_THREAD_POOL {
        try {
          lc.logout()
          func(null)
        } catch {
          case e:Throwable => func(e)
        }
      }
    }
  }
  
}