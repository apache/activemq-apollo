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
import collection.mutable.HashSet
import javax.security.auth.Subject
import java.security.cert.X509Certificate
import org.apache.activemq.apollo.util.OptionSupport._
import org.apache.activemq.jaas.{GroupPrincipal, UserPrincipal}
import org.apache.activemq.apollo.dto.PrincipalDTO
import javax.security.auth.login.LoginContext

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class SecurityContext {

  var user:String = _
  var password:String = _
  var certificates = Array[X509Certificate]()

  var login_context:LoginContext = _

  private var principles = Set[PrincipalDTO]()

  private var _subject:Subject = _

  def subject = _subject

  def subject_= (value:Subject) {
    _subject = value
    principles = Set[PrincipalDTO]()
    if( value!=null ) {
      import collection.JavaConversions._
      value.getPrincipals.foreach { x=>
        principles += new PrincipalDTO(x.getName, x.getClass.getName)
      }
    }
  }

  def is_allowed(acl:List[PrincipalDTO], default_kinds:List[String]):Boolean = {

    def kind_matches(kind:String):Boolean = {
      kind match {
        case null=>
          return !principles.map(_.kind).intersect(default_kinds.toSet).isEmpty
        case "*"=>
          return true;
        case kind=>
          return principles.map(_.kind).contains(kind)
      }
    }

    def principal_matches(p:PrincipalDTO):Boolean = {
      p.kind match {
        case null=>
          default_kinds.foreach { kind=>
            if( principles.contains(new PrincipalDTO(p.allow, kind)) ) {
              return true;
            }
          }
          return false;
        case "*"=>
          return principles.map(_.allow).contains(p.allow)
        case kind=>
          return principles.contains(p)
      }
    }

    acl.foreach { p =>
      p.deny match {
        case null =>
        case "*"=>
          return !kind_matches(p.kind)
        case id =>
          if( principal_matches(new PrincipalDTO(id, p.kind)) ) {
            return false;
          }
      }
      p.allow match {
        case null =>
        case "*"=>
          return kind_matches(p.kind)
        case id =>
          if( principal_matches(new PrincipalDTO(id, p.kind)) ) {
            return true
          }
      }
    }
    return false
  }


}