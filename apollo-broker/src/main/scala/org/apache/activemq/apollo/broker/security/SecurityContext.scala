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
import org.apache.activemq.jaas.UserPrincipal

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

  /**
   * A place for the authorization layer attach
   * some authorization data (i.e. an authorization
   * cache)
   */
  var authorization_data:AnyRef = _

  var subject:Subject = _

  def principles = subject.map(x=>collection.JavaConversions.asSet(x.getPrincipals).toSet).getOrElse(Set())

  def intersects(other_priciples:Set[Principal] ) = {
    !principles.intersect(other_priciples).isEmpty
  }

  def principle[T](kind:Class[T]):Option[T] ={
    principles.find( x=> kind.isAssignableFrom(x.getClass) ).map(kind.cast(_))
  }

  def user_principle = principle(classOf[UserPrincipal])
}