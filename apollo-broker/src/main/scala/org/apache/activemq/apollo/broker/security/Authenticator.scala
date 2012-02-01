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

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Authenticator {

  /**
   * If the authentication succeeds, then the subject and
   * principles fields of the SecurityContext should be populated.
   *
   * @returns null if the SecurityContext was authenticated. Otherwise
   * returns an error message that can be given to a client.
   */
  def authenticate(ctx:SecurityContext)(cb:(String)=>Unit)

  /**
   * Extracts the user name of the logged in user.
   */
  def user_name(ctx:SecurityContext):Option[String]

  def acl_principal_kinds:Set[String]

}