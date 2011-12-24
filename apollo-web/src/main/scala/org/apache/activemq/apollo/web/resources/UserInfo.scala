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
package org.apache.activemq.apollo.web.resources

import org.apache.activemq.apollo.broker.security.SecurityContext
import javax.servlet.http._
import java.util.concurrent.CountDownLatch

case class UserInfo(username:String, password:String) extends HttpSessionActivationListener with HttpSessionBindingListener {

  //
  // We hold on to the security_context so we can log the user out of the JAAS login module.
  // If the session is passivated, expired, or deleted, we automatically log the user
  // out of the JAAS login module.
  //
  @transient
  var security_context: SecurityContext = null
  
  def logout = {
    if(security_context!=null ) {
      val cd = new CountDownLatch(1)
      security_context.logout((e)=> {
        // ignore for now..
        cd.countDown();
      })
      security_context = null
      cd.await();
    }
  }

  def valueBound(event: HttpSessionBindingEvent) {}
  def valueUnbound(event: HttpSessionBindingEvent) = logout
  def sessionDidActivate(se: HttpSessionEvent) {}
  def sessionWillPassivate(se: HttpSessionEvent) = logout
}
