/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.stomp.test

import org.apache.activemq.apollo.broker.security._
import org.apache.activemq.apollo.broker.{Queue, Broker, VirtualHost}
import java.lang.Boolean

/**
 * This is an example implementation of a SecurityFactory which
 * provides the default authentication and authorization behaviours,
 * except that it applies a dynamic authorization scheme so that users
 * are only allowed to use queues named "user.${user}.**"
 */
class UserOwnershipSecurityFactory extends SecurityFactory {

  def install(broker: Broker) {
    DefaultSecurityFactory.install(broker)
  }

  def install(virtual_host: VirtualHost) {
    DefaultSecurityFactory.install(virtual_host)
    val default_authorizer = virtual_host.authorizer
    virtual_host.authorizer = new Authorizer() {
      def can(ctx: SecurityContext, action: String, resource: SecuredResource): Boolean = {
        resource.resource_kind match {
          case SecuredResource.QueueKind =>
            val id = resource.id
            return id.startsWith("user."+ctx.user) || id.startsWith("user."+ctx.user+".")
          case _ =>
            return default_authorizer.can(ctx, action, resource)
        }
      }
    }
  }
}
