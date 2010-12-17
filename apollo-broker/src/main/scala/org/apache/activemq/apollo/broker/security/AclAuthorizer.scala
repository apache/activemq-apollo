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

import scala.util.continuations._
import org.apache.activemq.apollo.util.path.Path
import org.apache.activemq.apollo.dto._
import org.apache.activemq.apollo.broker.{Connector, Destination, VirtualHost, Broker}

/**
 * <p>
 * Authorizes based on the acl configuration found in
 * the broker configuration model
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class AclAuthorizer(val default_kinds:List[String]) extends Authorizer {
  import collection.JavaConversions._

  def is_in(ctx: SecurityContext, allowed:java.util.List[PrincipalDTO]):Boolean = {
    ctx.is_allowed(allowed.toList, default_kinds)
  }

  def can_admin(ctx: SecurityContext, broker: Broker) = {
    if( broker.config.acl!=null ) {
      is_in(ctx, broker.config.acl.admins)
    } else {
      true
    }
  }

  def can_connect_to(ctx: SecurityContext, host: VirtualHost, connector:Connector):Boolean = {
    if( host.config.acl!=null && !is_in(ctx, host.config.acl.connects) ) {
      return false
    }
    if( connector.config.acl!=null && !is_in(ctx, connector.config.acl.connects) ) {
      return false
    }
    true
  }

  private def can_dest(ctx: SecurityContext, host: VirtualHost, dest: DestinationDTO)(func: DestinationAclDTO=>java.util.List[PrincipalDTO]) = {
    if( dest.acl!=null ) {
      is_in(ctx, func(dest.acl))
    } else {
      true
    }
  }

  def can_send_to(ctx: SecurityContext, host: VirtualHost, dest: DestinationDTO) = {
    can_dest(ctx, host, dest)(_.sends)
  }
  def can_receive_from(ctx: SecurityContext, host: VirtualHost, dest: DestinationDTO) = {
    can_dest(ctx, host, dest)(_.receives)
  }
  def can_destroy(ctx: SecurityContext, host: VirtualHost, dest: DestinationDTO) = {
    can_dest(ctx, host, dest)(_.destroys)
  }
  def can_create(ctx: SecurityContext, host: VirtualHost, dest: DestinationDTO) = {
    can_dest(ctx, host, dest)(_.creates)
  }

  private def can_queue(ctx: SecurityContext, host: VirtualHost, queue: QueueDTO)(func: QueueAclDTO=>java.util.List[PrincipalDTO]) = {
    if( queue.acl!=null ) {
      is_in(ctx, func(queue.acl))
    } else {
      true
    }
  }

  def can_send_to(ctx: SecurityContext, host: VirtualHost, queue: QueueDTO) = {
    can_queue(ctx, host, queue)(_.sends)
  }

  def can_receive_from(ctx: SecurityContext, host: VirtualHost, queue: QueueDTO) = {
    can_queue(ctx, host, queue)(_.receives)
  }

  def can_destroy(ctx: SecurityContext, host: VirtualHost, queue: QueueDTO) = {
    can_queue(ctx, host, queue)(_.destroys)
  }

  def can_create(ctx: SecurityContext, host: VirtualHost, queue: QueueDTO) = {
    can_queue(ctx, host, queue)(_.creates)
  }

  def can_consume_from(ctx: SecurityContext, host: VirtualHost, queue: QueueDTO) = {
    can_queue(ctx, host, queue)(_.consumes)
  }

}