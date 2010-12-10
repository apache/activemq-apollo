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

import org.apache.activemq.apollo.broker.{Destination, VirtualHost, Broker}
import scala.util.continuations._
import org.apache.activemq.apollo.util.path.Path
import org.apache.activemq.apollo.dto.{PrincipalDTO, QueueAclDTO, DestinationAclDTO, BindingDTO}

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

  var allow_deafult = true

  private def sync[T](func: =>T): T @suspendable = shift { k: (T=>Unit) =>
    k(func)
  }

  def is_in(ctx: SecurityContext, allowed:java.util.Set[PrincipalDTO]):Boolean = {
    ctx.intersects(allowed.toSet, default_kinds)
  }

  def can_admin(ctx: SecurityContext, broker: Broker) = sync {
    if( broker.config.acl!=null ) {
      is_in(ctx, broker.config.acl.admins)
    } else {
      allow_deafult
    }
  }

  def can_connect_to(ctx: SecurityContext, host: VirtualHost) = sync {
    if( host.config.acl!=null ) {
      is_in(ctx, host.config.acl.connects)
    } else {
      allow_deafult
    }
  }


  private def for_dest(ctx: SecurityContext, host: VirtualHost, dest: Path)(func: DestinationAclDTO=>java.util.Set[PrincipalDTO]) = {
    host.destination_config(dest).map { config=>
      if( config.acl!=null ) {
        is_in(ctx, func(config.acl))
      } else {
        allow_deafult
      }
    }.getOrElse(allow_deafult)
  }

  def can_send_to(ctx: SecurityContext, host: VirtualHost, dest: Path) = sync {
    for_dest(ctx, host, dest)(_.sends)
  }
  def can_receive_from(ctx: SecurityContext, host: VirtualHost, dest: Path) = sync {
    for_dest(ctx, host, dest)(_.receives)
  }
  def can_destroy(ctx: SecurityContext, host: VirtualHost, dest: Path) = sync  {
    for_dest(ctx, host, dest)(_.destroys)
  }
  def can_create(ctx: SecurityContext, host: VirtualHost, dest: Path) = sync  {
    for_dest(ctx, host, dest)(_.creates)
  }

  private def for_queue(ctx: SecurityContext, host: VirtualHost, dto: BindingDTO)(func: QueueAclDTO=>java.util.Set[PrincipalDTO]) = {
    host.queue_config(dto).map { config=>
      if( config.acl!=null ) {
        is_in(ctx, func(config.acl))
      } else {
        allow_deafult
      }
    }.getOrElse(allow_deafult)
  }

  def can_send_to(ctx: SecurityContext, host: VirtualHost, dest: BindingDTO) = sync  {
    for_queue(ctx, host, dest)(_.sends)
  }

  def can_receive_from(ctx: SecurityContext, host: VirtualHost, dest: BindingDTO) = sync  {
    for_queue(ctx, host, dest)(_.receives)
  }

  def can_destroy(ctx: SecurityContext, host: VirtualHost, dest: BindingDTO) = sync  {
    for_queue(ctx, host, dest)(_.destroys)
  }

  def can_create(ctx: SecurityContext, host: VirtualHost, dest: BindingDTO) = sync  {
    for_queue(ctx, host, dest)(_.creates)
  }

  def can_consume_from(ctx: SecurityContext, host: VirtualHost, dest: BindingDTO) = sync  {
    for_queue(ctx, host, dest)(_.consumes)
  }

}