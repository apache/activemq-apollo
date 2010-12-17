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
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.util.path.Path
import org.apache.activemq.apollo.dto.{DestinationDTO, QueueDTO, BindingDTO}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Authorizer {

  /**
   * @returns true if the user is an admin.
   */
  def can_admin(ctx:SecurityContext, broker:Broker):Boolean

  /**
   * @returns true if the user is allowed to connect to the virtual host
   */
  def can_connect_to(ctx:SecurityContext, host:VirtualHost, connector:Connector):Boolean

  /**
   * @returns true if the user is allowed to send to the destination
   */
  def can_send_to(ctx:SecurityContext, host:VirtualHost, dest:DestinationDTO):Boolean

  /**
   * @returns true if the user is allowed to receive from the destination
   */
  def can_receive_from(ctx:SecurityContext, host:VirtualHost, dest:DestinationDTO):Boolean

  /**
   * @returns true if the user is allowed to create the destination
   */
  def can_create(ctx:SecurityContext, host:VirtualHost, dest:DestinationDTO):Boolean

  /**
   * @returns true if the user is allowed to destroy the destination
   */
  def can_destroy(ctx:SecurityContext, host:VirtualHost, dest:DestinationDTO):Boolean


  /**
   * @returns true if the user is allowed to send to the queue
   */
  def can_send_to(ctx:SecurityContext, host:VirtualHost, queue:QueueDTO):Boolean

  /**
   * @returns true if the user is allowed to receive from the queue
   */
  def can_receive_from(ctx:SecurityContext, host:VirtualHost, queue:QueueDTO):Boolean

  /**
   * @returns true if the user is allowed to consume from the queue
   */
  def can_consume_from(ctx:SecurityContext, host:VirtualHost, queue:QueueDTO):Boolean

  /**
   * @returns true if the user is allowed to create the queue
   */
  def can_create(ctx:SecurityContext, host:VirtualHost, queue:QueueDTO):Boolean

  /**
   * @returns true if the user is allowed to destroy the queue
   */
  def can_destroy(ctx:SecurityContext, host:VirtualHost, queue:QueueDTO):Boolean

}