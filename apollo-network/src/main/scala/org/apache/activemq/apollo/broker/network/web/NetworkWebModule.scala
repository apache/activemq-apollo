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

package org.apache.activemq.apollo.broker.network.web

import org.apache.activemq.apollo.web.resources.{HelpResourceJSON, Resource}
import org.apache.activemq.apollo.web.WebModule
import javax.ws.rs.{PathParam, Produces, GET, Path}
import scala.Array
import javax.ws.rs.core.MediaType._
import org.apache.activemq.apollo.broker.network.dto.{LoadStatusDTO, ConsumerLoadDTO, DestinationLoadDTO}
import org.apache.activemq.apollo.broker.{Queue, LocalRouter}
import org.fusesource.hawtdispatch.Future
import org.apache.activemq.apollo.util.{FutureResult, Success}
import FutureResult._
import com.wordnik.swagger.annotations.{ApiOperation, Api}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */


object NetworkWebModule extends WebModule {
  override def web_resources = Set(
    classOf[NetworkResourceJSON],
    classOf[NetworkResourceHelp]
  )
}

@Path(          "/api/docs/network{ext:(\\.json)?}")
@Api(value =    "/api/json/network",
  listingPath = "/api/docs/network",
  listingClass = "org.apache.activemq.apollo.broker.network.web.NetworkResourceJSON")
class NetworkResourceHelp extends HelpResourceJSON

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
@Path(          "/api/json/network")
@Api(value =    "/api/json/network",
  listingPath = "/api/docs/network")
@Produces(Array("application/json"))
case class NetworkResourceJSON() extends Resource() {
//  import WebModule._

  @GET @Path("/load-status/{id}")
  @ApiOperation(value = "Gets a report of the message load on the host.")
  @Produces(Array(APPLICATION_JSON))
  def topics(@PathParam("id") id : String ):LoadStatusDTO = {
    with_virtual_host[LoadStatusDTO](id) { host =>
      val router: LocalRouter = host
      val queue_loads = Future.all {
        router.local_queue_domain.destination_by_id.values.map { value  =>
          monitoring[DestinationLoadDTO](value) {
            load_status(value)
          }
        }
      }
      queue_loads.map { queue_loads=>
        val rc = new LoadStatusDTO
        queue_loads.flatMap(_.success_option).foreach { load=>
          rc.queues.add(load)
        }
        Success(rc)
      }
    }
  }

  def load_status(queue:Queue) = {
    val rc = new DestinationLoadDTO
    rc.id = queue.id
    rc.message_count = queue.queue_size
    rc.message_size = queue.queue_items
    rc.message_count_enqueue_counter = queue.enqueue_item_counter
    rc.message_size_enqueue_counter = queue.enqueue_size_counter
    rc.message_count_dequeue_counter = queue.dequeue_item_counter
    rc.message_size_dequeue_counter = queue.dequeue_size_counter

    for( sub <- queue.all_subscriptions.values ) {
      val dto = new ConsumerLoadDTO
      dto.user = sub.consumer.user
      dto.selector = sub.consumer.jms_selector
      sub.ack_rates match {
        case Some((items_per_sec, size_per_sec) ) =>
          dto.ack_item_rate = items_per_sec
          dto.ack_size_rate = size_per_sec
        case _ =>
      }
      rc.consumers.add(dto)
    }
    rc
  }

}
