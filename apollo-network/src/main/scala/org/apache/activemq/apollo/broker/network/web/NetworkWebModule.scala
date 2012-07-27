package org.apache.activemq.apollo.broker.network.web

import org.apache.activemq.apollo.web.resources.{HelpResourceJSON, Resource}
import org.apache.activemq.apollo.web.WebModule
import javax.ws.rs.{PathParam, Produces, GET, Path}
import scala.Array
import javax.ws.rs.core.MediaType._
import com.wordnik.swagger.core.{Api, ApiOperation}
import org.apache.activemq.apollo.dto.{DestinationLoadDTO, LoadStatusDTO}
import org.apache.activemq.apollo.broker.LocalRouter
import org.fusesource.hawtdispatch.Future
import scala.Predef._
import org.apache.activemq.apollo.util.{FutureResult, Success}
import FutureResult._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */


object NetworkWebModule extends WebModule {

  def priority: Int = 101

  override def web_resources = Set(
    classOf[NetworkResourceJSON],
    classOf[NetworkResourceHelp]
  )

  def root_redirect: String = "broker"

}

@Path(          "/api/docs/network{ext:(\\.json)?}")
@Api(value =    "/api/json/network",
  listingPath = "/api/docs/network",
  listingClass = "org.apache.activemq.apollo.web.resources.NetworkResourceJSON")
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
            value.load_status
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

}
