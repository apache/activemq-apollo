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
package org.apache.activemq.apollo.broker

import _root_.java.io.{IOException}
import _root_.java.lang.{String}
import org.fusesource.hawtdispatch._
import protocol.{ProtocolHandler}
import org.apache.activemq.apollo.filter.BooleanExpression
import org.fusesource.hawtdispatch.transport._
import org.apache.activemq.apollo.dto._
import org.apache.activemq.apollo.util.{DeferringDispatched, Dispatched, Log, BaseService}
import scala.Some
import java.security.cert.X509Certificate

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Connection extends Log
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract class Connection() extends BaseService with DeferringDispatched {
  import Connection._

  private var _dispatch_queue = createQueue()
  def dispatch_queue = _dispatch_queue

  def _set_dispatch_queue(next_queue:DispatchQueue, on_complete:Task) {
    set_dispatch_queue(next_queue) {
      on_complete.run()
    }
  }

  def set_dispatch_queue(next_queue:DispatchQueue)(on_complete: =>Unit) {
    _dispatch_queue {
      if(transport!=null) {
        transport.setDispatchQueue(next_queue)
      }
      _dispatch_queue = next_queue
      on_complete
    }
  }

  var stopped = true
  var transport:Transport = null
  var transport_sink:TransportSink = null

  override protected def _start(on_completed:Task) = {
    stopped = false
    transport_sink = new TransportSink(transport)
    transport.setDispatchQueue(dispatch_queue);
    transport.setTransportListener(new TransportListener(){
      def onTransportFailure(error: IOException) = Connection.this.on_transport_failure(error)
      def onTransportDisconnected = Connection.this.on_transport_disconnected
      def onTransportConnected =  Connection.this.on_transport_connected
      def onTransportCommand(command: AnyRef) =  Connection.this.on_transport_command(command)
      def onRefill =  Connection.this.on_refill
    });
    transport.start(on_completed)
  }

  override protected def _stop(on_completed:Task) = {
    stopped = true
    transport.stop(on_completed)
  }

  protected def on_transport_failure(error:IOException) = {
    if (!stopped) {
        on_failure(error);
    }
  }


  protected def on_refill = {
    if( transport_sink.refiller !=null ) {
      transport_sink.refiller.run
    }
  }

  protected def on_transport_command(command: Object) = {
  }

  protected def on_transport_connected() = {
  }

  protected def on_transport_disconnected() = {
  }


  protected def on_failure(error:Exception) = {
    warn(error)
    transport.stop(NOOP)
  }
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class BrokerConnection(var connector: Connector, val id:Long) extends Connection {
  import Connection._

  var protocol_handler: ProtocolHandler = null;

  def session_id = Option(protocol_handler).flatMap(x=> Option(x.session_id))
  override def toString = "id: "+id.toString

  protected override  def _start(on_completed:Task) = {
    protocol_handler.set_connection(this);
    super._start(on_completed)
  }

  protected override def _stop(on_completed:Task) = {
    connector.stopped(this)
    super._stop(on_completed)
  }

  protected override def on_transport_connected() = {
    connector.broker.connection_log.info("connected: local:%s, remote:%s", transport.getLocalAddress, transport.getRemoteAddress)
    protocol_handler.on_transport_connected
  }

  protected override def on_transport_disconnected() = {
    connector.broker.connection_log.info("disconnected: local:%s, remote:%s", transport.getLocalAddress, transport.getRemoteAddress)
    protocol_handler.on_transport_disconnected
  }

  protected override def on_transport_command(command: Object) = {
    try {
      protocol_handler.on_transport_command(command);
    } catch {
      case e:Exception =>
        on_failure(e)
    }
  }

  protected override def on_transport_failure(error: IOException) = protocol_handler.on_transport_failure(error)

  def get_connection_status(debug:Boolean=false) = {
    val result = if( protocol_handler==null ) {
      new ConnectionStatusDTO
    } else {
      protocol_handler.create_connection_status(debug)
    }

    result.id = id.toString
    result.state = service_state.toString
    result.state_since = service_state.since
    result.protocol = protocol_handler.protocol
    result.connector = connector.id
    result.remote_address = Option(transport.getRemoteAddress).map(_.toString).getOrElse(null)
    result.local_address = Option(transport.getLocalAddress).map(_.toString).getOrElse(null)
    result.protocol_session_id = protocol_handler.session_id
    val wf = transport.getProtocolCodec
    if( wf!=null ) {
      result.write_counter = wf.getWriteCounter
      result.read_counter = wf.getReadCounter
      result.last_read_size = wf.getLastReadSize
      result.last_write_size = wf.getLastWriteSize
    }
    result
  }

  def protocol_codec[T<:AnyRef](clazz:Class[T]):T = {
    var rc = transport.getProtocolCodec
    while( rc !=null ) {
      if( clazz.isInstance(rc) ) {
        return clazz.cast(rc);
      }
      rc = rc match {
        case rc:WrappingProtocolCodec => rc.getNext
        case _ => null
      }
    }
    return null.asInstanceOf[T]
  }

  def certificates = {
    (transport match {
      case ttransport:SecuredSession=>
        Option(ttransport.getPeerX509Certificates)
      case _ =>
        protocol_codec(classOf[SecuredSession]) match {
          case null => None
          case protocol_codec=> Option(protocol_codec.getPeerX509Certificates)
        }
    }).getOrElse(Array[X509Certificate]())
  }
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ProtocolException(message:String, e:Throwable=null) extends Exception(message, e)


/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait ConsumerContext { // extends ClientContext, Subscription<MessageDelivery>, IFlowSink<MessageDelivery> {

    def getConsumerId() : String

    def getDestination(): DestinationDTO

    def getSelector() : String

    def getSelectorExpression() : BooleanExpression

    def isDurable() : Boolean

    def getSubscriptionName() : String

    /**
     * If the destination does not exist, should it automatically be
     * created?
     *
     * @return
     */
    def autoCreateDestination():Boolean

    def isPersistent() : Boolean

}

