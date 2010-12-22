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
import org.apache.activemq.apollo.util.{Log, BaseService}
import org.apache.activemq.apollo.filter.BooleanExpression
import org.apache.activemq.apollo.dto.ConnectionStatusDTO
import org.apache.activemq.apollo.transport.{TransportListener, DefaultTransportListener, Transport}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Connection extends Log
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract class Connection() extends BaseService  {
  import Connection._

  val dispatch_queue = createQueue()
  var stopped = true
  var transport:Transport = null
  var transport_sink:TransportSink = null

  override protected def _start(on_completed:Runnable) = {
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

  override protected def _stop(on_completed:Runnable) = {
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
    transport.stop
  }
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class BrokerConnection(val connector: Connector, val id:Long) extends Connection {

  var protocol_handler: ProtocolHandler = null;

  override def toString = "id: "+id.toString

  protected override  def _start(on_completed:Runnable) = {
    connector.dispatch_queue.retain
    protocol_handler.set_connection(this);
    super._start(on_completed)
  }

  protected override def _stop(on_completed:Runnable) = {
    connector.stopped(this)
    connector.dispatch_queue.release
    super._stop(on_completed)
  }

  protected override def on_transport_connected() = protocol_handler.on_transport_connected

  protected override def on_transport_disconnected() = protocol_handler.on_transport_disconnected

  protected override def on_transport_command(command: Object) = {
    try {
      protocol_handler.on_transport_command(command);
    } catch {
      case e:Exception =>
        on_failure(e)
    }
  }

  protected override def on_transport_failure(error: IOException) = protocol_handler.on_transport_failure(error)

  def get_connection_status = {
    val result = if( protocol_handler==null ) {
      new ConnectionStatusDTO
    } else {
      protocol_handler.create_connection_status
    }

    result.id = id
    result.state = service_state.toString
    result.state_since = service_state.since
    result.protocol = protocol_handler.protocol
    result.transport = transport.getTypeId
    result.remote_address = transport.getRemoteAddress
    val wf = transport.getProtocolCodec
    if( wf!=null ) {
      result.write_counter = wf.getWriteCounter
      result.read_counter = wf.getReadCounter
    }
    result
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

    def getDestination(): Destination

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

