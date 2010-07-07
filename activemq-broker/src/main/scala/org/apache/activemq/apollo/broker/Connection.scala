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

import _root_.java.beans.ExceptionListener
import _root_.java.io.{IOException}
import _root_.org.apache.activemq.filter.{BooleanExpression}
import _root_.org.apache.activemq.transport._
import _root_.org.apache.activemq.Service
import _root_.java.lang.{String}
import _root_.org.apache.activemq.util.{FactoryFinder}
import _root_.org.apache.activemq.wireformat.WireFormat
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import java.util.concurrent.atomic.AtomicLong
import org.fusesource.hawtdispatch.Dispatch

object Connection extends Log {
  val id_generator = new AtomicLong()
  def next_id = "connection:"+id_generator.incrementAndGet
}

abstract class Connection() extends TransportListener with Service  with DispatchLogging {

  override protected def log = Connection

  import Connection._
  val id = next_id
  val dispatchQueue = createQueue(id)
  dispatchQueue.setTargetQueue(Dispatch.getRandomThreadQueue)
  
  var name = "connection"
  var stopped = false;
  var transport:Transport = null

  def start() = start(null)

  def start(onCompleted:Runnable) = {
    transport.setDispatchQueue(dispatchQueue);
    transport.setTransportListener(Connection.this);
    transport.start(onCompleted)
  }

  def stop() = stop(null)

  def stop(onCompleted:Runnable) = {
    if( !stopped ) {
      stopped=true
      transport.stop()
      dispatchQueue.setDisposer(onCompleted)
      dispatchQueue.release
    }
  }

  def onTransportFailure(error:IOException) = {
    if (!stopped) {
        onFailure(error);
    }
  }

  def onFailure(error:Exception) = {
    warn(error)
    transport.stop
  }

  def onTransportDisconnected() = {
  }

  def onTransportConnected() = {
  }

}

class BrokerConnection(val broker: Broker) extends Connection {

  var protocol = "stomp"
  var protocolHandler: ProtocolHandler = null;

  override def start(onCompleted:Runnable) = {
    broker.dispatchQueue.retain
    protocolHandler = ProtocolHandlerFactory.createProtocolHandler(protocol)
    protocolHandler.setConnection(this);
    super.start(onCompleted)
  }

  override def stop(onCompleted:Runnable) = {
    if( !stopped ) {
      broker.runtime.stopped(this)
      broker.dispatchQueue.release
      super.stop(onCompleted)
    }
  }

  override def onTransportConnected() = protocolHandler.onTransportConnected

  override def onTransportDisconnected() = protocolHandler.onTransportDisconnected

  def onTransportCommand(command: Object) = {
    try {
      protocolHandler.onTransportCommand(command);
    } catch {
      case e:Exception =>
        onFailure(e)
    }
  }

  override def onTransportFailure(error: IOException) = protocolHandler.onTransportFailure(error)
}

class ProtocolException(message:String, e:Throwable=null) extends Exception(message, e) 

class MultiProtocolHandler extends ProtocolHandler {

  var connected = false

  def onTransportCommand(command:Any) = {

    if (!command.isInstanceOf[WireFormat]) {
      throw new ProtocolException("First command should be a WireFormat");
    }

    var wireformat:WireFormat = command.asInstanceOf[WireFormat];
    val protocol = wireformat.getName()
    val protocolHandler = try {
      // Create the new protocol handler..
       ProtocolHandlerFactory.createProtocolHandler(protocol);
    } catch {
      case e:Exception=>
      throw new ProtocolException("No protocol handler available for protocol: " + protocol, e);
    }
    protocolHandler.setConnection(connection);

    // replace the current handler with the new one.
    connection.protocol = protocol
    connection.protocolHandler = protocolHandler
    connection.transport.suspendRead
    protocolHandler.onTransportConnected
  }

  override def onTransportConnected = {
    connection.transport.resumeRead
  }

}

object ProtocolHandlerFactory {
    val PROTOCOL_HANDLER_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/apollo/broker/protocol/");

    def createProtocolHandler(protocol:String) = {
        PROTOCOL_HANDLER_FINDER.newInstance(protocol).asInstanceOf[ProtocolHandler]
    }
}

trait ProtocolHandler extends TransportListener {

  var connection:BrokerConnection = null;

  def setConnection(brokerConnection:BrokerConnection) = {
    this.connection = brokerConnection
  }

  def onTransportCommand(command:Any);

  def onTransportFailure(error:IOException) = {
    connection.stop()
  }

  def onTransportDisconnected() = {
  }

  def onTransportConnected() = {
  }

}

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

