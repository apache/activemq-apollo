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
package org.apache.activemq.apollo.broker.protocol

import org.fusesource.hawtbuf.Buffer
import java.nio.channels.ReadableByteChannel
import java.nio.ByteBuffer
import java.io.IOException
import java.lang.String
import java.util.concurrent.TimeUnit
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.util.OptionSupport
import org.apache.activemq.apollo.broker.{Connector, ProtocolException}
import org.apache.activemq.apollo.dto.{DetectDTO, AcceptingConnectorDTO}
import org.fusesource.hawtdispatch.transport.{WrappingProtocolCodec, Transport, ProtocolCodec}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object AnyProtocol extends BaseProtocol {

  def id = "any"

  def createProtocolCodec(connector:Connector) = new AnyProtocolCodec(connector)

  def createProtocolHandler(connector:Connector) = new AnyProtocolHandler

  def change_protocol_codec(transport:Transport, codec:ProtocolCodec) = {
    var current = transport.getProtocolCodec
    var wrapper:WrappingProtocolCodec = null
    while( current!=null ) {
      current = current match {
        case current:WrappingProtocolCodec =>
          wrapper = current
          current.getNext
        case _ => null
      }
    }
    if( wrapper!=null ) {
      wrapper.setNext(codec)
    } else {
      transport.setProtocolCodec(codec)
    }
  }

}

case class ProtocolDetected(id:String)

class AnyProtocolCodec(val connector:Connector) extends ProtocolCodec {

  var protocols =  ProtocolFactory.protocols.filter(_.isIdentifiable)

  if (protocols.isEmpty) {
    throw new IllegalArgumentException("No protocol configured for identification.")
  }
  val buffer = ByteBuffer.allocate(protocols.foldLeft(0) {(a, b) => a.max(b.maxIdentificaionLength)})
  def channel = transport.getReadChannel

  var transport:Transport = _
  def setTransport(t: Transport) =  transport = t

  var next:ProtocolCodec = _



  def read: AnyRef = {
    if (next!=null) {
      return next.read()
    }

    channel.read(buffer)
    val buff = new Buffer(buffer.array(), 0, buffer.position())
    protocols.foreach {protocol =>
      if (protocol.matchesIdentification(buff)) {
        return ProtocolDetected(protocol.id)
      }
    }
    if (buffer.position() == buffer.capacity) {
      throw new IOException("Could not identify the protocol.")
    }
    return null
  }

  def getReadCounter = buffer.position()

  def unread(buffer: Array[Byte]) = throw new UnsupportedOperationException()

  def write(value: Any) = ProtocolCodec.BufferState.FULL

  def full: Boolean = true

  def flush = ProtocolCodec.BufferState.FULL

  def getWriteCounter = 0L

  def protocol = "any"

  def getLastWriteSize = 0

  def getLastReadSize = 0

  def getWriteBufferSize = 0

  def getReadBufferSize = buffer.capacity()

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class AnyProtocolHandler extends ProtocolHandler {

  def protocol = "any"

  var discriminated = false

  def session_id = null
  var config:DetectDTO = _

  def async_die(client_message:String) = connection.stop(NOOP)

  override def on_transport_command(command: AnyRef) = {
    if( discriminated ) {
      throw new ProtocolException("Protocol already discriminated");
    }

    command match {
      case detected:ProtocolDetected =>
        discriminated = true
        val protocol = ProtocolFactory.get(detected.id).getOrElse(throw new ProtocolException("No protocol handler available for protocol: " + detected.id))
        val protocol_handler = protocol.createProtocolHandler(connection.connector)

        // Swap out the protocol codec
        val any_codec = connection.protocol_codec(classOf[AnyProtocolCodec])
        val next = protocol.createProtocolCodec(connection.connector)
        AnyProtocol.change_protocol_codec(connection.transport, next)
        val buff = new Buffer(any_codec.buffer.array(), 0, any_codec.buffer.position())
        next.unread(buff.toByteArray)

        // Swap out the protocol handler.
        connection.protocol_handler = protocol_handler
        connection.transport.suspendRead
        protocol_handler.set_connection(connection);
        connection.transport.getTransportListener.onTransportConnected()

      case _ =>
        throw new ProtocolException("Expected a ProtocolDetected object");
    }
  }

  override def on_transport_connected = {
    connection.transport.resumeRead
    import OptionSupport._
    import collection.JavaConversions._

    var codec = connection.protocol_codec(classOf[AnyProtocolCodec])

    val connector_config = connection.connector.config.asInstanceOf[AcceptingConnectorDTO]
    config = connector_config.protocols.flatMap{ _ match {
      case x:DetectDTO => Some(x)
      case _ => None
    }}.headOption.getOrElse(new DetectDTO)

    if( config.protocols!=null ) {
      val protocols = Set(config.protocols.split("\\s+").filter( _.length!=0 ):_*)
      codec.protocols = codec.protocols.filter(x=> protocols.contains(x.id))
    }

    val timeout = config.timeout.getOrElse(5000L)
  
    // Make sure client connects eventually...
    connection.dispatch_queue.after(timeout, TimeUnit.MILLISECONDS) {
      assert_discriminated
    }
  }

  def assert_discriminated = {
    if( connection.service_state.is_started && !discriminated ) {
      connection.stop(NOOP)
    }
  }

}

