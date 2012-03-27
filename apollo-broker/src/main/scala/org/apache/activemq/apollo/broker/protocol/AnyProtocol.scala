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
import org.apache.activemq.apollo.broker.store.MessageRecord
import org.fusesource.hawtdispatch.transport.ProtocolCodec
import java.nio.channels.{WritableByteChannel, ReadableByteChannel}
import java.nio.ByteBuffer
import java.io.IOException
import java.lang.String
import java.util.concurrent.TimeUnit
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.util.OptionSupport
import org.apache.activemq.apollo.broker.{Message, ProtocolException}
import org.apache.activemq.apollo.dto.{DetectDTO, AcceptingConnectorDTO}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class AnyProtocolFactory extends ProtocolFactory {

  def all_protocols: Array[Protocol] = ((ProtocolFactory.finder.singletons.map(_.create())).filter(_.isIdentifiable)).toArray

  def create() = {
    new AnyProtocol(()=>all_protocols)
  }

  def create(config: String): Protocol = {
    val MULTI = "any"
    val MULTI_PREFIXED = "any:"

    if (config == MULTI) {
      return new AnyProtocol(()=>all_protocols)
    } else if (config.startsWith(MULTI_PREFIXED)) {
      var names: Array[String] = config.substring(MULTI_PREFIXED.length).split(',')
      var protocols: Array[Protocol] = (names.flatMap {x => ProtocolFactory.get(x.trim)}).toArray
      return new AnyProtocol(()=>protocols)
    }
    return null
  }

}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class AnyProtocol(val func: ()=>Array[Protocol]) extends Protocol {

  lazy val protocols: Array[Protocol] = func()

  def id = "any"

  def createProtocolCodec = new AnyProtocolCodec(protocols)

  def createProtocolHandler = new AnyProtocolHandler

  def encode(message: Message) = throw new UnsupportedOperationException

  def decode(message: MessageRecord) = throw new UnsupportedOperationException

  def isIdentifiable = false

  def maxIdentificaionLength = throw new UnsupportedOperationException()

  def matchesIdentification(buffer: Buffer) = throw new UnsupportedOperationException()

}

case class ProtocolDetected(id:String, codec:ProtocolCodec)

class AnyProtocolCodec(val protocols: Array[Protocol]) extends ProtocolCodec {

  if (protocols.isEmpty) {
    throw new IllegalArgumentException("No protocol configured for identification.")
  }
  val buffer = ByteBuffer.allocate(protocols.foldLeft(0) {(a, b) => a.max(b.maxIdentificaionLength)})
  var channel: ReadableByteChannel = null

  def setReadableByteChannel(channel: ReadableByteChannel) = {this.channel = channel}

  def read: AnyRef = {
    if (channel == null) {
      throw new IllegalStateException
    }

    channel.read(buffer)
    val buff = new Buffer(buffer.array(), 0, buffer.position())
    protocols.foreach {protocol =>
      if (protocol.matchesIdentification(buff)) {
        val protocolCodec = protocol.createProtocolCodec()
        protocolCodec.unread(buff.toByteArray)
        return ProtocolDetected(protocol.id, protocolCodec)
      }
    }
    if (buffer.position() == buffer.capacity) {
      channel = null
      throw new IOException("Could not identify the protocol.")
    }
    return null
  }

  def getReadCounter = buffer.position()

  def unread(buffer: Array[Byte]) = throw new UnsupportedOperationException()

  def setWritableByteChannel(channel: WritableByteChannel) = {}

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

  def session_id = None

  var config:DetectDTO = _

  override def on_transport_command(command: AnyRef) = {

    if (!command.isInstanceOf[ProtocolDetected]) {
      throw new ProtocolException("Expected a ProtocolDetected object");
    }

    discriminated = true

    var protocol: ProtocolDetected = command.asInstanceOf[ProtocolDetected];
    val protocol_handler = ProtocolFactory.get(protocol.id) match {
      case Some(x) => x.createProtocolHandler
      case None =>
        throw new ProtocolException("No protocol handler available for protocol: " + protocol.id);
    }

     // replace the current handler with the new one.
    connection.protocol_handler = protocol_handler
    connection.transport.setProtocolCodec(protocol.codec)
    connection.transport.suspendRead

    protocol_handler.set_connection(connection);
    protocol_handler.on_transport_connected
  }

  override def on_transport_connected = {
    connection.transport.resumeRead
    import OptionSupport._
    import collection.JavaConversions._
    
    val connector_config = connection.connector.config.asInstanceOf[AcceptingConnectorDTO]
    config = connector_config.protocols.flatMap{ _ match {
      case x:DetectDTO => Some(x)
      case _ => None
    }}.headOption.getOrElse(new DetectDTO)
    
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

