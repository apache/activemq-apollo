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

import org.apache.activemq.apollo.broker.{Message, ProtocolException}
import org.fusesource.hawtbuf.{AsciiBuffer, Buffer}
import org.apache.activemq.apollo.store.MessageRecord
import org.apache.activemq.apollo.transport.{ProtocolCodec}
import java.nio.channels.{WritableByteChannel, ReadableByteChannel}
import java.nio.ByteBuffer
import java.io.IOException
import java.lang.String
import java.util.concurrent.TimeUnit
import org.fusesource.hawtdispatch._

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class MultiProtocolFactory extends ProtocolFactory.Provider {

  def all_protocols: Array[Protocol] = ((ProtocolFactory.providers.map(_.create())).filter(_.isIdentifiable)).toArray

  def create() = {
    new MultiProtocol(()=>all_protocols)
  }

  def create(config: String): Protocol = {
    val MULTI = "multi"
    val MULTI_PREFIXED = "multi:"

    if (config == MULTI) {
      return new MultiProtocol(()=>all_protocols)
    } else if (config.startsWith(MULTI_PREFIXED)) {
      var names: Array[String] = config.substring(MULTI_PREFIXED.length).split(',')
      var protocols: Array[Protocol] = (names.flatMap {x => ProtocolFactory.get(x.trim)}).toArray
      return new MultiProtocol(()=>protocols)
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
class MultiProtocol(val func: ()=>Array[Protocol]) extends Protocol {

  lazy val protocols: Array[Protocol] = func()

  def protocol = "multi"

  def createProtocolCodec = new MultiProtocolCodec(protocols)

  def createProtocolHandler = new MultiProtocolHandler

  def encode(message: Message) = throw new UnsupportedOperationException

  def decode(message: MessageRecord) = throw new UnsupportedOperationException

  def isIdentifiable = false

  def maxIdentificaionLength = throw new UnsupportedOperationException()

  def matchesIdentification(buffer: Buffer) = throw new UnsupportedOperationException()

}

class MultiProtocolCodec(val protocols: Array[Protocol]) extends ProtocolCodec {

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
        protocolCodec.unread(buff)
        return protocolCodec
      }
    }
    if (buffer.position() == buffer.capacity) {
      channel = null
      throw new IOException("Could not identify the protocol.")
    }
    return null
  }

  def getReadCounter = buffer.position()

  def unread(buffer: Buffer) = throw new UnsupportedOperationException()

  def setWritableByteChannel(channel: WritableByteChannel) = {}

  def write(value: Any) = throw new UnsupportedOperationException()

  def flush = throw new UnsupportedOperationException()

  def getWriteCounter = 0L

  def protocol = "multi"

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class MultiProtocolHandler extends ProtocolHandler {

  def protocol = "multi"

  var discriminated = false

  override def onTransportCommand(command: Any) = {

    if (!command.isInstanceOf[ProtocolCodec]) {
      throw new ProtocolException("Expected a protocol codec");
    }

    discriminated = true

    var codec: ProtocolCodec = command.asInstanceOf[ProtocolCodec];
    val protocol = codec.protocol()
    val protocolHandler = ProtocolFactory.get(protocol) match {
      case Some(x) => x.createProtocolHandler
      case None =>
        throw new ProtocolException("No protocol handler available for protocol: " + protocol);
    }

    protocolHandler.setConnection(connection);

    // replace the current handler with the new one.
    connection.protocolHandler = protocolHandler
    connection.transport.setProtocolCodec(codec)

    connection.transport.suspendRead
    protocolHandler.onTransportConnected
  }

  override def onTransportConnected = {
    connection.transport.resumeRead
    
    // Make sure client connects eventually...
    connection.dispatchQueue.after(5, TimeUnit.SECONDS) {
      assert_discriminated
    }
  }

  def assert_discriminated = {
    if( connection.serviceState.isStarted && !discriminated ) {
      connection.stop
    }
  }

}

