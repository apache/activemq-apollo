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
import org.apache.activemq.wireformat.{MultiWireFormatFactory, WireFormat}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class MultiProtocol extends Protocol {

  val wff = new MultiWireFormatFactory

  def name = new AsciiBuffer("multi")

  def createWireFormat = wff.createWireFormat
  def createProtocolHandler = new MultiProtocolHandler
  
  def encode(message: Message) = throw new UnsupportedOperationException
  def decode(message: Buffer) = throw new UnsupportedOperationException
}


/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class MultiProtocolHandler extends ProtocolHandler {

  var connected = false

  override def onTransportCommand(command:Any) = {

    if (!command.isInstanceOf[WireFormat]) {
      throw new ProtocolException("First command should be a WireFormat");
    }

    var wireformat:WireFormat = command.asInstanceOf[WireFormat];
    val protocol = wireformat.getName()
    val protocolHandler = try {
      // Create the new protocol handler..
       ProtocolFactory.get(protocol).createProtocolHandler
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
