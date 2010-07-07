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

import org.apache.activemq.transport.DefaultTransportListener
import java.io.{IOException}
import org.apache.activemq.apollo.broker.{Message, BrokerConnection}
import org.fusesource.hawtbuf.{Buffer, AsciiBuffer}
import org.apache.activemq.wireformat.WireFormat
import org.apache.activemq.apollo.util.ClassFinder


/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ProtocolFactory {

  val finder =  ClassFinder[Protocol]("META-INF/services/org.apache.activemq.apollo/protocols")
  var protocols = Map[AsciiBuffer, Protocol]()

  finder.find.foreach{ clazz =>
    try {

      val protocol = clazz.newInstance.asInstanceOf[Protocol]
      protocols += protocol.name -> protocol

    } catch {
      case e:Throwable =>
        e.printStackTrace
    }
  }

  def get(name:String):Protocol = get(new AsciiBuffer(name))

  def get(name:AsciiBuffer):Protocol = {
    protocols.get(name) match {
      case None =>
        throw new IllegalArgumentException("Protocol not found: "+name)
      case Some(x) => x
    }
  }

}

trait Protocol {

  def name:AsciiBuffer

  def createWireFormat:WireFormat
  def createProtocolHandler:ProtocolHandler

  def encode(message:Message):Buffer
  def decode(message:Buffer):Message

}


/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait ProtocolHandler extends DefaultTransportListener {

  var connection:BrokerConnection = null;

  def setConnection(brokerConnection:BrokerConnection) = {
    this.connection = brokerConnection
  }

  override def onTransportFailure(error:IOException) = {
    connection.stop()
  }

}
