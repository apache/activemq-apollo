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

import java.io.{IOException}
import org.fusesource.hawtbuf.{Buffer, AsciiBuffer}
import org.apache.activemq.apollo.util.ClassFinder
import org.apache.activemq.apollo.store.MessageRecord
import org.apache.activemq.apollo.transport._
import org.apache.activemq.apollo.broker.{Delivery, Message, BrokerConnection}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ProtocolFactory {

  trait Provider {
    def create():Protocol
    def create(config:String):Protocol
  }

  def discover = {
    val finder = new ClassFinder[Provider]("META-INF/services/org.apache.activemq.apollo/protocol-factory.index")
    finder.new_instances
  }

  var providers = discover

  def get(name:String):Option[Protocol] = {
    providers.foreach { provider=>
      val rc = provider.create(name)
      if( rc!=null ) {
        return Some(rc)
      }
    }
    None
  }
}

trait Protocol extends ProtocolCodecFactory.Provider {

  def createProtocolHandler:ProtocolHandler
  def encode(message:Message):MessageRecord
  def decode(message:MessageRecord):Message

}


/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait ProtocolHandler extends DefaultTransportListener {

  def protocol:String

  var connection:BrokerConnection = null;

  def setConnection(brokerConnection:BrokerConnection) = {
    this.connection = brokerConnection
  }

  override def onTransportFailure(error:IOException) = {
    connection.stop()
  }

}
