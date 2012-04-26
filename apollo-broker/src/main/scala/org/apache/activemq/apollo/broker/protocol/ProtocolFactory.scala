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

import java.io.IOException
import org.apache.activemq.apollo.broker.store.MessageRecord
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.dto.ConnectionStatusDTO
import org.apache.activemq.apollo.util.{Log, ClassFinder}
import org.apache.activemq.apollo.broker.{Broker, Message, BrokerConnection}

trait ProtocolFactory {
  def create():Protocol
  def create(config:String):Protocol
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ProtocolFactory {

  val finder = new ClassFinder[ProtocolFactory]("META-INF/services/org.apache.activemq.apollo/protocol-factory.index",classOf[ProtocolFactory])

  def get(name:String):Option[Protocol] = {
    finder.singletons.foreach { provider=>
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

object ProtocolHandler extends Log

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait ProtocolHandler {
  import ProtocolHandler._

  def protocol:String

  def session_id:Option[String]

  var connection:BrokerConnection = null;

  def set_connection(brokerConnection:BrokerConnection) = {
    this.connection = brokerConnection
  }

  def create_connection_status = new ConnectionStatusDTO

  def on_transport_failure(error:IOException) = {
    trace(error)
    connection.stop(NOOP)
  }

  def on_transport_disconnected = {}

  def on_transport_connected = {}

  def on_transport_command(command: AnyRef) = {}

}

@deprecated(message="Please use the ProtocolFilter2 interface instead", since="1.3")
trait ProtocolFilter {
  def filter[T](command: T):T
}

object ProtocolFilter2 {

  def create_filters(clazzes:List[String], handler:ProtocolHandler) = {
    clazzes.map { clazz =>
      val filter = ProtocolFilter2(Broker.class_loader.loadClass(clazz).newInstance().asInstanceOf[AnyRef])

      type ProtocolHandlerAware = { var protocol_handler:ProtocolHandler }
      try {
        filter.asInstanceOf[ProtocolHandlerAware].protocol_handler = handler
      } catch { case _ => }

      filter
    }
  }

  /**
   * Allows you to convert any ProtocolFilter object into a ProtocolFilter2 object.
   */
  def apply(filter:AnyRef):ProtocolFilter2 = {
    filter match {
      case self:ProtocolFilter2 => self
      case self:ProtocolFilter => new ProtocolFilter2() {
        override def filter_inbound[T](command: T): Option[T] = Some(self.filter(command))
        override def filter_outbound[T](command: T): Option[T] = Some(command)
      } 
      case null => null
      case _ => throw new IllegalArgumentException("Invalid protocol filter type: "+filter.getClass)
    }
  }
}

/**
 * A Protocol filter can filter frames being sent/received to and from a client.  It can modify
 * the frame or even drop it.
 */
abstract class ProtocolFilter2 {

  /**
   * Filters a command frame received from a client.
   * returns None if the filter wants to drop the frame.
   */
  def filter_inbound[T](frame: T):Option[T]

  /**
   * Filters a command frame being sent client.
   * returns None if the filter wants to drop the frame.
   */
  def filter_outbound[T](frame: T):Option[T]
}

