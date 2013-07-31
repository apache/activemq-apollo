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
import org.apache.activemq.apollo.util.{Log, ClassFinder}
import org.apache.activemq.apollo.broker.{Connector, Broker, Message, BrokerConnection}
import org.apache.activemq.apollo.dto.{SimpleProtocolFilterDTO, ProtocolFilterDTO, ConnectionStatusDTO}
import org.fusesource.hawtbuf.Buffer
import scala.collection.mutable.ListBuffer
import org.fusesource.hawtdispatch.transport.ProtocolCodec

trait Protocol extends ProtocolCodecFactory.Provider {
  def createProtocolHandler(connector: Connector):ProtocolHandler
}

abstract class BaseProtocol extends Protocol {
  def isIdentifiable = false
  def maxIdentificaionLength = throw new UnsupportedOperationException()
  def matchesIdentification(buffer: Buffer) = throw new UnsupportedOperationException()
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ProtocolFactory {

  private def finder = new ClassFinder[Protocol]("META-INF/services/org.apache.activemq.apollo/protocol-factory.index", classOf[Protocol])
  val protocols:Array[Protocol] = finder.singletons.toArray
  val protocols_by_id = Map(protocols.map(x=> (x.id, x)): _*)
  def get(name:String):Option[Protocol] = protocols_by_id.get(name)

}

trait MessageCodec {
  def id():String
  def encode(message:Message):MessageRecord
  def decode(message:MessageRecord):Message
}

object MessageCodecFactory {

  trait Provider {
    def create:Array[MessageCodec]
  }

  val codecs:Array[MessageCodec] = {
    val finder = new ClassFinder[Provider]("META-INF/services/org.apache.activemq.apollo/message-codec-factory.index", classOf[Provider])
    val rc = ListBuffer[MessageCodec]()
    for( provider <- finder.singletons; codec <- provider.create ) {
      rc += codec
    }
    rc.toArray
  }
  val codecs_by_id = Map(codecs.map(x=> (x.id, x)): _*)
  def apply(id:String) = codecs_by_id.get(id)
}

object ProtocolHandler extends Log

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait ProtocolHandler {
  import ProtocolHandler._

  def protocol:String

  def async_die(client_message:String)

  def session_id: String

  var connection:BrokerConnection = null;
  def defer(func: =>Unit) = connection.defer(func)

  def set_connection(brokerConnection:BrokerConnection) = {
    this.connection = brokerConnection
  }

  def create_connection_status(debug:Boolean) = new ConnectionStatusDTO

  def on_transport_failure(error:IOException) = {
    info(error)
    connection.stop(NOOP)
  }

  def on_transport_disconnected = {}

  def on_transport_connected = {}

  def on_transport_command(command: AnyRef) = {}

}

abstract class AbstractProtocolHandler extends ProtocolHandler

@deprecated(message="Please use the ProtocolFilter3 interface instead", since="1.3")
trait ProtocolFilter {
  def filter[T](command: T):T
}

object ProtocolFilter3Factory {

  val providers = new ClassFinder[Provider]("META-INF/services/org.apache.activemq.apollo/protocol-filter-factory.index",classOf[Provider])

  trait Provider {
    def create( dto:ProtocolFilterDTO, handler:ProtocolHandler ):ProtocolFilter3
  }

  def create( dto:ProtocolFilterDTO, handler:ProtocolHandler ):ProtocolFilter3 = {
    for( p <- providers.singletons ) {
      val rc = p.create(dto, handler)
      if( rc!=null ) {
        return rc;
      }  
    }
    throw new IllegalArgumentException("Cannot create a protocol filter for DTO: "+dto)
  }
}

object SimpleProtocolFilter3Factory extends ProtocolFilter3Factory.Provider {
  def create( dto:ProtocolFilterDTO, handler:ProtocolHandler ):ProtocolFilter3 = dto match {
    case dto:SimpleProtocolFilterDTO =>
      val instance = Broker.class_loader.loadClass(dto.kind).newInstance().asInstanceOf[AnyRef]
      val filter = instance match {
        case self:ProtocolFilter3 => self
        case self:ProtocolFilter2 => new ProtocolFilter3() {
          override def filter_inbound[T<:Object](command: T) = self.filter_inbound(command);
          override def filter_outbound[T<:Object](command: T) = self.filter_outbound(command);
        }
        case self:ProtocolFilter => new ProtocolFilter3() {
          override def filter_inbound[T<:Object](command: T) = self.filter(command)
          override def filter_outbound[T<:Object](command: T) = command
        }
        case null => null
        case _ => throw new IllegalArgumentException("Invalid protocol filter type: "+instance.getClass)
      }
      import language.reflectiveCalls
      type FilterDuckType = {
        var protocol_handler:ProtocolHandler
        var dto:SimpleProtocolFilterDTO
      }
      try {
        filter.asInstanceOf[FilterDuckType].protocol_handler = handler
      } catch { case _:Throwable => }
      try {
        filter.asInstanceOf[FilterDuckType].dto = dto
      } catch { case _:Throwable => }
      filter
    case _ => null
  }
}

object ProtocolFilter3 {
  def create_filters(dtos:java.util.List[ProtocolFilterDTO], handler:ProtocolHandler):java.util.List[ProtocolFilter3] = {
    import collection.JavaConversions._
    collection.JavaConversions.seqAsJavaList(create_filters(dtos.toList, handler));
  }
  def create_filters(dtos:List[ProtocolFilterDTO], handler:ProtocolHandler):List[ProtocolFilter3] = {
    dtos.map(ProtocolFilter3Factory.create(_, handler))
  }
}

/**
 * A Protocol filter can filter frames being sent/received to and from a client.  It can modify
 * the frame or even drop it.
 */
@deprecated(message="Please use the ProtocolFilter3 interface instead", since="1.7")
abstract class ProtocolFilter2 {

  /**
   * Filters a command frame received from a client.
   * returns None if the filter wants to drop the frame.
   */
  def filter_inbound[T](frame: T):T

  /**
   * Filters a command frame being sent client.
   * returns None if the filter wants to drop the frame.
   */
  def filter_outbound[T](frame: T):T
}

/**
 * A Protocol filter can filter frames being sent/received to and from a client.  It can modify
 * the frame or even drop it.
 */
abstract class ProtocolFilter3 {

  /**
   * Filters a command frame received from a client.
   * returns None if the filter wants to drop the frame.
   */
  def filter_inbound[T<:Object](frame: T):T

  /**
   * Filters a command frame being sent client.
   * returns None if the filter wants to drop the frame.
   */
  def filter_outbound[T<:Object](frame: T):T
}

