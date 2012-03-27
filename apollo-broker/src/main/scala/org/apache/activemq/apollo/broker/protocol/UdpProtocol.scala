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

import org.apache.activemq.apollo.broker.store.MessageRecord
import org.fusesource.hawtdispatch.transport.ProtocolCodec
import java.nio.ByteBuffer
import org.fusesource.hawtdispatch._
import java.nio.channels.{DatagramChannel, WritableByteChannel, ReadableByteChannel}
import java.net.SocketAddress
import org.apache.activemq.apollo.dto.{UdpDTO, AcceptingConnectorDTO}
import org.fusesource.hawtbuf.{AsciiBuffer, Buffer}
import java.util.Map.Entry
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.broker.security.SecurityContext
import java.lang.String


/**
 * <p>
 *   A protocol factory for the UDP protocol.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class UdpProtocolFactory extends ProtocolFactory {

  def create() = UdpProtocol

  def create(config: String): Protocol = {
    if (config == "udp") {
      return UdpProtocol
    }
    return null
  }

}

/**
 * <p>
 *   The UDP protocol made for handling the UDP transport.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object UdpProtocol extends Protocol {

  def id = "udp"

  def createProtocolCodec = new UdpProtocolCodec()
  def createProtocolHandler = new UdpProtocolHandler
  def encode(message: Message) = throw new UnsupportedOperationException
  def decode(message: MessageRecord) = throw new UnsupportedOperationException
  def isIdentifiable = false
  def maxIdentificaionLength = throw new UnsupportedOperationException()
  def matchesIdentification(buffer: Buffer) = throw new UnsupportedOperationException()

}

case class UdpMessage(from:SocketAddress, buffer:ByteBuffer)

class UdpProtocolCodec extends ProtocolCodec {

  def protocol = UdpProtocol.id

  var channel: DatagramChannel = null
  def setReadableByteChannel(channel: ReadableByteChannel) = {
    this.channel = channel.asInstanceOf[DatagramChannel]
  }

  var read_counter = 0L
  var read_read_size = 0L

  def read: AnyRef = {
    if (channel == null) {
      throw new IllegalStateException
    }
    val buffer = ByteBuffer.allocate(channel.socket().getReceiveBufferSize)
    val from = channel.receive(buffer)
    if( from == null ) {
      null
    } else {
      buffer.flip()
      read_read_size = buffer.remaining()
      read_counter += read_read_size
      UdpMessage(from, buffer)
    }
  }

  def getLastReadSize = read_read_size
  def getReadCounter = read_counter
  def getReadBufferSize = channel.socket().getReceiveBufferSize

  def unread(buffer: Array[Byte]) = throw new UnsupportedOperationException()

  // This protocol only supports receiving..
  def setWritableByteChannel(channel: WritableByteChannel) = {}
  def write(value: AnyRef) = ProtocolCodec.BufferState.FULL
  def full: Boolean = true
  def flush = ProtocolCodec.BufferState.FULL
  def getWriteCounter = 0L
  def getLastWriteSize = 0
  def getWriteBufferSize = 0

}

trait UdpDecoder {
  def init(handler:UdpProtocolHandler)
  def address(message:UdpMessage):AsciiBuffer
  def decode_addresses(value:AsciiBuffer):Array[SimpleAddress]
  def decode_delivery(message:UdpMessage):Delivery
}

class DefaultUdpDecoder extends UdpDecoder {

  var topic_address:AsciiBuffer = _
  var topic_address_decoded:Array[SimpleAddress] = _

  def init(handler:UdpProtocolHandler) = {
    val topic_name = Option(handler.config.topic).getOrElse("udp")
    topic_address_decoded = LocalRouter.destination_parser.decode_multi_destination(topic_name, (name)=> LocalRouter.destination_parser.decode_single_destination("topic:"+name, null))
    topic_address = new AsciiBuffer(LocalRouter.destination_parser.encode_destination(topic_address_decoded))
  }

  def address(message: UdpMessage) = topic_address
  def decode_addresses(value: AsciiBuffer) = topic_address_decoded
  def decode_delivery(message: UdpMessage) = {
    val delivery = new Delivery
    delivery.size = message.buffer.remaining()
    delivery.message = RawMessage(new Buffer(message.buffer))
    delivery
  }
}

object UdpProtocolHandler extends Log
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class UdpProtocolHandler extends ProtocolHandler {
  import UdpProtocolHandler._

  def protocol = UdpProtocol.id
  def session_id = None

  var decoder:UdpDecoder = _
  var buffer_size = 0
  var host:VirtualHost = _
  var connection_log:Log = _
  var config:UdpDTO = _

  def broker = connection.connector.broker
  def queue = connection.dispatch_queue

  override def on_transport_connected = {
    connection.transport.resumeRead
    import collection.JavaConversions._

    config = (connection.connector.config match {
      case connector_config:AcceptingConnectorDTO =>
        connector_config.protocols.flatMap{ _ match {
          case x:UdpDTO => Some(x)
          case _ => None
        }}.headOption
      case _ => None
    }).getOrElse(new UdpDTO)

    val decoder_name = Option(config.decoder).getOrElse(classOf[DefaultUdpDecoder].getName)
    decoder = try {
      this.getClass.getClassLoader.loadClass(decoder_name).newInstance().asInstanceOf[UdpDecoder]
    } catch {
      case x =>
        warn(x)
        connection.stop(NOOP)
        new DefaultUdpDecoder
    }
    buffer_size = MemoryPropertyEditor.parse(Option(config.buffer_size).getOrElse("640k")).toInt
    decoder.init(this)

    broker.dispatch_queue {
      var host = broker.get_default_virtual_host
      queue {
        this.host = host
        connection_log = this.host.connection_log
        connection.transport.resumeRead()
        if(host==null) {
          warn("Could not find default virtual host")
          connection.stop(NOOP)
        }
      }
    }
    
  }

  var producerRoutes = new LRUCache[AsciiBuffer, StompProducerRoute](1000) {
    override def onCacheEviction(eldest: Entry[AsciiBuffer, StompProducerRoute]) = {
      host.router.disconnect(eldest.getValue.addresses, eldest.getValue)
    }
  }

  override def on_transport_command(command: AnyRef) = {
    val msg = command.asInstanceOf[UdpMessage]
    val address = decoder.address(msg)
    var route = producerRoutes.get(address);
    if( route == null ) {
      route = new StompProducerRoute(address)
      producerRoutes.put(address, route)
      val security_context = new SecurityContext
      security_context.connector_id = connection.connector.id
      security_context.local_address = connection.transport.getLocalAddress
      host.dispatch_queue {
        val rc = host.router.connect(route.addresses, route, security_context)
        if( rc.isDefined ) {

        }
      }
    }
    route.send(msg);
  }

  class StompProducerRoute(dest: AsciiBuffer) extends DeliveryProducerRoute(host.router) {
    val addresses = decoder.decode_addresses(dest)
    val key = addresses.toList
    
    override def send_buffer_size = buffer_size
    override def connection = Some(UdpProtocolHandler.this.connection)
    override def dispatch_queue = queue

    var inbound_queue_size = 0

    val sink_switch = new MutableSink[Delivery]()

    val inbound_queue = new OverflowSink[Delivery](sink_switch) {
      override protected def onDelivered(value: Delivery) = {
        inbound_queue_size -= value.size
      }
    }

    override protected def on_connected = {
      sink_switch.downstream = Some(this)
    }

    def send(frame:UdpMessage) = {
      // Drop older entries to make room for this new one..
      while( inbound_queue_size >= buffer_size ) {
        inbound_queue.removeFirst
      }
      
      val delivery = decoder.decode_delivery(frame)
      inbound_queue_size += delivery.size
      inbound_queue.offer(delivery)
    }
  }
  
}

