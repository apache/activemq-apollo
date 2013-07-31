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

import org.fusesource.hawtdispatch.transport.{Transport, ProtocolCodec}
import java.nio.ByteBuffer
import org.fusesource.hawtdispatch._
import java.nio.channels.{DatagramChannel, WritableByteChannel, ReadableByteChannel}
import java.net.SocketAddress
import org.apache.activemq.apollo.dto.{ConnectionStatusDTO, ProtocolDTO, UdpDTO, AcceptingConnectorDTO}
import org.fusesource.hawtbuf.{AsciiBuffer, Buffer}
import java.util.Map.Entry
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.broker.security.SecurityContext
import scala.Some
import org.apache.activemq.apollo.broker.BlackHoleSink


case class UdpMessage(from:SocketAddress, buffer:ByteBuffer)

class UdpProtocolCodec extends ProtocolCodec {

  def protocol = "udp"

  var channel: DatagramChannel = null


  def setTransport(transport: Transport) {
    this.channel = transport.getReadChannel.asInstanceOf[DatagramChannel]
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
  def write(value: AnyRef) = ProtocolCodec.BufferState.FULL
  def full: Boolean = true
  def flush = ProtocolCodec.BufferState.FULL
  def getWriteCounter = 0L
  def getLastWriteSize = 0
  def getWriteBufferSize = 0

}

object UdpProtocolHandler extends Log

trait DecodedUdpMessage {

  /**
   * @return The virtual host the message should get routed to.
   * return null to route to the default virtual host
   */
  def host:VirtualHost

  /**
   * @return The destination name to route the message to.
   */
  def address:AsciiBuffer

  /**
   * @return The delivery to route.
   */
  def delivery:Delivery

  /**
   * @return The original UdpMesasge that the DecodedUdpMessage was
   * constructed from.
   */
  def message: UdpMessage

  /**
   * @return The SecurityContext to authenticate and authorize against. Return
   *         null if you want to bypass authentication and authorization.
   */
  def security_context:SecurityContext

  def size = message.buffer.remaining()

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract class UdpProtocolHandler extends ProtocolHandler {
  import UdpProtocolHandler._
  type ConfigTypeDTO <: ProtocolDTO
  def configClass:Class[ConfigTypeDTO]

  def protocol = "udp"
  var session_id:String = null

  def async_die(client_message:String) = null

  var buffer_size = 640*1024
  var connection_log:Log = _
  var messages_received = 0L
  var waiting_on = "client request"

  def broker = connection.connector.broker
  def queue = connection.dispatch_queue

  override def create_connection_status(debug:Boolean) = {
    var rc = new ConnectionStatusDTO
    rc.waiting_on = waiting_on
    rc.messages_received = messages_received
    rc
  }

  def configure(config:Option[ConfigTypeDTO]) = {}

  override def on_transport_connected = {
    connection.transport.resumeRead
    import collection.JavaConversions._
    session_id = "%s-%x".format(broker.default_virtual_host.config.id, broker.default_virtual_host.session_counter.incrementAndGet)

    configure(connection.connector.config match {
      case connector_config:AcceptingConnectorDTO =>
        connector_config.protocols.flatMap{ x=>
          if( x.getClass == configClass) {
            Some(x.asInstanceOf[ConfigTypeDTO])
          } else {
            None
          }
        }.headOption
      case _ => None
    })
  }

  def suspend_read(reason: String) = {
    waiting_on = reason
    connection.transport.suspendRead
  }

  def resume_read() = {
    waiting_on = "client request"
    connection.transport.resumeRead
  }


  var producerRoutes = new LRUCache[(VirtualHost, AsciiBuffer, SecurityContext#Key), UdpProducerRoute](1000) {
    override def onCacheEviction(eldest: Entry[(VirtualHost, AsciiBuffer, SecurityContext#Key), UdpProducerRoute]) = {
      val (host, address, key) = eldest.getKey
      val route = eldest.getValue
      host.dispatch_queue {
        host.router.disconnect(route.addresses, route)
      }
    }
  }

  override def on_transport_command(command: AnyRef):Unit = {
    decode(command.asInstanceOf[UdpMessage]) match {
      case Some(msg) =>
        messages_received += 1
        val address = msg.address
        var host = msg.host
        if( host == null ) {
          host = broker.default_virtual_host
        }
        val security_context = msg.security_context
        var sc_key = if( security_context!=null) security_context.to_key else null
        var route = producerRoutes.get((host, address, sc_key));
        if( route == null ) {
          try {
            route = new UdpProducerRoute(host, address)
          } catch {
            case e:Throwable =>
              // We could run into a error like the address not parsing
              debug(e, "Could not create the producer route")
              return
          }
          producerRoutes.put((host, address, sc_key), route)

          def fail_connect = {
            // Just drop messages..
            route.sink_switch.downstream = Some(BlackHoleSink())
          }

          def continue_connect = host.dispatch_queue {
            host.router.connect(route.addresses, route, security_context) match {
              case Some(error) => queue {
                debug("Could not connect the producer route: "+error)
                fail_connect
              }
              case None =>
            }
          }

          if( security_context!=null && host.authenticator!=null &&  host.authorizer!=null ) {
            suspend_read("authenticating")
            host.authenticator.authenticate(security_context) { auth_failure=>
              queue {
                resume_read
                auth_failure match {
                  case null=> continue_connect
                  case auth_failure=>
                    debug("Producer route failed authentication: "+auth_failure)
                    fail_connect
                }
              }
            }
          } else {
            continue_connect
          }
        }

        route.send(msg)

      case None =>
    }
  }

  class UdpProducerRoute(host:VirtualHost, dest: AsciiBuffer) extends DeliveryProducerRoute(host.router) {
    val addresses = decode_address(dest.toString)
    val key = addresses.toList
    
    override def send_buffer_size = buffer_size
    override def connection = Some(UdpProtocolHandler.this.connection)
    override def dispatch_queue = queue

    var inbound_queue_size = 0

    val sink_switch = new MutableSink[Delivery]()

    val inbound_queue = new OverflowSink[DecodedUdpMessage](sink_switch.map(_.delivery)) {
      override protected def onDelivered(value: DecodedUdpMessage) = {
        inbound_queue_size -= value.size
      }
    }

    override protected def on_connected = {
      sink_switch.downstream = Some(this)
    }

    def send(frame:DecodedUdpMessage) = {
      // Drop older entries to make room for this new one..
      while( inbound_queue_size >= buffer_size ) {
        inbound_queue.removeFirst
      }
      
      inbound_queue_size += frame.size
      inbound_queue.offer(frame)
    }
  }

  def decode(message: UdpMessage):Option[DecodedUdpMessage]
  def decode_address(address:String):Array[SimpleAddress]
}

/**
 * <p>
 *   The UDP protocol made for handling the UDP transport.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class UdpProtocol extends BaseProtocol {

  def id = "udp"
  def createProtocolCodec(connector:Connector):ProtocolCodec = new UdpProtocolCodec()

  def createProtocolHandler(connector:Connector):ProtocolHandler = new UdpProtocolHandler {
    type ConfigTypeDTO = UdpDTO
    def configClass = classOf[ConfigTypeDTO]

    var default_host:VirtualHost = _
    var topic_address:AsciiBuffer = _

    override def configure(c: Option[ConfigTypeDTO]) = {
      val config = c.getOrElse(new ConfigTypeDTO)
      buffer_size = MemoryPropertyEditor.parse(Option(config.buffer_size).getOrElse("640k")).toInt
      val topic_address_decoded = decode_address(Option(config.topic).getOrElse("udp"))
      topic_address = new AsciiBuffer(LocalRouter.destination_parser.encode_destination(topic_address_decoded))
      default_host = broker.default_virtual_host
    }

    def decode_address(address:String):Array[SimpleAddress] = {
      LocalRouter.destination_parser.decode_multi_destination(address, (name)=> LocalRouter.destination_parser.decode_single_destination("topic:"+name, null))
    }

    def decode(udp: UdpMessage) = Some(new DecodedUdpMessage {
      def message = udp
      def host = null
      def security_context = null
      def address = topic_address

      def delivery = {
        val delivery = new Delivery
        delivery.size = message.buffer.remaining()
        delivery.message = RawMessage(new Buffer(message.buffer))
        delivery
      }
    })

  }
}
