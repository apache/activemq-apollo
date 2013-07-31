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
package org.apache.activemq.apollo.broker

import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.broker.protocol.{ProtocolFactory, Protocol}
import org.fusesource.hawtdispatch.transport._
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.util.OptionSupport._
import java.net.SocketAddress
import org.apache.activemq.apollo.util.{Log, ClassFinder}
import org.apache.activemq.apollo.dto._
import security.SecuredResource
import transport.TransportFactory

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Connector extends Log {
}

trait Connector extends BaseService with SecuredResource {

  def broker:Broker
  def id:String
  def stopped(connection:BrokerConnection):Unit
  def config:ConnectorTypeDTO
  def accepted:LongCounter
  def connected:LongCounter
  def update(config: ConnectorTypeDTO, on_complete:Task):Unit
  def socket_address:SocketAddress
  def status:ServiceStatusDTO
  def resource_kind = SecuredResource.ConnectorKind
  def update_buffer_settings = {}

  def protocol_codec_config[T<:ProtocolDTO](clazz:Class[T]):Option[T] = {
    import collection.JavaConversions._
    val connector_config = config.asInstanceOf[AcceptingConnectorDTO]
    for( x <- connector_config.protocols ) {
      if( clazz.isInstance(x) ) {
        return Some(clazz.cast(x))
      }
    }
    return None
  }

}

trait ConnectorFactory {
  def create(broker:Broker, dto:ConnectorTypeDTO):Connector
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ConnectorFactory {

  val finder = new ClassFinder[ConnectorFactory]("META-INF/services/org.apache.activemq.apollo/connector-factory.index",classOf[ConnectorFactory])

  def create(broker:Broker, dto:ConnectorTypeDTO):Connector = {
    if( dto == null ) {
      return null
    }
    finder.singletons.foreach { provider=>
      val connector = provider.create(broker, dto)
      if( connector!=null ) {
        return connector;
      }
    }
    return null
  }
}

object AcceptingConnectorFactory extends ConnectorFactory with Log {

  def create(broker: Broker, dto: ConnectorTypeDTO): Connector = dto match {
    case dto:AcceptingConnectorDTO =>
      if( dto.getClass != classOf[AcceptingConnectorDTO] ) {
        // ignore sub classes of AcceptingConnectorDTO
        null;
      } else {
        val rc = new AcceptingConnector(broker, dto.id)
        rc.config = dto
        rc
      }
    case _ =>
      null
  }
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class AcceptingConnector(val broker:Broker, val id:String) extends Connector {

  import Connector._

  override val dispatch_queue = broker.dispatch_queue

  var config = new AcceptingConnectorDTO
  config.id = id
  config.bind = "tcp://0.0.0.:0"

  var transport_server:TransportServer = _
  var protocol:Protocol = _
  val accepted = new LongCounter()
  val connected = new LongCounter()

  override def toString = "connector: "+config.id

  def socket_address = Option(transport_server).map(_.getSocketAddress).getOrElse(null)

  var dead_messages_sent:Long = 0
  var dead_messages_received:Long = 0
  var dead_read_counter:Long = 0
  var dead_write_counter:Long = 0
  var receive_buffer_auto_tune = true;
  var send_buffer_auto_tune = true;

  def status = {
    val result = new ConnectorStatusDTO
    result.id = id.toString
    result.state = service_state.toString
    result.state_since = service_state.since
    result.connection_counter = accepted.get
    result.connected = connected.get
    result.protocol = Option(config.protocol).getOrElse("any")
    result.local_address = Option(socket_address).map(_.toString).getOrElse("any")

    result.messages_sent = dead_messages_sent
    result.messages_received = dead_messages_received
    result.read_counter = dead_read_counter
    result.write_counter = dead_write_counter

    for( (id, connection) <- broker.connections if connection.connector eq this ) {
      result.connections.add( new LongIdLabeledDTO(id, connection.transport.getRemoteAddress.toString ) )
      val status = connection.get_connection_status(false)
      if( status!=null ) {
        result.messages_sent += status.messages_sent
        result.messages_received += status.messages_received
        result.read_counter += status.read_counter
        result.write_counter += status.write_counter
      }
    }
    result
  }



  object BrokerAcceptListener extends TransportServerListener {
    def onAcceptError(e: Exception): Unit = {
      warn(e, "Error occured while accepting client connection.")
    }

    def onAccept(transport: Transport): Unit = {
      if( protocol!=null ) {
        transport.setProtocolCodec(protocol.createProtocolCodec(AcceptingConnector.this))
      }

      accepted.incrementAndGet
      connected.incrementAndGet()
      var connection = new BrokerConnection(AcceptingConnector.this, broker.connection_id_counter.incrementAndGet)
      connection.dispatch_queue.setLabel("connection %d to %s".format(connection.id, transport.getRemoteAddress))
      connection.protocol_handler = protocol.createProtocolHandler(AcceptingConnector.this)
      connection.transport = transport

      broker.connections.put(connection.id, connection)
      broker.current_period.max_connections = broker.current_period.max_connections.max(broker.connections.size)
      if ( broker.current_period.max_connections > broker.max_connections_in_5min ) {
        // re-tune the buffer settings if max is getting bumped.
        broker.tune_send_receive_buffers
      }

      try {
        connection.start(NOOP)
      } catch {
        case e1: Exception => {
          onAcceptError(e1)
        }
      }

      if(at_connection_limit) {
        // We stop accepting connections at this point.
        info("Connection limit reached. Clients connected: %d", connected.get)
        transport_server.suspend
      }
    }
  }

  def at_connection_limit = {
    connected.get >= config.connection_limit.getOrElse(Integer.MAX_VALUE)
  }

  /**
   */
  def update(config: ConnectorTypeDTO, on_completed:Task) = dispatch_queue {
    if ( !service_state.is_started || this.config == config ) {
      this.config = config.asInstanceOf[AcceptingConnectorDTO]
      on_completed.run
    } else {
      // if the connector config is updated.. lets stop, apply config, then restart
      // the connector.
      stop(^{
        this.config = config.asInstanceOf[AcceptingConnectorDTO]
        start(on_completed)
      })
    }
  }

  var send_buffer_size:Option[Int] = None
  var receive_buffer_size:Option[Int] = None

  override def _start(on_completed:Task) = {
    def mem_size(value:String) = Option(value).map(MemoryPropertyEditor.parse(_).toInt)

    assert(config!=null, "Connector must be configured before it is started.")
    receive_buffer_auto_tune = config.receive_buffer_auto_tune.getOrElse(true)
    send_buffer_auto_tune = config.send_buffer_auto_tune.getOrElse(true)

    accepted.set(0)
    connected.set(0)
    var protocol_name = config.protocol.getOrElse("any")
    protocol = ProtocolFactory.get(protocol_name).getOrElse(throw new Exception("protocol named '"+protocol_name+"' does not exist"))
    transport_server = TransportFactory.bind( config.bind )
    transport_server.setDispatchQueue(dispatch_queue)
    transport_server.setBlockingExecutor(Broker.BLOCKABLE_THREAD_POOL);
    transport_server.setTransportServerListener(BrokerAcceptListener)

    transport_server match {
      case transport_server:BrokerAware =>
        transport_server.set_broker(broker)
      case _ =>
    }

    transport_server match {
      case transport_server:SslTransportServer =>
        if( broker.key_storage!=null ) {
          transport_server.setTrustManagers(broker.key_storage.create_trust_managers)
          transport_server.setKeyManagers(broker.key_storage.create_key_managers)
        } else {
          warn("You are using a transport that expects the broker's key storage to be configured.")
        }
      case _ =>
    }

    update_buffer_settings

    transport_server.start(^{
      broker.console_log.info("Accepting connections at: "+transport_server.getBoundAddress)
      on_completed.run
    })
  }

  var last_receive_buffer_size = 0
  var last_send_buffer_size = 0

  //
  // This method get call once a second to re-tune the socket buffer sizes if needed.
  //
  override
  def update_buffer_settings {
    transport_server match {
      case transport_server: TcpTransportServer =>

        if(receive_buffer_auto_tune){
          val next_receive_buffer_size = broker.auto_tuned_send_receiver_buffer_size
          if( next_receive_buffer_size!=last_receive_buffer_size ) {
            debug("%s connector receive_buffer_size set to: %d", id, next_receive_buffer_size)

            // lets avoid updating the socket settings each period.
            transport_server.setReceiveBufferSize(next_receive_buffer_size)
            last_receive_buffer_size = next_receive_buffer_size
          }
        }


        if(send_buffer_auto_tune){
          val next_send_buffer_size = broker.auto_tuned_send_receiver_buffer_size
          if( next_send_buffer_size!=last_send_buffer_size ) {
            debug("%s connector send_buffer_size set to: %d", id, next_send_buffer_size)
            // lets avoid updating the socket settings each period.
            transport_server.setSendBufferSize(next_send_buffer_size)
            last_send_buffer_size = next_send_buffer_size
          }
        }


      case _ =>
    }
  }

  override def _stop(on_completed:Task): Unit = {
    transport_server.stop(^{
      broker.console_log.info("Stopped connector at: "+config.bind)
      transport_server = null
      protocol = null
      on_completed.run
    })
  }

  /**
   * Connections callback into the connector when they are stopped so that we can
   * stop tracking them.
   */
  def stopped(connection:BrokerConnection) = dispatch_queue {
    val at_limit = at_connection_limit
    if( broker.connections.remove(connection.id).isDefined ) {
      connected.decrementAndGet()
      val status = connection.get_connection_status(false)
      if( status!=null ) {
        dead_messages_sent += status.messages_sent
        dead_messages_received += status.messages_received
        dead_read_counter += status.read_counter
        dead_write_counter += status.write_counter
      }
      if( at_limit ) {
        transport_server.resume
      }
    }
  }

}
