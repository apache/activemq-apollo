/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.amqp

import java.util.concurrent.TimeUnit
import collection.mutable.{ListBuffer, HashMap}

import org.fusesource.hawtdispatch._
import org.fusesource.hawtbuf._

import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.dto._
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.util.path.{PathParser, Path}
import path.LiteralPart
import protocol.ProtocolHandler
import org.apache.activemq.apollo.broker.security.SecurityContext
import org.apache.activemq.apollo.amqp.dto._
import org.fusesource.hawtbuf.Buffer._
import org.apache.activemq.apollo.broker.Delivery
import org.apache.activemq.apollo.filter.{FilterException, BooleanExpression}
import org.apache.activemq.apollo.selector.SelectorParser
import java.util
import java.io.IOException
import org.apache.activemq.apollo.broker.Session
import store.StoreUOW
import org.apache.activemq.apollo.broker.FullSink
import org.apache.activemq.apollo.broker.SubscriptionAddress

import org.apache.qpid.proton.engine
import org.apache.qpid.proton.framing.TransportFrame
import org.apache.qpid.proton.hawtdispatch.impl.{AmqpListener, AmqpTransport, AmqpProtocolCodec}
import org.apache.qpid.proton.engine._
import org.apache.qpid.proton.engine.impl.{ProtocolTracer, DeliveryImpl, LinkImpl, TransportImpl}
import org.apache.qpid.proton.amqp
import amqp.{Symbol => AmqpSymbol, UnsignedInteger, Binary, DescribedType}
import amqp.transport.SenderSettleMode
import amqp.messaging._
import amqp.transaction._
import org.apache.qpid.proton.message.impl.MessageImpl

object AmqpProtocolHandler extends Log {

  // How long we hold a failed connection open so that the remote end
  // can get the resulting error message.
  val DEFAULT_DIE_DELAY = 5 * 1000L
  val WAITING_ON_CLIENT_REQUEST = () => "client request"

  val DEFAULT_DESTINATION_PARSER = new DestinationParser
  DEFAULT_DESTINATION_PARSER.queue_prefix = "queue://"
  DEFAULT_DESTINATION_PARSER.topic_prefix = "topic://"
  DEFAULT_DESTINATION_PARSER.dsub_prefix = "dsub://"
  DEFAULT_DESTINATION_PARSER.temp_queue_prefix = "temp-queue://"
  DEFAULT_DESTINATION_PARSER.temp_topic_prefix = "temp-topic://"
  DEFAULT_DESTINATION_PARSER.destination_separator = ","
  DEFAULT_DESTINATION_PARSER.path_separator = "."
  DEFAULT_DESTINATION_PARSER.any_child_wildcard = "*"
  DEFAULT_DESTINATION_PARSER.any_descendant_wildcard = "**"

  val COPY = org.apache.qpid.proton.amqp.Symbol.getSymbol("copy");

  val JMS_SELECTOR = AmqpSymbol.valueOf("jms-selector")
  val NO_LOCAL = AmqpSymbol.valueOf("no-local");
  val ORIGIN = AmqpSymbol.valueOf("origin");

  val EMPTY_BYTE_ARRAY = Array[Byte]()

  def toBytes(value: Long): Array[Byte] = {
    val buffer: Buffer = new Buffer(8)
    buffer.bigEndianEditor.writeLong(value)
    return buffer.data
  }

  private def toLong(value: Binary): Long = {
    val buffer: Buffer = new Buffer(value.getArray, value.getArrayOffset, value.getLength)
    return buffer.bigEndianEditor.readLong
  }
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class AmqpProtocolHandler extends ProtocolHandler {

  import AmqpProtocolHandler._

  val security_context = new SecurityContext

  var connection_log: Log = AmqpProtocolHandler
  var host: VirtualHost = null
  var waiting_on = WAITING_ON_CLIENT_REQUEST
  var config: AmqpDTO = _
  var dead = false
  var protocol_convert = "full"
  var prefetch = 100

  def session_id = security_context.session_id

  def protocol = AmqpProtocol.id

  def broker = connection.connector.broker

  def queue = connection.dispatch_queue

  def die_delay = {
    OptionSupport(config.die_delay).getOrElse(DEFAULT_DIE_DELAY)
  }

  lazy val buffer_size = MemoryPropertyEditor.parse(Option(config.buffer_size).getOrElse("640k")).toInt
  var messages_sent = 0L
  var messages_received = 0L

  override def create_connection_status(debug:Boolean) = {
    var rc = new AmqpConnectionStatusDTO
    rc.protocol_version = "1.0.0"
    rc.user = security_context.user
    //    rc.subscription_count = consumers.size
    rc.waiting_on = waiting_on()
    rc.messages_sent = messages_sent
    rc.messages_received = messages_received
    rc
  }

  class ProtocolException(msg: String) extends RuntimeException(msg)

  class Break extends RuntimeException

  private def async_die(error_code: String, msg: String, e: Throwable = null) = try {
    die(error_code, msg, e)
  } catch {
    case x: Break =>
  }

  def async_die(client_message:String) = async_die("system-error", client_message)

  private def die[T](error_code: String, msg: String, e: Throwable = null): T = {
    if (e != null) {
      connection_log.info(e, "AMQP connection '%s' error: %s", security_context.remote_address, msg, e)
    } else {
      connection_log.info("AMQP connection '%s' error: %s", security_context.remote_address, msg)
    }
    if (!dead) {
      dead = true
      waiting_on = () => "shutdown"
      connection.transport.resumeRead

      on_transport_disconnected()
      proton.setLocalError(amqp_error(error_code, msg));
      proton.close()
      pump_out

      // TODO: if there are too many open connections we should just close the connection
      // without waiting for the error to get sent to the client.
      queue.after(die_delay, TimeUnit.MILLISECONDS) {
        connection.stop(NOOP)
      }
    }
    throw new Break()
  }

  def amqp_error(name: String = "", message: String = "") = {
    if ( name==null || message == null ) {
      println("crap")
    }
    new EndpointError(name, message)
  }

  def suspend_read(reason: => String) = {
    waiting_on = reason _
    connection.transport.suspendRead
    // heart_beat_monitor.suspendRead
  }

  def resume_read() = {
    waiting_on = WAITING_ON_CLIENT_REQUEST
    connection.transport.resumeRead
    // heart_beat_monitor.resumeRead
  }

  var amqp_connection:AmqpTransport = _
  var amqp_trace = false

  def codec = connection.transport.getProtocolCodec.asInstanceOf[AmqpProtocolCodec]

  def proton = amqp_connection.connection()

  def pump_out = {
    queue.assertExecuting()
    amqp_connection.pumpOut()
  }

  override def on_transport_connected() = {} // should not get called
  override def on_transport_command(command: AnyRef): Unit = sys.error("should not get called")

  override def set_connection(connection: BrokerConnection) = {
    super.set_connection(connection)
    import collection.JavaConversions._

    security_context.connector_id = connection.connector.id
    security_context.certificates = connection.certificates
    security_context.local_address = connection.transport.getLocalAddress
    security_context.remote_address = connection.transport.getRemoteAddress

    val connector_config = connection.connector.config.asInstanceOf[AcceptingConnectorDTO]
    config = connector_config.protocols.find(_.isInstanceOf[AmqpDTO]).map(_.asInstanceOf[AmqpDTO]).getOrElse(new AmqpDTO)
    amqp_trace = OptionSupport(config.trace).getOrElse(amqp_trace)

    def mem_size(value:String, default:String) = MemoryPropertyEditor.parse(Option(value).getOrElse(default)).toInt
    codec.setMaxFrameSize(mem_size(config.max_frame_size, "100M"))

    if( config.queue_prefix!=null ||
        config.topic_prefix!=null ||
        config.destination_separator!=null ||
        config.path_separator!= null ||
        config.any_child_wildcard != null ||
        config.any_descendant_wildcard!= null ||
        config.regex_wildcard_start!= null ||
        config.regex_wildcard_end!= null
    ) {

      destination_parser = new DestinationParser().copy(DEFAULT_DESTINATION_PARSER)
      if( config.queue_prefix!=null ) { destination_parser.queue_prefix = config.queue_prefix }
      if( config.topic_prefix!=null ) { destination_parser.topic_prefix = config.topic_prefix }
      if( config.temp_queue_prefix!=null ) { destination_parser.temp_queue_prefix = config.temp_queue_prefix }
      if( config.temp_topic_prefix!=null ) { destination_parser.temp_topic_prefix = config.temp_topic_prefix }
      if( config.destination_separator!=null ) { destination_parser.destination_separator = config.destination_separator }
      if( config.path_separator!=null ) { destination_parser.path_separator = config.path_separator }
      if( config.any_child_wildcard!=null ) { destination_parser.any_child_wildcard = config.any_child_wildcard }
      if( config.any_descendant_wildcard!=null ) { destination_parser.any_descendant_wildcard = config.any_descendant_wildcard }
      if( config.regex_wildcard_start!=null ) { destination_parser.regex_wildcard_start = config.regex_wildcard_start }
      if( config.regex_wildcard_end!=null ) { destination_parser.regex_wildcard_end = config.regex_wildcard_end }
    }

    amqp_connection = AmqpTransport.accept(connection.transport)
    amqp_connection.setListener(amqp_listener)
    if( amqp_trace ) {
      amqp_connection.setProtocolTracer(new ProtocolTracer() {
        def receivedFrame(transportFrame: TransportFrame) = {
          println("RECV: %s | %s".format(security_context.remote_address, transportFrame.getBody()));
  //        connection_log.trace("RECV: %s | %s", security_context.remote_address, transportFrame.getBody());
        }
        def sentFrame(transportFrame: TransportFrame) = {
          println("SEND: %s | %s".format(security_context.remote_address, transportFrame.getBody()));
  //        connection_log.trace("SEND: %s | %s", security_context.remote_address, transportFrame.getBody());
        }
      });
    }
    connection.transport.resumeRead()
  }

  val amqp_listener = new AmqpListener() {

    override def processSaslConnect(protonTransport: TransportImpl) = {
      val sasl = protonTransport.sasl();
      sasl.setMechanisms(Array("ANONYMOUS", "PLAIN"));
      sasl.server();
      sasl
    }

    override def processSaslEvent(sasl: Sasl): Sasl = {
      // Lets try to complete the sasl handshake.
      if (sasl.getRemoteMechanisms().length > 0) {
        if ("PLAIN" == sasl.getRemoteMechanisms()(0)) {
          val data = new Array[Byte](sasl.pending());
          sasl.recv(data, 0, data.length);
          val parts = new Buffer(data).split(0);
          if (parts.length > 0) {
            security_context.user = parts(0).utf8.toString
          }
          if (parts.length > 1) {
            security_context.password = parts(1).utf8.toString
          }
          // We can't really auth at this point since we don't know the client id yet.. :(
          sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
          null
        } else if ("ANONYMOUS" == sasl.getRemoteMechanisms()(0)) {
          sasl.done(Sasl.SaslOutcome.PN_SASL_OK);
          null
        } else {
          sasl.done(Sasl.SaslOutcome.PN_SASL_PERM);
          null
        }
      } else {
        sasl
      }
    }

    override def processRemoteOpen(endpoint: Endpoint, onComplete: Task) {
      endpoint match {
        case connection:engine.Connection =>
          processConnectionOpen(connection, onComplete)
        case session:engine.Session =>
          session.open(); onComplete.run()
        case sender:engine.Sender =>
          processSenderOpen(sender, onComplete)
        case receiver:engine.Receiver =>
          processReceiverOpen(receiver, onComplete)
        case _ =>
          async_die("system-error", "Unknown Endpoint")
      }
    }

    override def processRemoteClose(endpoint: Endpoint, onComplete: Task) {
      endpoint match {
        case connection:engine.Connection =>
          processConnectionClose(connection, onComplete)
        case session:engine.Session =>
          session.close(); onComplete.run()
        case sender:engine.Sender =>
          processSenderClose(sender, onComplete)
        case receiver:engine.Receiver =>
          processReceiverClose(receiver, onComplete)
        case _ =>
          async_die("system-error", "Unknown Endpoint")
      }
    }


    def processConnectionOpen(conn: engine.Connection, onComplete: Task) {
      security_context.remote_application = conn.getRemoteContainer()

      suspend_read("host lookup")
      broker.dispatch_queue {
        val virtual_host = proton.getRemoteHostname match {
          case null => broker.default_virtual_host
          case "" => broker.default_virtual_host
          case host => broker.get_virtual_host(ascii(host))
        }
        queue {
          resume_read
          if (virtual_host == null) {
            onComplete.run()
            async_die("invalid virtual host", "invalid virtual host: " + proton.getRemoteHostname)
          } else if (!virtual_host.service_state.is_started) {
            onComplete.run()
            async_die("virtual host not ready", "")
          } else {
            connection_log = virtual_host.connection_log
            host = virtual_host
            proton.setLocalContainerId(virtual_host.id)
            security_context.session_id = "%s-%x".format(host.config.id, host.session_counter.incrementAndGet)
            //                proton.open()
            //                callback.onSuccess(response)
            if (virtual_host.authenticator != null && virtual_host.authorizer != null) {
              suspend_read("authenticating and authorizing connect")
              virtual_host.authenticator.authenticate(security_context) {
                auth_failure =>
                  queue {
                    if (auth_failure != null) {
                      onComplete.run()
                      async_die("Authentication failure", "%s. Credentials=%s".format(auth_failure, security_context.credential_dump))
                    } else if (!virtual_host.authorizer.can(security_context, "connect", connection.connector)) {
                      onComplete.run()
                      async_die("Authorization failure", "Not authorized to connect to connector '%s'. Principals=%s".format(connection.connector.id, security_context.principal_dump))
                    } else if (!virtual_host.authorizer.can(security_context, "connect", virtual_host)) {
                      onComplete.run()
                      async_die("Authorization failure", "Not authorized to connect to virtual host '%s'. Principals=%s".format(virtual_host.id, security_context.principal_dump))
                    } else {
                      resume_read
                      proton.open()
                      onComplete.run()
                    }
                  }
              }
            } else {
              proton.open()
              onComplete.run()
            }
          }
        }
      }
    }

    def processReceiverOpen(receiver: Receiver, onComplete: Task) {
      // Client producer is attaching..
      receiver.setSource(receiver.getRemoteSource());
      receiver.setTarget(receiver.getRemoteTarget());

      receiver.getRemoteTarget() match {
        case target: Coordinator =>
          set_attachment(receiver, coordinatorContext)
          receiver.flow(prefetch);
          receiver.open();
          onComplete.run()

        case amqp_target: Target =>

          val (address, addresses, actualTarget) = decode_target(amqp_target)
          receiver.setTarget(actualTarget);
          if (addresses == null) {
            close_with_error(receiver, "invalid-address", "Invaild address: " + address)
            onComplete.run()
            return
          }

          link_counter += 1
          val route = new AmqpProducerRoute(link_counter, receiver, addresses)
          producers += (link_counter -> route)

          host.dispatch_queue {
            val rc = host.router.connect(route.addresses, route, security_context)
            queue {
              rc match {
                case Some(failure) =>
                  close_with_error(receiver, "Could not connect", failure)
                  onComplete.run()
                case None =>
                  // If the remote has not closed on us yet...
                  if (receiver.getRemoteState == EndpointState.ACTIVE) {
                    set_attachment(receiver, route)
                    receiver.flow(prefetch);
                    receiver.open()
                  } else {
                    receiver.close()
                  }
                  onComplete.run()
              }
            }
          }
      }
    }

    def get_attachment(endpoint:Endpoint):AnyRef = {
      amqp_connection.context(endpoint).getAttachment()
    }

    def set_attachment(endpoint:Endpoint, value:AnyRef) = {
      amqp_connection.context(endpoint).setAttachment(value)
    }

    def processSenderClose(sender: Sender, onComplete: Task) = {
      get_attachment(sender) match {
        case null =>
          sender.close()
          onComplete.run()
        case consumer: AmqpConsumer =>
          // Lets disconnect the route.
          set_attachment(sender, null)
          consumer.close
      }
    }

    def processReceiverClose(receiver: Receiver, onComplete: Task) {
      get_attachment(receiver) match {
        case route: AmqpProducerRoute =>
          // Lets disconnect the route.
          set_attachment(receiver, null)
          producers -= route.id
          host.dispatch_queue {
            host.router.disconnect(route.addresses, route)
            queue {
              receiver.close()
              route.release
              onComplete.run()
            }
          }
        case _ =>
          receiver.close()
          onComplete.run()
      }
    }

    override def processDelivery(delivery: engine.Delivery) {
      get_attachment(delivery.getLink) match {
        case null =>
        case producer: ProducerSupport =>
          producer.process(delivery.asInstanceOf[DeliveryImpl])
        case consumer: AmqpConsumer =>
          consumer.process(delivery.asInstanceOf[DeliveryImpl])
      }
    }

    def processSenderOpen(sender: Sender, onComplete: Task) {
      // Client consumer is attaching..
      sender.setSource(sender.getRemoteSource());
      sender.setTarget(sender.getRemoteTarget());

      var source = sender.getRemoteSource().asInstanceOf[Source]
      if( source == null ) {
        // Source get set to null when a durable sub is being ended.
        source = new amqp.messaging.Source();
        source.setAddress("dsub://"+sender.getName);
        source.setDurable(TerminusDurability.UNSETTLED_STATE)
        source.setExpiryPolicy(TerminusExpiryPolicy.NEVER)
        sender.setSource(source);
      }

      val (address, requested_addresses, actual) = decode_source(source)
      sender.setSource(actual);
      if (requested_addresses == null) {
        sender.setSource(null)
        close_with_error(sender, "amqp:not-found", "Invaild address: " + address)
        onComplete.run()
        return
      }

      var noLocal = false
      val filter = source.getFilter()
      val selector = if (filter != null) {
        var value = filter.get(NO_LOCAL).asInstanceOf[DescribedType];
        if( value!=null ) {
          // TODO: setup a no-local filter.
          noLocal = true
        }
        value = filter.get(JMS_SELECTOR).asInstanceOf[DescribedType]
        if (value != null) {
          val selector = value.getDescribed().toString()
          try {
            (selector, SelectorParser.parse(selector))
          } catch {
            case e: FilterException =>
              sender.setSource(null)
              close_with_error(sender, "amqp:invalid-field", "Invalid selector expression '%s': %s".format(selector, e.getMessage))
              onComplete.run()
              return
          }
        } else {
          null
        }
      } else {
        null
      }

      val presettle = sender.getRemoteSenderSettleMode() == SenderSettleMode.SETTLED;

      def is_multi_destination = if (requested_addresses.length > 1) {
        true
      } else {
        PathParser.containsWildCards(requested_addresses(0).path)
      }

      val persistent = TerminusDurability.UNSETTLED_STATE == source.getDurable() && source.getExpiryPolicy == TerminusExpiryPolicy.NEVER
      val addresses: Array[_ <: BindAddress] = if (persistent) {
        val dsubs = ListBuffer[BindAddress]()
        val topics = ListBuffer[BindAddress]()
        requested_addresses.foreach {
          address =>
            address.domain match {
              case "dsub" => dsubs += address
              case "topic" => topics += address
              case _ =>
                sender.setSource(null)
                close_with_error(sender, "invalid-from-seq", "A durable link can only be used on a topic destination")
                onComplete.run()
                return
            }
        }
        sender.getName()
        val s = if (selector == null) null else selector._1
        if( !topics.isEmpty ) {
          dsubs += SubscriptionAddress(destination_parser.decode_path(sender.getName), s, topics.toArray)
        }
        dsubs.toArray
      } else {
        requested_addresses
      }

      var browser = addresses.find(_.domain != "queue").isEmpty && (source.getDistributionMode() == COPY)
      var browser_end = false
      var exclusive = !browser && false
      var include_seq: Option[Long] = None
      val from_seq_opt: Option[Long] = None

      if (from_seq_opt.isDefined && is_multi_destination) {
        sender.setSource(null)
        close_with_error(sender, "invalid-from-seq", "The from-seq header is only supported when you subscribe to one destination")
        onComplete.run()
        return
      }
      val from_seq = from_seq_opt.getOrElse(0L)


      link_counter += 1
      val id = link_counter
      val consumer = new AmqpConsumer(sender, id, addresses, presettle, selector, noLocal, browser, exclusive, include_seq, from_seq, browser_end);
      consumers += (id -> consumer)

      host.dispatch_queue {
        host.router.bind(consumer.addresses, consumer, security_context) { rc =>
          queue {
            rc match {
              case Some(reason) =>
                consumers -= id
                consumer.release
                sender.setSource(null)
                close_with_error(sender, "amqp:not-found", reason)
                onComplete.run()
              case None =>
                set_attachment(sender, consumer)
                sender.open()
                onComplete.run()
            }
          }
        }
      }
    }

    var gracefully_closed = false
    override def processFailure(e: Throwable) {
      var msg = "Internal Server Error: " + e
      if( connection_log!=AmqpProtocolHandler ) {
        // but we also want the error on the apollo.log file.
        warn(e, msg)
      }
      async_die("internal-error", msg, e)
    }

    override def processTransportFailure(error: IOException) {
      on_transport_disconnected()
      if( !gracefully_closed ) {
        connection_log.info("Shutting connection '%s'  down due to: %s", security_context.remote_address, error)
        connection.stop(NOOP)
      }
    }

    def processConnectionClose(conn: engine.Connection, onComplete: Task) {
      gracefully_closed = true
      on_transport_disconnected()
      conn.close()
      onComplete.run()
      queue.after(die_delay, TimeUnit.MILLISECONDS) {
        connection.stop(NOOP)
      }
    }

    override def processRefill() = {
      for( c <- consumers.values ) {
        c.session_manager.drain_overflow
      }
    }
  }

  var disconnected = false
  override def on_transport_disconnected() = {
    queue.assertExecuting()
    if( !disconnected ) {
      disconnected = true

      // Rollback any in-progress transactions..
      for( (id, tx) <- transactions ) {
        tx.rollback
      }
      transactions.clear()

      for (producer <- producers.values) {
        val addresses = producer.addresses
        host.dispatch_queue {
          host.router.disconnect(producer.addresses, producer)
          producer.release()
        }
      }
      producers = Map()


      for (consumer <- consumers.values) {
        val addresses = consumer.addresses
        host.dispatch_queue {
          host.router.unbind(addresses, consumer, false , security_context)
          consumer.release()
        }
      }
      consumers = Map()
      security_context.logout( e => {
        if(e!=null) {
          connection_log.info(e, "STOMP connection '%s' log out error: %s", security_context.remote_address, e)
        }
      })
      trace("amqp protocol resources released")
    }
  }

  var destination_parser = DEFAULT_DESTINATION_PARSER
  var temp_destination_map = HashMap[SimpleAddress, SimpleAddress]()

  def decode_addresses(value: String): Array[SimpleAddress] = {
    val rc = destination_parser.decode_multi_destination(value)
    if (rc == null) {
      return null
    }
    rc.map {
      dest =>
        if (dest.domain.startsWith("temp-")) {
          temp_destination_map.getOrElseUpdate(dest, {
            val parts = LiteralPart("temp") :: LiteralPart(broker.id) :: LiteralPart(session_id) :: dest.path.parts
            SimpleAddress(dest.domain.stripPrefix("temp-"), Path(parts))
          })
        } else {
          dest
        }
    }
  }

  def decode_target(target: Target) = {
    var dynamic = target.getDynamic()
    if (dynamic) {
      temp_dest_counter += 1
      val parts = LiteralPart("temp") :: LiteralPart(broker.id) :: LiteralPart(session_id) :: LiteralPart(temp_dest_counter.toString) :: Nil
      val rc = SimpleAddress("queue", Path(parts))
      val actual = new Target();
      var address = destination_parser.encode_destination(rc)
      actual.setAddress(address);
      actual.setDynamic(true);
      (address, Array(rc), actual)
    } else {
      val address = target.getAddress
      decode_addresses(address) match {
        case null =>
          (address, null, target)
        case addresses =>
          (address, addresses, target)
      }
    }
  }

  def decode_source(source: Source) = {
    var dynamic = source.getDynamic()
    if (dynamic) {
      temp_dest_counter += 1
      val parts = LiteralPart("temp") :: LiteralPart(broker.id) :: LiteralPart(session_id) :: LiteralPart(temp_dest_counter.toString) :: Nil
      val rc = SimpleAddress("queue", Path(parts))
      val actual = new Source();
      var address = destination_parser.encode_destination(rc)
      actual.setAddress(address);
      actual.setDynamic(true);
      (address, Array(rc), actual)
    } else {
      val address = source.getAddress
      decode_addresses(address) match {
        case null =>
          (address, null, source)
        case addresses =>
          (address, addresses, source)
      }
    }
  }

  var temp_dest_counter = 0L

  trait ProducerSupport {
    var current = new ByteArrayOutputStream();

    def process(delivery: DeliveryImpl): Unit = {
      val receiver = delivery.getLink.asInstanceOf[Receiver]
      if (!delivery.isReadable()) {
        trace("it was not readable!");
        return;
      }

      if (current == null) {
        current = new ByteArrayOutputStream();
      }

      var data = new Array[Byte](1024 * 4);
      var done = false
      while (!done) {
        val count = receiver.recv(data, 0, data.length)
        if (count > 0) {
          current.write(data, 0, count);
        } else {
          if (count == 0) {
            // Expecting more deliveries..
            return;
          }
          done = true
        }
      }

      val buffer = current.toBuffer();
      current = null;
      onMessage(receiver, delivery, new AmqpMessage(buffer));
    }

    def onMessage(receiver:Receiver, delivery: DeliveryImpl, buffer: AmqpMessage): Unit
  }

  class AmqpProducerRoute(val id:Long, val receiver: Receiver, val addresses: Array[SimpleAddress]) extends DeliveryProducerRoute(host.router) with ProducerSupport {

    val key = addresses.toList

    override def send_buffer_size = buffer_size

    override def connection = Some(AmqpProtocolHandler.this.connection)

    override def dispatch_queue = queue


    /**
     * Called for each value what is passed on to the down stream sink.
     */
    override protected def onDelivered(value: Delivery) {
      receiver.flow(1)
      pump_out
    }

    def onMessage(receiver:Receiver, delivery: DeliveryImpl, m: AmqpMessage) = {

      // Update the message to attach some producer context to the footer..
      // of the message.
      val dm = m.decoded
      val footer_map:java.util.Map[AnyRef,AnyRef] = if( dm.getFooter == null ) {
        val map = new java.util.HashMap[AnyRef,AnyRef]
        dm.setFooter(new Footer(map))
        map
      } else {
        dm.getFooter.getValue.asInstanceOf[java.util.Map[AnyRef,AnyRef]]
      }
      footer_map.put(ORIGIN, session_id)
      val message = new AmqpMessage(null, dm)

      val d = new Delivery
      d.message = message
      d.size = message.encoded.length
      var decoded = message.decoded
      if (decoded.getProperties != null) {
        if (decoded.getProperties.getAbsoluteExpiryTime != null) {
          d.expiration = decoded.getProperties.getAbsoluteExpiryTime.getTime
        }
      }
      if (decoded.getHeader != null) {
        if (decoded.getHeader.getDurable != null) {
          d.persistent = decoded.getHeader.getDurable.booleanValue()
        }
        if (decoded.getHeader.getDeliveryCount != null) {
          d.redeliveries = decoded.getHeader.getDeliveryCount.shortValue()
        }
      }

      if (!delivery.remotelySettled()) {
        d.ack = (result, uow) => {
          queue {
            result match {
              case Consumed =>
                delivery.disposition(new Accepted())
                delivery.settle()
              case _ =>
                async_die("uknown", "Unexpected NAK from broker")
            }
            pump_out
          }
        }
      } else {
        delivery.settle()
      }

      delivery.getRemoteState() match {
        case state:TransactionalState =>
          transactions.get(toLong(state.getTxnId())) match {
            case Some(tx) =>
              tx.add((uow)=>{
                d.uow = uow
                val accepted = this.offer(d)
                assert(accepted)
              })
            case None =>
              die("uknown-tx", "txid in the delivery remote state is invalid")
          }
          receiver.advance();
        case _ =>
          val accepted = this.offer(d)
          assert(accepted)
          receiver.advance();
      }
    }
  }

  def close_with_error(link: Link, error_name: String = "", error_message: String = "") = {
    link.asInstanceOf[LinkImpl].setLocalError(amqp_error(error_name, error_message))
    link.close()
  }

  var link_counter = 0L
  var producers = Map[Long, AmqpProducerRoute]()
  var consumers = Map[Long, AmqpConsumer]()
  var message_id_counter = 0L

  class AmqpConsumer(sender: Sender,
                     val subscription_id: Long,
                     val addresses: Array[_ <: BindAddress],
                     val presettle: Boolean,
                     val selector: (String, BooleanExpression),
                     val noLocal:Boolean,
                     override val browser: Boolean,
                     override val exclusive: Boolean,
                     val include_seq: Option[Long],
                     val from_seq: Long,
                     override val close_on_drain: Boolean
                            ) extends Task with Retained with DeliveryConsumer {

    override def toString = "amqp subscription:" + sender.getName + ", remote address: " + security_context.remote_address


    /// Retained interface...
    val base_retained = new BaseRetained {
      override def dispose() {
        do_dispose()
        super.dispose()
      }
    }

    def printST(name:String) = {
      AmqpProtocolHandler.synchronized {
        val e = new Exception
        println(sender.getName+":"+name+":"+retained())
        println("  "+e.getStackTrace.drop(2).take(10).mkString("\n  "))
        System.out.flush()
      }
    }

    def release() = {
//      printST("release")
      base_retained.release()
    }
    def retain() = {
//      printST("retain")
      base_retained.retain()
    }
    def retained() = base_retained.retained()

    ///////////////////////////////////////////////////////////////////
    // DeliveryConsumer Interface..
    ///////////////////////////////////////////////////////////////////
    def connect(p: DeliveryProducer) = new AmqpConsumerSession(p)
    def dispatch_queue = queue
    override def connection = Option(AmqpProtocolHandler.this.connection)

    def is_persistent = false
    def matches(delivery: Delivery):Boolean = {
      if( delivery.message.codec eq AmqpMessageCodec ) {
        if ( noLocal ) {
          val origin = delivery.message.asInstanceOf[AmqpMessage].getFooterProperty(ORIGIN)
          if ( origin == session_id ) {
            return false
          }
        }
        if( selector!=null ) {
          selector._2.matches(delivery.message)
        } else {
          true
        }
      } else {
        false
      }
    }
    override def start_from_tail = from_seq == -1

    override def jms_selector = if (selector != null) {
      selector._1
    } else {
      null
    }

    override def user = security_context.user

    var starting_seq: Long = 0L

    override def set_starting_seq(seq: Long): Unit = {
      starting_seq = seq
    }

    def isSenderClosed = {
      sender.getLocalState == EndpointState.CLOSED
    }

    def close = {
      consumers -= subscription_id
      val drop = sender.getSource.asInstanceOf[Source].getExpiryPolicy != TerminusExpiryPolicy.NEVER
      host.dispatch_queue {
        host.router.unbind(addresses, this, drop , security_context)
        release()
      }
    }

    var nextTagId = 0L;
    val tagCache = new util.HashSet[Array[Byte]]();
    val unsettled = new HashMap[AsciiBuffer, org.apache.qpid.proton.engine.Delivery]()

    def nextTag: Array[Byte] = {
      var rc: Array[Byte] = null
      if (tagCache != null && !tagCache.isEmpty()) {
        val iterator = tagCache.iterator();
        rc = iterator.next();
        iterator.remove();
      } else {
        rc = java.lang.Long.toHexString(nextTagId).getBytes("UTF-8");
        nextTagId += 1
      }
      return rc;
    }

    def checkinTag(data: Array[Byte]) = {
      if (tagCache.size() < 1024) {
        tagCache.add(data);
      }
    }

    // As the Task attachment to the Sender, we are run
    // every events are fired on the sender endpoint.
    def run() = {
      queue.assertExecuting()
      // If the endpoint is active, and we have been drained of msgs, let the remote end know about it.
      if( sender.getLocalState == EndpointState.ACTIVE &&
          sender.getRemoteState==EndpointState.ACTIVE &&
          redeliveries.isEmpty &&
          session_manager.overflowed_sessions.isEmpty ) {
        sender.drained()
      }
    }

    val redeliveries = new util.LinkedList[(Session[Delivery], Delivery)]()
    val session_manager = new SessionSinkMux[Delivery](FullSink(), queue, Delivery, 100, buffer_size) {
      override def time_stamp = broker.now

      override def drain_overflow: Unit = {
        queue.assertExecuting()
        var pumpNeeded = false
        try {
          while ((sender.getCredit - sender.getQueued) > 0) {
            val value = poll
            if (value == null) {
              return
            }

            val (session, apollo_delivery) = value
            val message = if (apollo_delivery.message.codec == AmqpMessageCodec) {
              apollo_delivery.message.asInstanceOf[AmqpMessage].decoded
            } else {
              val (body, content_type) = protocol_convert match {
                case "body" => (apollo_delivery.message.getBodyAs(classOf[Buffer]), "protocol/" + apollo_delivery.message.codec.id + ";conv=body")
                case _ => (apollo_delivery.message.encoded, "protocol/" + apollo_delivery.message.codec.id())
              }

              message_id_counter += 1

              val message = new MessageImpl
              message.setMessageId(session_id + message_id_counter)
              message.setBody(new Data(new Binary(body.data, body.offset, body.length)))
              message.setContentType(content_type)
              message.setDurable(apollo_delivery.persistent)
              if (apollo_delivery.expiration > 0) {
                message.setExpiryTime(apollo_delivery.expiration)
              }
              message
            }

            if (apollo_delivery.redeliveries > 0) {
              message.setDeliveryCount(apollo_delivery.redeliveries)
              message.setFirstAcquirer(false)
            }

            val buffer = new AmqpMessage(null, message).encoded;
            val proton_delivery = if (presettle) {
              sender.delivery(EMPTY_BYTE_ARRAY, 0, 0).asInstanceOf[DeliveryImpl];
            } else {
              val tag = nextTag
              val proton_delivery = sender.delivery(tag, 0, tag.length).asInstanceOf[DeliveryImpl];
              unsettled.put(new AsciiBuffer(tag), proton_delivery)
              proton_delivery
            }

            val sent = sender.send(buffer.data, buffer.offset, buffer.length);
            assert( sent == buffer.length )
            delivered(session, apollo_delivery.size)
            pumpNeeded = true
            proton_delivery.setContext(value)
            if (presettle) {
              settle(proton_delivery, Consumed, false, null);
            } else {
              sender.advance();
            }
          }
        } finally {
          if( pumpNeeded ) {
            pumpNeeded = false
            pump_out
          }
        }
      }

      override def poll: (Session[Delivery], Delivery) = {
        if( redeliveries.isEmpty ) {
          super.poll
        } else {
          redeliveries.removeFirst()
        }
      }
    }

    def process(proton_delivery:DeliveryImpl):Unit = {
      val state = proton_delivery.getRemoteState();
      state match {
        case outcome:amqp.messaging.Outcome =>
          process(proton_delivery, outcome, null)
        case state:TransactionalState =>
          transactions.get(toLong(state.getTxnId())) match {
            case Some(tx) =>
              tx.add((uow)=>{
                  process(proton_delivery, state.getOutcome, uow)
              }
//                , ()=>{ settle(proton_delivery, null, true, null) }
              )
            case None =>
              die("uknown-tx", "txid in the delivery remote state is invalid")
          }
      }
    }

    def process(proton_delivery:DeliveryImpl, outcome:amqp.messaging.Outcome, uow:StoreUOW):Unit = {
      outcome match {
        case null =>
          if( !proton_delivery.remotelySettled() ) {
              proton_delivery.disposition(new Accepted());
          }
          settle(proton_delivery, Consumed, false, uow);
        case accepted:Accepted =>
          if( !proton_delivery.remotelySettled() ) {
              proton_delivery.disposition(new Accepted());
          }
          settle(proton_delivery, Consumed, false, uow);
        case rejected:amqp.messaging.Rejected =>
          // re-deliver /w incremented delivery counter.
          settle(proton_delivery, null, true, uow);
        case release:amqp.messaging.Released =>
          // re-deliver && don't increment the counter.
          settle(proton_delivery, null, false, uow);
        case modified:amqp.messaging.Modified =>
          def b(v:java.lang.Boolean) = v!=null && v.booleanValue()
          var ackType = if(b(modified.getUndeliverableHere())) {
              // receiver does not want the message..
              // perhaps we should DLQ it?
              Poisoned;
          } else {
            // Delivered ??
            null
          }
          settle(proton_delivery, ackType, b(modified.getDeliveryFailed()), uow);
      }
    }

    def settle(delivery:DeliveryImpl, ackType:DeliveryResult, incrementRedelivery:Boolean, uow:StoreUOW):Unit = {
      val ctx = delivery.getContext.asInstanceOf[(Session[Delivery], Delivery)]
      if( ctx==null ) {
        return
      }
      val (session, apollo_delivery) = ctx
      if( incrementRedelivery ) {
        apollo_delivery.redelivered
      }

      val tag = delivery.getTag();
      if( tag !=null && tag.length>0 ) {
          checkinTag(tag);
      }

      if( ackType == null ) {
        redeliveries.addFirst((session, apollo_delivery))
        session_manager.drain_overflow
        delivery.settle()
      } else {
        if( apollo_delivery.ack != null ) {
          apollo_delivery.ack(ackType, uow)
        }
        delivery.settle()
      }
      pump_out
    }

    class AmqpConsumerSession(p: DeliveryProducer) extends DeliverySession with SessionSinkFilter[Delivery] {

      def producer = p
      def consumer = AmqpConsumer.this
      val downstream = session_manager.open(producer.dispatch_queue)

      // Delegate all the flow control stuff to the session
      override def full = {
        val rc = super.full
        rc
      }

      def offer(delivery: Delivery) = {
        if (full) {
          false
        } else {
          delivery.message.retain()
          val rc = downstream.offer(delivery)
          assert(rc, "offer should be accepted since it was not full")
          true
        }
      }

      def close {
        session_manager.close(downstream, (delivery)=>{
          if( delivery.ack !=null ) {
            delivery.ack(Undelivered, null)
          }
        })
      }
    }

    def do_dispose() = queue {
      def reject(value:(Session[Delivery], Delivery), result:DeliveryResult) ={
        val (_, delivery) = value
        if( delivery.ack!=null ) {
          delivery.ack(result, null)
        }
      }

      for( v <- unsettled.values ) {
        val value = v.getContext.asInstanceOf[(Session[Delivery], Delivery)]
        if( value!=null ) {
          v.setContext(null)
          reject(value, Delivered)
        }
      }

      var next = session_manager.poll
      while( next!=null ) {
        reject(next, Undelivered)
        next = session_manager.poll
      }

      sender.close()
      pump_out
    }

  }


  class TransactionQueue {
    // TODO: eventually we want to back this /w a broker Queue which
    // can provides persistence and memory swapping.

    val queue = ListBuffer[((StoreUOW)=>Unit, ()=>Unit)]()

    def add(on_commit:(StoreUOW)=>Unit, on_rollback:()=>Unit=null):Unit = {
      queue += ((on_commit, on_rollback))
    }

    def commit(on_complete: => Unit) = {
      if( host.store!=null ) {
        val uow = host.store.create_uow
//        println("UOW starting: "+uow.asInstanceOf[DelayingStoreSupport#DelayableUOW].uow_id)
        uow.on_complete {
//          println("UOW completed: "+uow.asInstanceOf[DelayingStoreSupport#DelayableUOW].uow_id)
          on_complete
        }
        queue.foreach{ _._1(uow) }
        uow.release
      } else {
        queue.foreach{ _._1(null) }
        on_complete
      }
    }

    def rollback = {
      queue.foreach{ case (x, y) =>
        if( y != null ) {
          y()
        }
      }
    }

  }

  val transactions = HashMap[Long, TransactionQueue]()

  def create_tx_queue(txid:Long):TransactionQueue = {
    if ( transactions.contains(txid) ) {
      die("invalid-tx", "transaction allready started")
    } else {
      val queue = new TransactionQueue
      transactions.put(txid, queue)
      queue
    }
  }

  def remove_tx_queue(txid:Long):TransactionQueue = {
    transactions.remove(txid).getOrElse(die("invalid-tx", "transaction not active: %d".format(txid)))
  }

  var nextTransactionId = 0L;
  object coordinatorContext extends ProducerSupport {

    def onMessage(receiver: Receiver, delivery: DeliveryImpl, buffer: AmqpMessage) = {
      val msg = buffer.decoded;
      val action = msg.getBody().asInstanceOf[AmqpValue].getValue();
      action match {
        case declare: Declare =>
          if (declare.getGlobalId() != null) {
            throw new Exception("don't know how to handle a declare /w a set GlobalId");
          }

          val txid = nextTransactionId
          nextTransactionId += 1

          create_tx_queue(txid)

          val declared = new Declared();
          declared.setTxnId(new Binary(toBytes(txid)));
          delivery.disposition(declared);
          delivery.settle();

        case discharge: Discharge =>
          val txid = toLong(discharge.getTxnId());
//                  ExceptionResponse er = (ExceptionResponse)response;
//                  Rejected rejected = new Rejected();
//                  ArrayList errors = new ArrayList();
//                  errors.add(er.getException().getMessage());
//                  rejected.setError(errors);
//                  delivery.disposition(rejected);

          val tx_queue = remove_tx_queue(txid);
          if (discharge.getFail()) {
            tx_queue.rollback
            delivery.settle();
            pump_out
          } else {
            tx_queue.commit {
              queue {
                delivery.settle();
                pump_out
              }
            }
          }
        case _ =>
          throw new Exception("Expected coordinator message type: " + action.getClass());
      }

    }
  }

}

