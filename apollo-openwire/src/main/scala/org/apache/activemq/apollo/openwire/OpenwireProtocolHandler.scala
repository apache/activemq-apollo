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

package org.apache.activemq.apollo.openwire

import OpenwireConstants._

import org.fusesource.hawtdispatch._
import org.fusesource.hawtbuf._
import collection.mutable.{ListBuffer, HashMap}

import org.apache.activemq.apollo.broker._
import BufferConversions._
import java.io.IOException
import org.apache.activemq.apollo.selector.SelectorParser
import org.apache.activemq.apollo.filter.{BooleanExpression, FilterException}
import org.apache.activemq.apollo.transport._
import org.apache.activemq.apollo.broker.store._
import org.apache.activemq.apollo.util._
import java.util.concurrent.TimeUnit
import java.util.Map.Entry
import protocol._
import scala.util.continuations._
import security.SecurityContext
import tcp.TcpTransport
import codec.OpenWireFormat
import command._
import org.apache.activemq.apollo.openwire.dto.OpenwireConnectionStatusDTO
import org.apache.activemq.apollo.dto.{TopicDestinationDTO, DurableSubscriptionDestinationDTO, DestinationDTO}

object OpenwireProtocolHandler extends Log {

  val DEFAULT_DIE_DELAY = 5 * 1000L
  var die_delay = DEFAULT_DIE_DELAY

  val preferred_wireformat_settings = new WireFormatInfo();
  preferred_wireformat_settings.setVersion(OpenWireFormat.DEFAULT_VERSION);
  preferred_wireformat_settings.setStackTraceEnabled(true);
  preferred_wireformat_settings.setCacheEnabled(true);
  preferred_wireformat_settings.setTcpNoDelayEnabled(true);
  preferred_wireformat_settings.setTightEncodingEnabled(true);
  preferred_wireformat_settings.setSizePrefixDisabled(false);
  preferred_wireformat_settings.setMaxInactivityDuration(30 * 1000 * 1000);
  preferred_wireformat_settings.setMaxInactivityDurationInitalDelay(10 * 1000 * 1000);
  preferred_wireformat_settings.setCacheSize(1024);
  preferred_wireformat_settings.setMaxFrameSize(OpenWireFormat.DEFAULT_MAX_FRAME_SIZE);
}


/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

class OpenwireProtocolHandler extends ProtocolHandler {

  var minimum_protocol_version = 1

  import OpenwireProtocolHandler._

  def dispatchQueue: DispatchQueue = connection.dispatch_queue

  def protocol = PROTOCOL

  var outbound_sessions: SinkMux[Command] = null
  var connection_session: Sink[Command] = null
  var closed = false

  var last_command_id=0

  def next_command_id = {
    last_command_id += 1
    last_command_id
  }

  var producerRoutes = new LRUCache[List[DestinationDTO], DeliveryProducerRoute](10) {
    override def onCacheEviction(eldest: Entry[List[DestinationDTO], DeliveryProducerRoute]) = {
      host.router.disconnect(eldest.getKey.toArray, eldest.getValue)
    }
  }

  var host: VirtualHost = null

  private def queue = connection.dispatch_queue

  var session_id: AsciiBuffer = _
  var wire_format: OpenWireFormat = _
  var login: Option[AsciiBuffer] = None
  var passcode: Option[AsciiBuffer] = None
  var dead = false
  val security_context = new SecurityContext

  var heart_beat_monitor: HeartBeatMonitor = new HeartBeatMonitor

  var waiting_on: String = "client request"
  var current_command: Object = _


  override def create_connection_status = {
    var rc = new OpenwireConnectionStatusDTO
    rc.protocol_version = ""+(if (wire_format == null) 0 else wire_format.getVersion)
    rc.user = login.map(_.toString).getOrElse(null)
    //    rc.subscription_count = consumers.size
    rc.waiting_on = waiting_on
    rc
  }

  def suspendRead(reason: String) = {
    waiting_on = reason
    connection.transport.suspendRead
  }

  def resumeRead() = {
    waiting_on = "client request"
    connection.transport.resumeRead
  }

  def ack(command: Command):Unit = {
    if (command.isResponseRequired()) {
      val rc = new Response();
      rc.setCorrelationId(command.getCommandId());
      connection_session.offer(rc);
    }
  }


  override def on_transport_failure(error: IOException) = {
    if (!connection.stopped) {
      error.printStackTrace
      suspendRead("shutdown")
      debug(error, "Shutting connection down due to: %s", error)
      connection.stop
    }
  }

  override def on_transport_connected():Unit = {
    security_context.local_address = connection.transport.getLocalAddress
    security_context.remote_address = connection.transport.getRemoteAddress
    outbound_sessions = new SinkMux[Command](connection.transport_sink.map {
      x:Command =>
        x.setCommandId(next_command_id)
        debug("sending openwire command: %s", x.toString())
        x
    }, dispatchQueue, OpenwireCodec)
    connection_session = new OverflowSink(outbound_sessions.open(dispatchQueue));
    connection_session.refiller = NOOP

    // Send our preferred wire format settings..
    connection.transport.offer(preferred_wireformat_settings)

    resumeRead
    reset {
      suspendRead("virtual host lookup")
      this.host = connection.connector.broker.get_default_virtual_host
      resumeRead
      if(host==null) {
        async_die("Could not find default virtual host")
      }
    }
  }

  override def on_transport_disconnected():Unit = {
    if (!closed) {
      closed = true;
      dead = true;

      heart_beat_monitor.stop

      import collection.JavaConversions._
      producerRoutes.foreach{
        case (dests, route) => host.router.disconnect(dests.toArray, route)
      }
      producerRoutes.clear

      //      consumers.foreach{
      //        case (_, consumer) =>
      //          if (consumer.binding == null) {
      //            host.router.unbind(consumer.destination, consumer)
      //          } else {
      //            host.router.get_queue(consumer.binding) {
      //              queue =>
      //                queue.foreach(_.unbind(consumer :: Nil))
      //            }
      //          }
      //      }
      //      consumers = Map()
      trace("openwire protocol resources released")
    }
  }


  override def on_transport_command(command: Object):Unit = {
    if( dead ) {
      // We stop processing client commands once we are dead
      return;
    }
    try {
      current_command = command
      trace("received: %s", command)
      if (wire_format == null) {
        command match {
          case codec: OpenwireCodec =>
            // this is passed on to us by the protocol discriminator
            // so we know which wire format is being used.
          case command: WireFormatInfo =>
            on_wire_format_info(command)
          case _ =>
            die("Unexpected command: " + command.getClass);
        }
      } else {
        command match {
          case msg:ActiveMQMessage=> on_message(msg)
          case ack:MessageAck=> on_message_ack(ack)
          case info:TransactionInfo => on_transaction_info(info)
          case info:ProducerInfo=> on_producer_info(info)
          case info:ConsumerInfo=> on_consumer_info(info)
          case info:SessionInfo=> on_session_info(info)
          case info:ConnectionInfo=> on_connection_info(info)
          case info:RemoveInfo=> on_remove_info(info)
          case info:KeepAliveInfo=> ack(info)
          case info:ShutdownInfo=> ack(info); connection.stop
          case info:FlushCommand=> ack(info)

          // case info:ConnectionControl=>
          // case info:ConnectionError=>
          // case info:ConsumerControl=>
          // case info:DestinationInfo=>
          // case info:RemoveSubscriptionInfo=>
          // case info:ControlCommand=>

          ///////////////////////////////////////////////////////////////////
          // Methods for cluster operations
          // These commands are sent to the broker when it's acting like a
          //client to another broker.
          // /////////////////////////////////////////////////////////////////
          // case info:BrokerInfo=>
          // case info:MessageDispatch=>
          // case info:MessageDispatchNotification=>
          // case info:ProducerAck=>


          case _ =>
            die("Unspported command: " + command.getClass);
        }
      }
    } catch {
      case e: Break =>
      case e: Exception =>
        e.printStackTrace
        async_die("Internal Server Error")
    } finally {
      current_command = null
    }
  }

  class ProtocolException(msg:String) extends RuntimeException(msg)
  class Break extends RuntimeException

  def async_fail(msg: String, actual:Command=null):Unit = try {
    fail(msg, actual)
  } catch {
    case x:Break=>
  }

  def fail[T](msg: String, actual:Command=null):T = {
    def respond(command:Command) = {
      if(command.isResponseRequired()) {
        val e = new ProtocolException(msg);
        e.fillInStackTrace

        val rc = new ExceptionResponse();
        rc.setCorrelationId(command.getCommandId());
        rc.setException(e)
        connection_session.offer(rc);
      }
    }
    (current_command,actual) match {
       case (null, null)=>
       case (null, command:Command)=>
       case (command:Command, null)=>
       case (command:Command, command2:Command)=>
         respond(command)
    }
    throw new Break()
  }

  def async_die(msg: String):Unit = try {
    die(msg)
  } catch {
    case x:Break=>
  }

  /**
   * A protocol error that cannot be recovered from. It results in the connections being terminated.
   */
  def die[T](msg: String):T = {
    if (!dead) {
      dead = true
      debug("Shutting connection down due to: " + msg)
      // TODO: if there are too many open connections we should just close the connection
      // without waiting for the error to get sent to the client.
      queue.after(die_delay, TimeUnit.MILLISECONDS) {
        connection.stop()
      }
      fail(msg)
    }
    throw new Break()
  }



  def on_wire_format_info(info: WireFormatInfo) = {

    if (!info.isValid()) {
      die("Remote wire format magic is invalid")
    } else if (info.getVersion() < minimum_protocol_version) {
      die("Remote wire format (%s) is lower the minimum version required (%s)".format(info.getVersion(), minimum_protocol_version))
    }

    wire_format = connection.transport.getProtocolCodec.asInstanceOf[OpenwireCodec].format
    wire_format.renegotiateWireFormat(info, preferred_wireformat_settings)

    connection.transport match {
      case x: TcpTransport =>
        x.getSocketChannel.socket.setTcpNoDelay(wire_format.isTcpNoDelayEnabled())
      case _ =>
    }

    val inactive_time = preferred_wireformat_settings.getMaxInactivityDuration().min(info.getMaxInactivityDuration())
    val initial_delay = preferred_wireformat_settings.getMaxInactivityDurationInitalDelay().min(info.getMaxInactivityDurationInitalDelay())

    if (initial_delay != inactive_time) {
      die("We only support an initial delay inactivity duration equal to the max inactivity duration")
    }

    if (inactive_time > 0) {
      heart_beat_monitor.read_interval = inactive_time
      // lets be a little forgiving to account to packet transmission latency.
      heart_beat_monitor.read_interval += inactive_time.min(5000)

      heart_beat_monitor.on_dead = () => {
        async_die("Stale connection.  Missed heartbeat.")
      }

      heart_beat_monitor.write_interval = inactive_time
      heart_beat_monitor.on_keep_alive = () => {
        // we don't care if the offer gets rejected.. since that just
        // means there is other traffic getting transmitted.
        connection.transport.offer(new KeepAliveInfo)
      }
    }

    heart_beat_monitor.transport = connection.transport
    heart_beat_monitor.start

    // Give the client some info about this broker.
    val brokerInfo = new BrokerInfo();
    brokerInfo.setBrokerId(new BrokerId(host.config.id));
    brokerInfo.setBrokerName(host.config.id);
    brokerInfo.setBrokerURL(host.broker.get_connect_address);
    connection_session.offer(brokerInfo);
  }

  val all_connections = new HashMap[ConnectionId, ConnectionContext]();
  val all_sessions = new HashMap[SessionId, SessionContext]();
  val all_producers = new HashMap[ProducerId, ProducerContext]();
  val all_consumers = new HashMap[ConsumerId, ConsumerContext]();
  val all_transactions = new HashMap[TransactionId, TransactionContext]();
  val all_temp_dests = List[ActiveMQDestination]();

  class ConnectionContext(val info: ConnectionInfo) {

    val sessions = new HashMap[SessionId, SessionContext]();
    val transactions = new HashMap[TransactionId, TransactionContext]();

    def default_session_id = new SessionId(info.getConnectionId(), -1)

    def attach = {
      // create the default session.
      new SessionContext(this, new SessionInfo(default_session_id)).attach
      all_connections.put(info.getConnectionId, this)
    }

    def dettach = {
      sessions.values.toArray.foreach(_.dettach)
      transactions.values.toArray.foreach(_.dettach)
      all_connections.remove(info.getConnectionId)
    }
  }

  class SessionContext(val parent: ConnectionContext, val info: SessionInfo) {
    val producers = new HashMap[ProducerId, ProducerContext]();
    val consumers = new HashMap[ConsumerId, ConsumerContext]();

    def attach = {
      parent.sessions.put(info.getSessionId, this)
      all_sessions.put(info.getSessionId, this)
    }

    def dettach = {
      producers.values.toArray.foreach(_.dettach)
      consumers.values.toArray.foreach(_.dettach)
      parent.sessions.remove(info.getSessionId)
      all_sessions.remove(info.getSessionId)
    }


  }

  def noop = shift {  k: (Unit=>Unit) => k() }

  class ProducerContext(val parent: SessionContext, val info: ProducerInfo) {
    def attach = {
      parent.producers.put(info.getProducerId, this)
      all_producers.put(info.getProducerId, this)
    }

    def dettach = {
      parent.producers.remove(info.getProducerId)
      all_producers.remove(info.getProducerId)
    }
  }

  class ConsumerContext(val parent: SessionContext, val info: ConsumerInfo) extends BaseRetained with DeliveryConsumer {

    var selector_expression:BooleanExpression = _
    var destination:Array[DestinationDTO] = _

    override def exclusive = info.isExclusive
    override def browser = info.isBrowser

    def attach = {

      if( info.getDestination == null ) fail("destination was not set")

      destination = info.getDestination.toDestination
      parent.consumers.put(info.getConsumerId, this)
      all_consumers.put(info.getConsumerId, this)
      var is_durable_sub = info.getSubscriptionName!=null

      selector_expression = info.getSelector match {
        case null=> null
        case x=>
          try {
            SelectorParser.parse(x)
          } catch {
            case e:FilterException =>
              fail("Invalid selector expression: "+e.getMessage)
          }
      }

      if( is_durable_sub ) {
        destination = destination.map { _ match {
          case x:TopicDestinationDTO=>
            val rc = new DurableSubscriptionDestinationDTO()
            rc.path = x.path
            if( is_durable_sub ) {
              rc.subscription_id = ""
              if( parent.parent.info.getClientId != null ) {
                rc.subscription_id += parent.parent.info.getClientId + ":"
              }
              rc.subscription_id += info.getSubscriptionName
            }
            rc.filter = info.getSelector
            rc
          case _ => die("A durable subscription can only be used on a topic destination")
          }
        }
      }

      reset {
        val rc = host.router.bind(destination, this, security_context)
        rc match {
          case None =>
            ack(info)
            noop
          case Some(reason) =>
            fail(reason, info)
            noop
        }
      }
      this.release
    }

    def dettach = {
      host.router.unbind(destination, this, false , security_context)
      parent.consumers.remove(info.getConsumerId)
      all_consumers.remove(info.getConsumerId)
    }

    ///////////////////////////////////////////////////////////////////
    // DeliveryConsumer impl
    ///////////////////////////////////////////////////////////////////

    def dispatch_queue = OpenwireProtocolHandler.this.dispatchQueue

    override def connection = Some(OpenwireProtocolHandler.this.connection)

    def is_persistent = false

    def matches(delivery:Delivery) = {
      if( delivery.message.protocol eq OpenwireProtocol ) {
        if( selector_expression!=null ) {
          selector_expression.matches(delivery.message)
        } else {
          true
        }
      } else {
        false
      }
    }

    def connect(p:DeliveryProducer) = new DeliverySession with SinkFilter {
      retain

      def producer = p
      def consumer = ConsumerContext.this
      var closed = false

      val outbound_session = outbound_sessions.open(producer.dispatch_queue)

      def downstream = outbound_session

      def close = {

        assert(producer.dispatch_queue.isExecuting)
        if( !closed ) {
          closed = true
          if( browser ) {
            // Then send the end of browse message.
            var dispatch = new MessageDispatch
            dispatch.setConsumerId(this.consumer.info.getConsumerId)
            dispatch.setMessage(null)
            dispatch.setDestination(null)

            if( outbound_session.full ) {
              // session is full so use an overflow sink so to hold the message,
              // and then trigger closing the session once it empties out.
              val sink = new OverflowSink(outbound_session)
              sink.refiller = ^{
                outbound_sessions.close(outbound_session)
                release
              }
              sink.offer(dispatch)
            } else {
              outbound_session.offer(dispatch)
              outbound_sessions.close(outbound_session)
              release
            }
          } else {
            outbound_sessions.close(outbound_session)
            release
          }
        }
      }

      def remaining_capacity = outbound_session.remaining_capacity

      // Delegate all the flow control stuff to the session
      def offer(delivery:Delivery) = {
        if( outbound_session.full ) {
          false
        } else {
          var msg = delivery.message.asInstanceOf[OpenwireMessage].message
          ack_handler.track(msg.getMessageId, delivery.ack)
          val dispatch = new MessageDispatch
          dispatch.setConsumerId(info.getConsumerId)
          dispatch.setDestination(msg.getDestination)
          dispatch.setMessage(msg)

          val rc = outbound_session.offer(dispatch)
          assert(rc, "offer should be accepted since it was not full")
          true
        }
      }
    }

    object ack_handler {

      // TODO: Need to validate all the range ack cases...
      var consumer_acks = ListBuffer[(MessageId, (Boolean, StoreUOW)=>Unit)]()

      def track(id:MessageId, callback:(Boolean, StoreUOW)=>Unit) = {
        queue {
          consumer_acks += (( id, callback ))
        }

      }

      def apply(messageAck: MessageAck, uow:StoreUOW=null) = {

        var found = false
        val (acked, not_acked) = consumer_acks.partition{ case (id, _)=>
          if( found ) {
            false
          } else {
            if( id == messageAck.getLastMessageId ) {
              found = true
            }
            true
          }
        }

        if( acked.isEmpty ) {
          async_fail("ACK failed, invalid message id: %s".format(messageAck.getLastMessageId), messageAck)
        } else {
          consumer_acks = not_acked
          acked.foreach{case (_, callback)=>
            if( callback!=null ) {
              callback(true, uow)
            }
          }
        }
      }

    }

  }

  class TransactionContext(val parent: ConnectionContext, val id: TransactionId) {

    // TODO: eventually we want to back this /w a broker Queue which
    // can provides persistence and memory swapping.
//    Buffer xid = null;
//    if (tid.isXATransaction()) {
//      xid = XidImpl.toBuffer((Xid) tid);
//    }
//    t = host.getTransactionManager().createTransaction(xid);
//    transactions.put(tid, t);

    val actions = ListBuffer[(StoreUOW)=>Unit]()

    def attach = {
      parent.transactions.put(id, this)
      all_transactions.put(id, this)
    }

    def dettach = {
      actions.clear
      parent.transactions.remove(id)
      all_transactions.remove(id)
    }

    def apply(proc:(StoreUOW)=>Unit) = {
      actions += proc
    }

    def commit(onComplete: => Unit) = {

      val uow = if( host.store!=null ) {
        host.store.create_uow
      } else {
        null
      }

      actions.foreach { proc =>
        proc(uow)
      }

      if( uow!=null ) {
        uow.on_complete(^{
          onComplete
        })
        uow.release
      } else {
        onComplete
      }

    }

    def rollback() = {
      actions.clear
    }

  }

  def create_tx_ctx(connection:ConnectionContext, txid:TransactionId):TransactionContext= {
    if ( all_transactions.contains(txid) ) {
      die("transaction allready started")
    } else {
      val context = new TransactionContext(connection, txid)
      context.attach
      context
    }
  }

  def get_or_create_tx_ctx(connection:ConnectionContext, txid:TransactionId):TransactionContext = {
    all_transactions.get(txid) match {
      case Some(ctx)=> ctx
      case None=>
        val context = new TransactionContext(connection, txid)
        context.attach
        context
    }
  }

  def get_tx_ctx(txid:TransactionId):TransactionContext = {
    all_transactions.get(txid) match {
      case Some(ctx)=> ctx
      case None=> die("transaction not active: %d".format(txid))
    }
  }

  def remove_tx_ctx(txid:TransactionId):TransactionContext= {
    all_transactions.get(txid) match {
      case None=>
        die("transaction not active: %d".format(txid))
      case Some(tx)=>
        tx.dettach
        tx
    }
  }

  ///////////////////////////////////////////////////////////////////
  // Connection / Session / Consumer / Producer state tracking.
  ///////////////////////////////////////////////////////////////////

  def on_connection_info(info: ConnectionInfo) = {
    val id = info.getConnectionId()
    if (!all_connections.contains(id)) {
      new ConnectionContext(info).attach
    }
    ack(info);
  }

  def on_session_info(info: SessionInfo) = {
    val id = info.getSessionId();
    if (!all_sessions.contains(id)) {
      val parent = all_connections.get(id.getParentId()).getOrElse(die("Cannot add a session to a connection that had not been registered."))
      new SessionContext(parent, info).attach
    }
    ack(info);
  }

  def on_producer_info(info: ProducerInfo) = {
    val id = info.getProducerId
    if (!all_producers.contains(id)) {
      val parent = all_sessions.get(id.getParentId()).getOrElse(die("Cannot add a producer to a session that had not been registered."))
      new ProducerContext(parent, info).attach
    }
    ack(info);
  }

  def on_consumer_info(info: ConsumerInfo) = {
    val id = info.getConsumerId
    if (!all_consumers.contains(id)) {
      val parent = all_sessions.get(id.getParentId()).getOrElse(die("Cannot add a consumer to a session that had not been registered."))
      new ConsumerContext(parent, info).attach
    } else {
      ack(info);
    }
  }

  def on_remove_info(info: RemoveInfo) = {
    info.getObjectId match {
      case id: ConnectionId => all_connections.get(id).foreach(_.dettach)
      case id: SessionId => all_sessions.get(id).foreach(_.dettach)
      case id: ProducerId => all_producers.get(id).foreach(_.dettach)
      case id: ConsumerId => all_consumers.get(id).foreach(_.dettach )
      // case id: DestinationInfo =>
      case _ => die("Invalid object id.")
    }
    ack(info)
  }

  ///////////////////////////////////////////////////////////////////
  // Methods for transaction management
  ///////////////////////////////////////////////////////////////////
  def on_transaction_info(info:TransactionInfo) = {
    val parent = all_connections.get(info.getConnectionId()).getOrElse(die("Cannot add a session to a connection that had not been registered."))
    val id = info.getTransactionId
    info.getType match {
      case TransactionInfo.BEGIN =>
        get_or_create_tx_ctx(parent, id)
        ack(info)

      case TransactionInfo.COMMIT_ONE_PHASE =>
        get_tx_ctx(id).commit {
          ack(info)
        }

      case TransactionInfo.ROLLBACK =>
        get_tx_ctx(id).rollback
        ack(info)

      case TransactionInfo.END =>
        die("XA not yet supported")
      case TransactionInfo.PREPARE =>
        die("XA not yet supported")
      case TransactionInfo.COMMIT_TWO_PHASE =>
        die("XA not yet supported")
      case TransactionInfo.RECOVER =>
        die("XA not yet supported")
      case TransactionInfo.FORGET =>
        die("XA not yet supported")

      case _ =>
        fail("Transaction info type unknown: " + info.getType)

    }
  }

  ///////////////////////////////////////////////////////////////////
  // Core message processing
  ///////////////////////////////////////////////////////////////////

  def on_message(msg: ActiveMQMessage) = {
    val producer = all_producers.get(msg.getProducerId).getOrElse(die("Producer associated with the message has not been registered."))

    if (msg.getOriginalDestination() == null) {
      msg.setOriginalDestination(msg.getDestination());
    }

    if( msg.getTransactionId==null ) {
      perform_send(msg)
    } else {
      get_or_create_tx_ctx(producer.parent.parent, msg.getTransactionId) { (uow)=>
        perform_send(msg, uow)
      }
    }
  }

  def perform_send(msg:ActiveMQMessage, uow:StoreUOW=null): Unit = {

    val destiantion = msg.getDestination.toDestination
    val key = destiantion.toList
    producerRoutes.get(key) match {
      case null =>
        // create the producer route...

        val route = new DeliveryProducerRoute(host.router) {
          override def connection = Some(OpenwireProtocolHandler.this.connection)
          override def dispatch_queue = queue
          refiller = ^ {
            resumeRead
          }
        }

        // don't process frames until producer is connected...
        connection.transport.suspendRead
        reset {
          val rc = host.router.connect(destiantion, route, security_context)
          rc match {
            case Some(failure) =>
              async_fail(failure, msg)
            case None =>
              if (!connection.stopped) {
                resumeRead
                producerRoutes.put(key, route)
                send_via_route(route, msg, uow)
              }
          }
        }

      case route =>
        // we can re-use the existing producer route
        send_via_route(route, msg, uow)

    }
  }

  def send_via_route(route:DeliveryProducerRoute, message:ActiveMQMessage, uow:StoreUOW) = {
    if( !route.targets.isEmpty ) {

      // We may need to add some headers..
      val delivery = new Delivery
      delivery.message = new OpenwireMessage(message)
      delivery.size = message.getSize
      delivery.uow = uow

      if( message.isResponseRequired ) {
        delivery.ack = { (consumed, uow) =>
          dispatchQueue <<| ^{
            ack(message)
          }
        }
      }

      // routes can always accept at least 1 delivery...
      assert( !route.full )
      route.offer(delivery)
      if( route.full ) {
        // but once it gets full.. suspend, so that we get more messages
        // until it's not full anymore.
        suspendRead("blocked destination: "+route.overflowSessions.mkString(", "))
      }

    } else {
      // info("Dropping message.  No consumers interested in message.")
      ack(message)
    }
    //    message.release
  }


  def on_message_ack(info:MessageAck) = {
    val consumer = all_consumers.get(info.getConsumerId).getOrElse(die("Cannot ack a message on a consumer that had not been registered."))
    info.getTransactionId match {
      case null =>
        consumer.ack_handler(info)
      case txid =>
        get_or_create_tx_ctx(consumer.parent.parent, txid){ (uow)=>
          consumer.ack_handler(info, uow)
        }
    }
    ack(info)
  }

  //  public Response processAddDestination(DestinationInfo info) throws Exception {
  //      ActiveMQDestination destination = info.getDestination();
  //      if (destination.isTemporary()) {
  //          // Keep track of it so that we can remove them this connection
  //          // shuts down.
  //          temporaryDestinations.add(destination);
  //      }
  //      host.createQueue(destination);
  //      return ack(info);
  //  }


  //


}
