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
package org.apache.activemq.apollo.amqp

import java.util.concurrent.TimeUnit
import java.util.Date
import scala.collection.mutable.{ListBuffer, HashMap}

import org.fusesource.hawtdispatch._
import org.fusesource.hawtbuf._
import org.fusesource.hawtbuf.Buffer._

import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.dto._
import org.apache.activemq.apollo.broker._
import org.apache.activemq.apollo.util.path.{PathParser, Path, LiteralPart}
import org.apache.activemq.apollo.selector.SelectorParser
import org.apache.activemq.apollo.filter.{BooleanExpression, FilterException}
import org.apache.activemq.apollo.broker.protocol.ProtocolHandler
import org.apache.activemq.apollo.broker.security.SecurityContext
import org.apache.activemq.apollo.amqp.dto._

import org.fusesource.amqp._
import org.fusesource.amqp.callback._
import org.fusesource.amqp.callback.Callback
import org.fusesource.amqp.types._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object AMQPMessage {

  def apply(annotated:Envelope):AMQPMessage = {
    val payload = MessageSupport.toBuffer(annotated)
    val rc = AMQPMessage(payload)
    rc._annotated = annotated
    rc
  }
}

case class AMQPMessage(payload:Buffer) extends org.apache.activemq.apollo.broker.Message {
  import AmqpProtocolHandler._
  def protocol = AmqpProtocol

  var _annotated:Envelope = _
  def annotated = {
    if ( _annotated ==null ) {
      _annotated = MessageSupport.decodeEnvelope(payload)
    }
    _annotated
  }
  
  def getBodyAs[T](toType : Class[T]): T = {
    if (toType == classOf[Buffer]) {
      payload
    } else if( toType == classOf[String] ) {
      payload.utf8
    } else if (toType == classOf[AsciiBuffer]) {
      payload.ascii
    } else if (toType == classOf[UTF8Buffer]) {
      payload.utf8
    } else {
      null
    }
  }.asInstanceOf[T]

  def getLocalConnectionId: AnyRef = annotated.getDeliveryAnnotations.getValue.get(SENDER_CONTAINER_KEY) match {
    case x:AMQPString => x.getValue
    case _ => null
  }

  def getProperty(name: String)= annotated match {
    case null => null
    case annotated =>
      annotated.getMessage.getApplicationProperties match {
        case null => null
        case props =>
          props.getValue.get(new AMQPString(name)).asInstanceOf[Object]
      }
  }

  def release() {}
  def retain() {}
  def retained(): Int = 0
}


object AmqpProtocolHandler extends Log {
  val SENDER_CONTAINER_KEY = new AMQPString("sender-container")

  // How long we hold a failed connection open so that the remote end
  // can get the resulting error message.
  val DEFAULT_DIE_DELAY = 5*1000L
  val WAITING_ON_CLIENT_REQUEST = ()=> "client request"

  val DEFAULT_DETINATION_PARSER = new DestinationParser
  DEFAULT_DETINATION_PARSER.queue_prefix = "/queue/"
  DEFAULT_DETINATION_PARSER.topic_prefix = "/topic/"
  DEFAULT_DETINATION_PARSER.dsub_prefix = "/dsub/"
  DEFAULT_DETINATION_PARSER.temp_queue_prefix = "/temp-queue/"
  DEFAULT_DETINATION_PARSER.temp_topic_prefix = "/temp-topic/"
  DEFAULT_DETINATION_PARSER.destination_separator = ","
  DEFAULT_DETINATION_PARSER.path_separator = "."
  DEFAULT_DETINATION_PARSER.any_child_wildcard = "*"
  DEFAULT_DETINATION_PARSER.any_descendant_wildcard = "**"

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class AmqpProtocolHandler extends ProtocolHandler {
  import AmqpProtocolHandler._

  val security_context = new SecurityContext

  var connection_log:Log = AmqpProtocolHandler
  var host:VirtualHost = null
  var waiting_on = WAITING_ON_CLIENT_REQUEST
  var config:AmqpDTO = _
  var dead = false

  def session_id = security_context.session_id
  def protocol = AmqpCodec.PROTOCOL
  def broker = connection.connector.broker
  def queue = connection.dispatch_queue

  def die_delay = {
    OptionSupport(config.die_delay).getOrElse(DEFAULT_DIE_DELAY)
  }

  lazy val buffer_size = MemoryPropertyEditor.parse(Option(config.buffer_size).getOrElse("640k")).toInt
  var amqp_connection:AMQPConnection = _
  var messages_sent = 0L
  var messages_received = 0L

  override def create_connection_status = {
    var rc = new AmqpConnectionStatusDTO
    rc.protocol_version = "1.0.0"
    rc.user = security_context.user
//    rc.subscription_count = consumers.size
    rc.waiting_on = waiting_on()
    rc.messages_sent = messages_sent
    rc.messages_received = messages_received
    rc
  }

  class ProtocolException(msg:String) extends RuntimeException(msg)
  class Break extends RuntimeException

  private def async_die(msg:String, e:Throwable=null) = try {
    die(msg, e)
  } catch {
    case x:Break=>
  }

  private def die[T](msg:String, e:Throwable=null):T = {
    if( e!=null) {
      connection_log.info(e, "AMQP connection '%s' error: %s", security_context.remote_address, msg, e)
    } else {
      connection_log.info("AMQP connection '%s' error: %s", security_context.remote_address, msg)
    }
    if( !dead ) {
      dead = true
      waiting_on = ()=>"shutdown"
      connection.transport.resumeRead

      // TODO: if there are too many open connections we should just close the connection
      // without waiting for the error to get sent to the client.
      queue.after(die_delay, TimeUnit.MILLISECONDS) {
        connection.stop(NOOP)
      }
    }
    throw new Break()
  }

  override def set_connection(connection: BrokerConnection) = {
    super.set_connection(connection)
    import collection.JavaConversions._

    val connector_config = connection.connector.config.asInstanceOf[AcceptingConnectorDTO]
    config = connector_config.protocols.find( _.isInstanceOf[AmqpDTO]).map(_.asInstanceOf[AmqpDTO]).getOrElse(new AmqpDTO)

    val options = new AMQPServerConnectionOptions
    options.setTransport(connection.transport);
    options.setMaxFrameSize(1024*4)
    options.setIdleTimeout(-1);
    options.setLogger(new AMQPConnectionOptions.Logger {
      override def _debug(format: String, args: Array[AnyRef]) {
        println(System.currentTimeMillis()+": "+format.format(args: _*))
        // connection_log.debug(format, args:_*)
      }
      override def _trace(format: String, args: Array[AnyRef]) {
        println(System.currentTimeMillis()+": "+format.format(args: _*))
        // connection_log.trace(format, args:_*)
      }
    })
    options.setListener(new AMQPConnection.Listener(){

      override def onBegin(begin: Begin) = {
        val rc = new AMQPServerSessionOptions
        rc.setIncomingWindow(100)
        rc.setOutgoingWindow(100)
        rc.setListener(session_listener)
        rc
      }

      override def onAccepted(session: AMQPSession) {
        connection_log.info("accepted: "+session)
      }
      override def onException(error: Throwable) {
        error.printStackTrace();
      }

      override def onOpen(request: Open, response: Open, callback: Callback[Open]) {
        handle_open(request, response, callback)
      }
    })
    amqp_connection = AMQP.open(options, new Callback[AMQPConnection] {
      override def onSuccess(value: AMQPConnection) {
        println("AMQP connection is open.")
      }
      override def onFailure(value: Throwable) {
        println("Failed to open AMQP connection: "+value)
      }
    })

  }
  override def on_transport_connected() = sys.error("should not get called")
  override def on_transport_disconnected() = sys.error("should not get called")
  override def on_transport_command(command:AnyRef):Unit = sys.error("should not get called")

  def suspend_read(reason: =>String) = {
    waiting_on = reason _
    connection.transport.suspendRead
    // heart_beat_monitor.suspendRead
  }
  def resume_read() = {
    waiting_on = WAITING_ON_CLIENT_REQUEST
    connection.transport.resumeRead
    // heart_beat_monitor.resumeRead
  }

  def handle_open(request: Open, response: Open, callback: Callback[Open]) = {
    broker.dispatch_queue {
      suspend_read("host lookup")
      val host = request.getHostname match {
        case null => broker.default_virtual_host
        case host=> broker.get_virtual_host(ascii(host))
      }
      queue {
        resume_read
        if(host==null) {
          callback.onFailure(new AMQPException("Invalid virtual host: "+host));
        } else if(!host.service_state.is_started) {
          callback.onFailure(new AMQPException("virtual host not ready"));
        } else {
          response.setContainerID(host.id)
          this.host=host
          callback.onSuccess(response)
//          security_context.session_id = Some("%s-%x".format(this.host.config.id, this.host.session_counter.incrementAndGet))
//          connection_log = host.connection_log
//          if( host.authenticator!=null &&  host.authorizer!=null ) {
//            suspend_read("authenticating and authorizing connect")
//            host.authenticator.authenticate(security_context) { auth_failure=>
//              dispatchQueue {
//                if( auth_failure!=null ) {
//                  async_die("%s. Credentials=%s".format(auth_failure, security_context.credential_dump))
//                } else if( !host.authorizer.can(security_context, "connect", connection.connector) ) {
//                  async_die("Not authorized to connect to connector '%s'. Principals=%s".format(connection.connector.id, security_context.principal_dump))
//                } else if( !host.authorizer.can(security_context, "connect", this.host) ) {
//                  async_die("Not authorized to connect to virtual host '%s'. Principals=%s".format(this.host.id, security_context.principal_dump))
//                } else {
//                  resume_read
//                  send_connected
//                }
//              }
//            }
//          } else {
//            send_connected
//          }
        }
      }
    }
  }

  var destination_parser = DEFAULT_DETINATION_PARSER
  var temp_destination_map = HashMap[SimpleAddress, SimpleAddress]()

  def decode_addresses(value:String):Array[SimpleAddress] = {
    val rc = destination_parser.decode_multi_destination(value)
    if( rc==null ) {
      return null
    }
    rc.map { dest =>
      if( dest.domain.startsWith("temp-") ) {
        temp_destination_map.getOrElseUpdate(dest, {
          val parts = LiteralPart("temp") :: LiteralPart(broker.id) :: LiteralPart(session_id.get) :: dest.path.parts
          SimpleAddress(dest.domain.stripPrefix("temp-"), Path(parts))
        })
      } else {
        dest
      }
    }
  }

  class AmqpProducerRoute(val dest: String, val addresses:Array[SimpleAddress]) extends DeliveryProducerRoute(host.router) {

    val key = addresses.toList
    var is_connected = false

    override def send_buffer_size = buffer_size

    override def connection = Some(AmqpProtocolHandler.this.connection)

    override def connected() = is_connected = true

    override def dispatch_queue = queue

    refiller = ^ {
      resume_read
    }
  }

  object session_listener extends AMQPSession.Listener{
    override def onAttach(attach: Attach, callback: Callback[AMQPEndpoint]) {
      if( attach.getRole == Role.SENDER.getValue ) {
        val target = attach.getTarget.asInstanceOf[Target]
        target.getAddress match {
          case address:AMQPString =>
            var dest = address.getValue
            decode_addresses(dest) match {
              case null => callback.onFailure(new Exception("Invaild address: "+dest))
              case addresses => attach_sender(attach, dest, addresses, callback)
            }
          case _ =>
            callback.onFailure(new Exception("Invaild address: "+target.getAddress))
        }
      } else {
        val source = attach.getSource.asInstanceOf[Source]
        source.getAddress match {
          case address:AMQPString =>
            var dest = address.getValue
            decode_addresses(dest) match {
              case null => callback.onFailure(new Exception("Invaild address: "+dest))
              case addresses => attach_receiver(attach, dest, addresses, callback)
            }
          case _ =>
            callback.onFailure(new Exception("Invaild address: "+source.getAddress))
        }
      }
    }

    override def onClose(error: Error) {
      if( error!=null ) {
        info("peer closed the AMQP session due to: "+error)
      }
    }

  }

  def attach_sender(attach: Attach, address: String, addresses:Array[SimpleAddress], callback: Callback[AMQPEndpoint]) {
    val target = new AmqpProducerRoute(address, addresses)
    var receiver: AMQPReceiver = null
    // create the producer route...
    val options = new AMQPReceiverOptions();
    options.setSource(attach.getSource.asInstanceOf[Source])
    options.setTarget(attach.getTarget.asInstanceOf[Target])
    options.setName(attach.getName)
    options.setSenderSettleMode(SenderSettleMode.valueOf(attach.getSndSettleMode))
    options.setReceiverSettleMode(ReceiverSettleMode.valueOf(attach.getRcvSettleMode))
    options.setMaxMessageSize(10 * 1024 * 1024);

    def pump = {
      while (target.is_connected && !target.full && receiver.peek() != null) {

        val amqpDelivery = receiver.poll()

        // Update the message /w who sent it to us..
        val amqpMessage = amqpDelivery.getMessage;
        if (amqpMessage.getDeliveryAnnotations == null) {
          amqpMessage.setDeliveryAnnotations(new DeliveryAnnotations(new MapEntries()))
        }
        amqpMessage.getDeliveryAnnotations.getValue.add(SENDER_CONTAINER_KEY, new AMQPString(amqp_connection.remoteContainerId()))

        val apolloDelivery = new Delivery
        apolloDelivery.message = AMQPMessage(amqpMessage)
        apolloDelivery.size = amqpDelivery.payload.length()
        //                delivery.expiration = message.expiration
        //                delivery.persistent = message.persistent
        //                delivery.uow = uow
        //                get(frame.headers, RETAIN).foreach { retain =>
        //                  delivery.retain = retain match {
        //                    case SET => RetainSet
        //                    case REMOVE => RetainRemove
        //                    case _ => RetainIgnore
        //                  }
        //                }

        if (!amqpDelivery.isSettled) {
          apolloDelivery.ack = {
            (consumed, uow) =>
              queue <<| ^ {
                amqpDelivery.ack()
              }
          }
        }

        target.offer(apolloDelivery)
      }
    }

    target.refiller = ^ {
      pump
    }
    options.setListener(new AMQPEndpoint.Listener {
      override def onTransfer() = pump

      override def onClosed(senderClosed: Boolean, error: Error) {
        if (error != null) {
          debug("Peer closed link due to error: %s", error)
        }
        host.dispatch_queue {
          host.router.disconnect(target.addresses, target)
        }
      }
    })

    // start with 0 credit window so that we don't receive any messages
    // until we have verified if that we can connect to the destination..
    options.credit = 0
    receiver = AMQP.createReceiver(options)
    callback.onSuccess(receiver)

    host.dispatch_queue {
      val rc = host.router.connect(target.addresses, target, security_context)
      queue {
        rc match {
          case Some(failure) =>
            receiver.detach(true, failure, null)
          case None =>
            // Add credit to start receiving messages.
            receiver.addCredit(50)
        }
      }
    }
  }
  var protocol_convert = "full"

  class AMQPConsumer(
      val subscription_id:String,
      val addresses:Array[_ <: BindAddress],
      val selector:(String, BooleanExpression),
      override val browser:Boolean,
      override val exclusive:Boolean,
      val include_seq:Option[Long],
      val from_seq:Long,
      override val close_on_drain:Boolean
    ) extends BaseRetained with DeliveryConsumer {

    var sender:AMQPSender = _

    override def toString = "amqp subscription:"+subscription_id+", remote address: "+security_context.remote_address

    ///////////////////////////////////////////////////////////////////
    // DeliveryConsumer Interface..
    ///////////////////////////////////////////////////////////////////
    def connect(p:DeliveryProducer) = new AMQPConsumerSession(p)
    def dispatch_queue = queue
    override def connection = Option(AmqpProtocolHandler.this.connection)
    def is_persistent = false
    def matches(message: Delivery) = true

    override def start_from_tail = from_seq == -1
    override def jms_selector = if(selector!=null){ selector._1 } else { null }
    override def user = security_context.user

    var starting_seq:Long = 0L
    override def set_starting_seq(seq: Long):Unit = {
      starting_seq=seq
    }

    ///////////////////////////////////////////////////////////////////
    // Sink[(Session[Delivery], Delivery)] interface..
    ///////////////////////////////////////////////////////////////////
    object sink extends Sink[(Session[Delivery], Delivery)] {
      var refiller: Task = NOOP
      def full: Boolean = sender.full()
      def offer(value: (Session[Delivery], Delivery)): Boolean = {
        if( full ) {
          return false
        }
        
        val (session, delivery) = value
        val message = delivery.message

        
        val header = new Header()
        header.setDeliveryCount(delivery.redeliveries)
        header.setDurable(delivery.persistent)
        header.setFirstAcquirer(delivery.redeliveries == 0)

//        if( include_seq.isDefined ) {
//          frame = frame.append_headers((include_seq.get, ascii(delivery.seq.toString))::Nil)
//        }

        var annotated = if( message.protocol eq AmqpProtocol ) {
          val original = message.asInstanceOf[AMQPMessage].annotated
          var annotated = new Envelope
          annotated.setHeader(header)
          annotated.setDeliveryAnnotations(original.getDeliveryAnnotations)
          annotated.setMessageAnnotations(original.getMessageAnnotations)
          annotated.setMessage(original.getMessage)
          annotated.setFooter(original.getFooter)
          annotated
        } else {
          
          val (body, content_type) =  protocol_convert match{
            case "body" => (message.getBodyAs(classOf[Buffer]), "protocol/"+message.protocol.id()+";conv=body")
            case _ => (message.encoded, "protocol/"+message.protocol.id())
          }
          
          val bare = new types.Message
          bare.setData(new Data(body))
          var properties = new Properties()
          properties.setContentType(ascii(content_type))
          if( delivery.expiration!= 0 ) {
            properties.setAbsoluteExpiryTime(new Date(delivery.expiration))
          }
          bare.setProperties(properties)
          var annotated = new Envelope
          annotated.setHeader(header)
          annotated.setMessage(bare)
          annotated
        }
        
        sender.send(annotated, null)

        messages_sent += 1
        return true
      }
    }

    ///////////////////////////////////////////////////////////////////
    // AMQPEndpoint.Listener interface..
    ///////////////////////////////////////////////////////////////////
    object endpoint_listener extends AMQPEndpoint.Listener {
      override def onTransfer = queue {
        sink.refiller.run()
      }
      override def onClosed(senderClosed: Boolean, error: Error) {
        if (error != null) {
          debug("Peer closed link due to error: %s", error)
        }
        host.dispatch_queue {
          host.router.unbind(addresses, AMQPConsumer.this, false, security_context)
        }
      }
    }

    val session_manager = new SessionSinkMux[Delivery](sink, queue, Delivery) {
      override def time_stamp = broker.now
    }

    class AMQPConsumerSession(p:DeliveryProducer) extends DeliverySession with SessionSinkFilter[Delivery] {

      def producer = p
      def consumer = AMQPConsumer.this
      val downstream = session_manager.open(producer.dispatch_queue, 1, buffer_size)

      // Delegate all the flow control stuff to the session
      override def full = {
        val rc = super.full
        rc
      }

      def offer(delivery:Delivery) = {
        if( full ) {
          false
        } else {
          delivery.message.retain()
          val rc = downstream.offer(delivery)
          assert(rc, "offer should be accepted since it was not full")
          true
        }
      }

      def close {}
    }
  }

  def attach_receiver(attach: Attach, address: String, requested_addresses:Array[SimpleAddress], callback: Callback[AMQPEndpoint]) = try {
    val options = new AMQPSenderOptions();

    val src = attach.getSource.asInstanceOf[Source]
    src.setDefaultOutcome(new Released())

    if (attach.getSndSettleMode == SenderSettleMode.SETTLED.getValue) {
      // if we are settling... then no other outcomes are possible..
      src.setOutcomes(Array())
    } else {
      src.setOutcomes(Array(
        new AMQPSymbol(Accepted.SYMBOLIC_ID),
        new AMQPSymbol(Rejected.SYMBOLIC_ID),
        new AMQPSymbol(Released.SYMBOLIC_ID),
        new AMQPSymbol(Modified.SYMBOLIC_ID)
      ))
    }
    options.setSource(src)

    options.setTarget(attach.getTarget.asInstanceOf[Target])
    options.setName(attach.getName)
    options.setSenderSettleMode(SenderSettleMode.valueOf(attach.getSndSettleMode))
    options.setReceiverSettleMode(ReceiverSettleMode.valueOf(attach.getRcvSettleMode))
    options.setMaxMessageSize(10 * 1024 * 1024);


    val subscription_id = attach.getName
    var persistent = false
    var browser = false
    var browser_end = browser && true
    var exclusive = !browser && false
    var include_seq: Option[Long] = None
    val from_seq_opt: Option[Long] = None

    def is_multi_destination = if (requested_addresses.length > 1) {
      true
    } else {
      PathParser.containsWildCards(requested_addresses(0).path)
    }
    if (from_seq_opt.isDefined && is_multi_destination) {
      die("The from-seq header is only supported when you subscribe to one destination");
    }

    val selector = attach.getProperties match {
      case null => null
      case properties =>
        properties.get(new AMQPString("selector")) match {
          case null => null
          case x:AMQPString =>
            try {
              (x.getValue, SelectorParser.parse(x.getValue))
            } catch {
              case e: FilterException =>
                die("Invalid selector expression '%s': ".format(x, e.getMessage))
            }
          case x =>
            die("Invalid selector expression '%s'".format(x))
        }
    }

    val addresses:Array[_ <: BindAddress] = if (persistent) {
      val dsubs = ListBuffer[BindAddress]()
      val topics = ListBuffer[BindAddress]()
      requested_addresses.foreach { address =>
          address.domain match {
            case "dsub" => dsubs += address
            case "topic" => topics += address
            case _ => die("A persistent subscription can only be used on a topic destination")
          }
      }
      val s = if (selector == null) null else selector._1
      dsubs += SubscriptionAddress(destination_parser.decode_path(subscription_id), s, topics.toArray)
      dsubs.toArray
    } else {
      requested_addresses
    }

    val from_seq = from_seq_opt.getOrElse(0L)

    val source = new AMQPConsumer(subscription_id, addresses, selector, browser, exclusive, include_seq, from_seq, browser_end);
    options.setListener(source.endpoint_listener)
    source.sender = AMQP.createSender(options)

    host.dispatch_queue {
      val rc = host.router.bind(addresses, source, security_context)
      source.release
      queue {
        rc match {
          case Some(failure) =>
            source.sender.detach(true, failure, null)
          case None =>
        }
      }
    }
    callback.onSuccess(source.sender)
  } catch {
    case e => callback.onFailure(e)
  }

}

