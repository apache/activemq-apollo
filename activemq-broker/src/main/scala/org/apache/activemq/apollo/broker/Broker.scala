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

import _root_.java.io.{File}
import _root_.java.lang.{String}
import _root_.org.apache.activemq.util.{FactoryFinder}
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import org.fusesource.hawtdispatch.{Dispatch, DispatchQueue, BaseRetained}
import org.fusesource.hawtbuf._
import ReporterLevel._
import AsciiBuffer._
import org.apache.activemq.apollo.dto.{VirtualHostDTO, BrokerDTO}
import collection.{JavaConversions, SortedMap}
import java.util.LinkedList

/**
 * <p>
 * The BrokerFactory creates Broker objects from a URI.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object BrokerFactory {

    val BROKER_FACTORY_HANDLER_FINDER = new FactoryFinder("META-INF/services/org/apache/activemq/apollo/broker/");

    trait Handler {
        def createBroker(brokerURI:String):Broker
    }


    def createHandler(name:String):Handler = {
      BROKER_FACTORY_HANDLER_FINDER.newInstance(name).asInstanceOf[Handler]
    }

    /**
     * Creates a broker from a URI configuration
     *
     * @param brokerURI the URI scheme to configure the broker
     * @param startBroker whether or not the broker should have its
     *                {@link Broker#start()} method called after
     *                construction
     * @throws Exception
     */
    def createBroker(brokerURI:String, startBroker:Boolean=false):Broker = {
      var scheme = FactoryFinder.getScheme(brokerURI)
      if (scheme==null ) {
          throw new IllegalArgumentException("Invalid broker URI, no scheme specified: " + brokerURI)
      }
      var handler = createHandler(scheme)
      var broker = handler.createBroker(brokerURI)
      if (startBroker) {
          broker.start();
      }
      return broker;
    }

}


object BufferConversions {

  implicit def toAsciiBuffer(value:String) = new AsciiBuffer(value)
  implicit def toUTF8Buffer(value:String) = new UTF8Buffer(value)
  implicit def fromAsciiBuffer(value:AsciiBuffer) = value.toString
  implicit def fromUTF8Buffer(value:UTF8Buffer) = value.toString

  implicit def toAsciiBuffer(value:Buffer) = value.ascii
  implicit def toUTF8Buffer(value:Buffer) = value.utf8
}




object Broker extends Log {

  val STICK_ON_THREAD_QUEUES = true

  /**
   * Creates a default a configuration object.
   */
  def default() = {
    val rc = new BrokerDTO
    rc.id = "default"
    rc.enabled = true
    rc.virtualHosts.add(VirtualHost.default)
    rc.connectors.add(Connector.default)
    rc.basedir = "./activemq-data/default"
    rc
  }

  /**
   * Validates a configuration object.
   */
  def validate(config: BrokerDTO, reporter:Reporter):ReporterLevel = {
    new Reporting(reporter) {
      if( empty(config.id) ) {
        error("Broker id must be specified.")
      }
      if( config.virtualHosts.isEmpty ) {
        error("Broker must define at least one virtual host.")
      }
      if( empty(config.basedir) ) {
        error("Broker basedir must be defined.")
      }

      import JavaConversions._
      for (host <- config.virtualHosts ) {
        result |= VirtualHost.validate(host, reporter)
      }
      for (connector <- config.connectors ) {
        result |= Connector.validate(connector, reporter)
      }
    }.result
  }
}

/**
 * <p>
 * A Broker is parent object of all services assoicated with the serverside of
 * a message passing system.  It keeps track of all running connections,
 * virtual hosts and assoicated messaging destintations.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Broker() extends BaseService with DispatchLogging with LoggingReporter {
  
  import Broker._
  override protected def log = Broker

  var config: BrokerDTO = default

  var dataDirectory: File = null
  var defaultVirtualHost: VirtualHost = null
  var virtualHosts: Map[AsciiBuffer, VirtualHost] = Map()
  var connectors: List[Connector] = Nil

  val dispatchQueue = createQueue("broker");
  if( STICK_ON_THREAD_QUEUES ) {
    dispatchQueue.setTargetQueue(Dispatch.getRandomThreadQueue)
  }

  def id = config.id

  override def toString() = "broker: "+id 

  /**
   * Validates and then applies the configuration.
   */
  def configure(config: BrokerDTO, reporter:Reporter) = ^{
    if ( validate(config, reporter) < ERROR ) {
      this.config = config

      if( serviceState.isStarted ) {
        // TODO: apply changes while he broker is running.
        reporter.report(WARN, "Updating broker configuration at runtime is not yet supported.  You must restart the broker for the change to take effect.")

      }
    }
  } >>: dispatchQueue


  override def _start(onCompleted:Runnable) = {

    // create the runtime objects from the config
    {
      import JavaConversions._
      dataDirectory = new File(config.basedir)
      defaultVirtualHost = null
      for (c <- config.virtualHosts) {
        val host = new VirtualHost(this)
        host.configure(c, this)
        virtualHosts += ascii(c.id)-> host
        // first defined host is the default virtual host
        if( defaultVirtualHost == null ) {
          defaultVirtualHost = host
        }
      }
      for (c <- config.connectors) {
        val connector = new Connector(this)
        connector.configure(c, this)
        connectors ::= connector
      }
    }

    // Start them up..
    val tracker = new LoggingTracker("broker startup", dispatchQueue)
    virtualHosts.valuesIterator.foreach( x=>
      tracker.start(x)
    )
    connectors.foreach( x=>
      tracker.start(x)
    )

    tracker.callback(onCompleted)
  }


  def _stop(onCompleted:Runnable): Unit = {
    val tracker = new LoggingTracker("broker shutdown", dispatchQueue)
    // Stop accepting connections..
    connectors.foreach( x=>
      tracker.stop(x)
    )
    // Shutdown the virtual host services
    virtualHosts.valuesIterator.foreach( x=>
      tracker.stop(x)
    )
    tracker.callback(onCompleted)
  }

  def getVirtualHost(name: AsciiBuffer, cb: (VirtualHost) => Unit) = reply(cb) {
    virtualHosts.getOrElse(name, null)
  } >>: dispatchQueue

  def getDefaultVirtualHost(cb: (VirtualHost) => Unit) = reply(cb) {
    defaultVirtualHost
  } >>: dispatchQueue

}


/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait QueueLifecyleListener {

    /**
     * A destination has bean created
     *
     * @param queue
     */
    def onCreate(queue:Queue);

    /**
     * A destination has bean destroyed
     *
     * @param queue
     */
    def onDestroy(queue:Queue);

}




object Queue extends Log {
  val maxOutboundSize = 1024*1204*5
}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Queue(val destination:Destination) extends BaseRetained with Route with DeliveryConsumer with DeliveryProducer with DispatchLogging {
  override protected def log = Queue

  override val dispatchQueue:DispatchQueue = createQueue("queue:"+destination);
  dispatchQueue.setTargetQueue(getRandomThreadQueue)
  dispatchQueue {
    debug("created queue for: "+destination)
  }

  val delivery_buffer  = new DeliveryBuffer
  delivery_buffer.eventHandler = ^{ drain_delivery_buffer }

  val session_manager = new DeliverySessionManager(delivery_buffer, dispatchQueue)

  setDisposer(^{
    dispatchQueue.release
    session_manager.release
  })

  class ConsumerState(val consumer:DeliverySession) {
    var bound=true

    def deliver(value:Delivery):Unit = {
      val delivery = Delivery(value)
      delivery.setDisposer(^{
        ^{ completed(value) } >>:dispatchQueue
      })
      consumer.deliver(delivery);
      delivery.release
    }

    def completed(delivery:Delivery) = {
      // Lets get back on the readyList if  we are still bound.
      if( bound ) {
        readyConsumers.addLast(this)
      }
      delivery_buffer.ack(delivery)
    }
  }

  var allConsumers = Map[DeliveryConsumer,ConsumerState]()
  val readyConsumers = new LinkedList[ConsumerState]()

  def connected(consumers:List[DeliveryConsumer]) = bind(consumers)
  def bind(consumers:List[DeliveryConsumer]) = retaining(consumers) {
      for ( consumer <- consumers ) {
        val cs = new ConsumerState(consumer.open_session(dispatchQueue))
        allConsumers += consumer->cs
        readyConsumers.addLast(cs)
      }
      drain_delivery_buffer
    } >>: dispatchQueue

  def unbind(consumers:List[DeliveryConsumer]) = releasing(consumers) {
      for ( consumer <- consumers ) {
        allConsumers.get(consumer) match {
          case Some(cs)=>
            cs.bound = false
            cs.consumer.close
            allConsumers -= consumer
            readyConsumers.remove(cs)
          case None=>
        }
      }
    } >>: dispatchQueue

  def disconnected() = throw new RuntimeException("unsupported")

  def collocate(value:DispatchQueue):Unit = {
    if( value.getTargetQueue ne dispatchQueue.getTargetQueue ) {
      println(dispatchQueue.getLabel+" co-locating with: "+value.getLabel);
      this.dispatchQueue.setTargetQueue(value.getTargetQueue)
    }
  }


  def drain_delivery_buffer: Unit = {
    while (!readyConsumers.isEmpty && !delivery_buffer.isEmpty) {
      val cs = readyConsumers.removeFirst
      val delivery = delivery_buffer.receive
      cs.deliver(delivery)
    }
  }

  def open_session(producer_queue:DispatchQueue) = new DeliverySession {

    val session = session_manager.session(producer_queue)
    val consumer = Queue.this
    retain

    def deliver(delivery:Delivery) = session.send(delivery)

    def close = {
      session.close
      release
    }
  }

  def matches(message:Delivery) = { true }

//  def open_session(producer_queue:DispatchQueue) = new ConsumerSession {
//    val consumer = StompQueue.this
//    val deliveryQueue = new DeliveryOverflowBuffer(delivery_buffer)
//    retain
//
//    def deliver(delivery:Delivery) = using(delivery) {
//      deliveryQueue.send(delivery)
//    } >>: queue
//
//    def close = {
//      release
//    }
//  }


}

class XQueue(val destination:Destination) {

// TODO:
//    private VirtualHost virtualHost;
//
//    Queue() {
//        this.queue = queue;
//    }
//
//    /*
//     * (non-Javadoc)
//     *
//     * @see
//     * org.apache.activemq.broker.DeliveryTarget#deliver(org.apache.activemq
//     * .broker.MessageDelivery, org.apache.activemq.flow.ISourceController)
//     */
//    public void deliver(MessageDelivery message, ISourceController<?> source) {
//        queue.add(message, source);
//    }
//
//    public final void addSubscription(final Subscription<MessageDelivery> sub) {
//        queue.addSubscription(sub);
//    }
//
//    public boolean removeSubscription(final Subscription<MessageDelivery> sub) {
//        return queue.removeSubscription(sub);
//    }
//
//    public void start() throws Exception {
//        queue.start();
//    }
//
//    public void stop() throws Exception {
//        if (queue != null) {
//            queue.stop();
//        }
//    }
//
//    public void shutdown(Runnable onShutdown) throws Exception {
//        if (queue != null) {
//            queue.shutdown(onShutdown);
//        }
//    }
//
//    public boolean hasSelector() {
//        return false;
//    }
//
//    public boolean matches(MessageDelivery message) {
//        return true;
//    }
//
//    public VirtualHost getBroker() {
//        return virtualHost;
//    }
//
//    public void setVirtualHost(VirtualHost virtualHost) {
//        this.virtualHost = virtualHost;
//    }
//
//    public void setDestination(Destination destination) {
//        this.destination = destination;
//    }
//
//    public final Destination getDestination() {
//        return destination;
//    }
//
//    public boolean isDurable() {
//        return true;
//    }
//
//    public static class QueueSubscription implements BrokerSubscription {
//        Subscription<MessageDelivery> subscription;
//        final Queue queue;
//
//        public QueueSubscription(Queue queue) {
//            this.queue = queue;
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.broker.BrokerSubscription#connect(org.apache.
//         * activemq.broker.protocol.ProtocolHandler.ConsumerContext)
//         */
//        public void connect(ConsumerContext subscription) throws UserAlreadyConnectedException {
//            this.subscription = subscription;
//            queue.addSubscription(subscription);
//        }
//
//        /*
//         * (non-Javadoc)
//         *
//         * @see
//         * org.apache.activemq.broker.BrokerSubscription#disconnect(org.apache
//         * .activemq.broker.protocol.ProtocolHandler.ConsumerContext)
//         */
//        public void disconnect(ConsumerContext context) {
//            queue.removeSubscription(subscription);
//        }
//
//        /* (non-Javadoc)
//         * @see org.apache.activemq.broker.BrokerSubscription#getDestination()
//         */
//        public Destination getDestination() {
//            return queue.getDestination();
//        }
//    }

  // TODO:
  def matches(message:Delivery) = false
  def deliver(message:Delivery) = {
    // TODO:
  }

  def getDestination() = destination

  def shutdown = {}
}
