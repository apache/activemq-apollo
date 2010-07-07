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
import _root_.java.util.{LinkedList, LinkedHashMap, ArrayList, HashMap}
import _root_.org.apache.activemq.transport._
import _root_.org.apache.activemq.Service
import _root_.java.lang.{String}
import _root_.org.apache.activemq.util.buffer.{Buffer, UTF8Buffer, AsciiBuffer}
import _root_.org.apache.activemq.util.{FactoryFinder, IOHelper}
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import _root_.org.fusesource.hawtdispatch.{DispatchQueue, BaseRetained}

import _root_.scala.collection.JavaConversions._
import _root_.scala.reflect.BeanProperty

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

import BufferConversions._

object BrokerConstants extends Log {
  val CONFIGURATION = "CONFIGURATION"
  val STOPPED = "STOPPED"
  val STARTING = "STARTING"
  val STOPPING = "STOPPING"
  val RUNNING = "RUNNING"
  val UNKNOWN = "UNKNOWN"
  
  val DEFAULT_VIRTUAL_HOST_NAME = new AsciiBuffer("default")
}

class Broker() extends Service with Logging {
  
  import BrokerConstants._
  override protected def log = BrokerConstants

  // The configuration state of the broker... It can be modified directly until the broker
  // is started.
  @BeanProperty
  val connectUris: ArrayList[String] = new ArrayList[String]
  @BeanProperty
  val virtualHosts: LinkedHashMap[AsciiBuffer, VirtualHost] = new LinkedHashMap[AsciiBuffer, VirtualHost]
  @BeanProperty
  val transportServers: ArrayList[TransportServer] = new ArrayList[TransportServer]
  @BeanProperty
  var dataDirectory: File = null
  @BeanProperty
  var name = "broker";
  @BeanProperty
  var defaultVirtualHost: VirtualHost = null

  def start = runtime.start
  def stop = runtime.stop

  def addVirtualHost(host: VirtualHost) = {
    if (host.names.isEmpty) {
      throw new IllegalArgumentException("Virtual host must be configured with at least one host name.")
    }
    for (name <- host.names) {
      if (virtualHosts.containsKey(name)) {
        throw new IllegalArgumentException("Virtual host with host name " + name + " already exists.")
      }
    }
    for (name <- host.names) {
      virtualHosts.put(name, host)
    }
    if (defaultVirtualHost == null) {
      defaultVirtualHost = host
    }
  }

  // Holds the runtime state of the broker all access should be serialized
  // via a the dispatch queue and therefore all requests are setup to return
  // results via callbacks.
  object runtime {

    class BrokerAcceptListener extends TransportAcceptListener {
      def onAcceptError(error: Exception): Unit = {
        error.printStackTrace
        warn("Accept error: " + error)
        debug("Accept error details: ", error)
      }

      def onAccept(transport: Transport): Unit = {
        var connection = new BrokerConnection(Broker.this)
        connection.transport = transport
        clientConnections.add(connection)
        try {
          connection.start
        }
        catch {
          case e1: Exception => {
            onAcceptError(e1)
          }
        }
      }
    }

    var state = CONFIGURATION
    val dispatchQueue = createQueue("broker");
    val clientConnections: ArrayList[Connection] = new ArrayList[Connection]

    def removeConnectUri(uri: String): Unit = ^ {
      connectUris.remove(uri)
    } ->: dispatchQueue

    def getVirtualHost(name: AsciiBuffer, cb: (VirtualHost) => Unit) = callback(cb) {
      virtualHosts.get(name)
    } ->: dispatchQueue

    def getConnectUris(cb: (ArrayList[String]) => Unit) = callback(cb) {
      new ArrayList(connectUris)
    } ->: dispatchQueue


    def getDefaultVirtualHost(cb: (VirtualHost) => Unit) = callback(cb) {
      defaultVirtualHost
    } ->: dispatchQueue

    def addVirtualHost(host: VirtualHost) = ^ {
      Broker.this.addVirtualHost(host)
    } ->: dispatchQueue

    def getState(cb: (String) => Unit) = callback(cb) {state} ->: dispatchQueue

    def addConnectUri(uri: String) = ^ {
      connectUris.add(uri)
    } ->: dispatchQueue

    def getName(cb: (String) => Unit) = callback(cb) {
      name;
    } ->: dispatchQueue

    def getVirtualHosts(cb: (ArrayList[VirtualHost]) => Unit) = callback(cb) {
      new ArrayList[VirtualHost](virtualHosts.values)
    } ->: dispatchQueue

    def getTransportServers(cb: (ArrayList[TransportServer]) => Unit) = callback(cb) {
      new ArrayList[TransportServer](transportServers)
    } ->: dispatchQueue

    def start = ^ {
      if (state == CONFIGURATION) {
        // We can apply defaults now
        if (dataDirectory == null) {
          dataDirectory = new File(IOHelper.getDefaultDataDirectory)
        }

        if (defaultVirtualHost == null) {
          defaultVirtualHost = new VirtualHost()
          defaultVirtualHost.broker = Broker.this
          defaultVirtualHost.names = DEFAULT_VIRTUAL_HOST_NAME.toString :: Nil
          virtualHosts.put(DEFAULT_VIRTUAL_HOST_NAME, defaultVirtualHost)
        }

        state = STARTING

        for (virtualHost <- virtualHosts.values) {
          virtualHost.start
        }
        for (server <- transportServers) {
          server.setDispatchQueue(dispatchQueue)
          server.setAcceptListener(new BrokerAcceptListener)
          server.start
        }
        state = RUNNING
      } else {
        warn("Can only start a broker that is in the " + CONFIGURATION + " state.  Broker was " + state)
      }
    } ->: dispatchQueue

    def stop: Unit = ^ {
      if (state == RUNNING) {
        state = STOPPING

        for (server <- transportServers) {
          stopService(server)
        }
        for (connection <- clientConnections) {
          stopService(connection)
        }
        for (virtualHost <- virtualHosts.values) {
          stopService(virtualHost)
        }
        state = STOPPED;
      }

    } ->: dispatchQueue
  }


  /**
   * Helper method to help stop broker services and log error if they fail to start.
   * @param server
   */
  private def stopService(server: Service): Unit = {
    try {
      server.stop
    } catch {
      case e: Exception => {
        warn("Could not stop " + server + ": " + e)
        debug("Could not stop " + server + " due to: ", e)
      }
    }
  }
}


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




object Queue {
  val maxOutboundSize = 1024*1204*5
}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Queue(val destination:Destination) extends BaseRetained with Route with DeliveryConsumer with DeliveryProducer {

  override val queue:DispatchQueue = createQueue("queue:"+destination);
  queue.setTargetQueue(getRandomThreadQueue)

  val delivery_buffer  = new DeliveryBuffer
  delivery_buffer.eventHandler = ^{ drain_delivery_buffer }

  val session_manager = new DeliverySessionManager(delivery_buffer, queue)

  setDisposer(^{
    queue.release
    session_manager.release
  })

  class ConsumerState(val consumer:DeliverySession) {
    var bound=true

    def deliver(value:Delivery):Unit = {
      val delivery = Delivery(value)
      delivery.setDisposer(^{
        ^{ completed(value) } ->:queue
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
        val cs = new ConsumerState(consumer.open_session(queue))
        allConsumers += consumer->cs
        readyConsumers.addLast(cs)
      }
      drain_delivery_buffer
    } ->: queue

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
    } ->: queue

  def disconnected() = throw new RuntimeException("unsupported")

  def collocate(value:DispatchQueue):Unit = {
    if( value.getTargetQueue ne queue.getTargetQueue ) {
      println(queue.getLabel+" co-locating with: "+value.getLabel);
      this.queue.setTargetQueue(value.getTargetQueue)
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
//    } ->: queue
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