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

import _root_.java.util.concurrent.atomic.AtomicLong
import _root_.org.fusesource.hawtbuf._
import _root_.org.fusesource.hawtdispatch._
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._
import _root_.org.fusesource.hawtdispatch.ScalaDispatchHelpers._

import collection.JavaConversions
import org.apache.activemq.apollo.util._
import collection.mutable.{ListBuffer, HashMap}
import org.apache.activemq.apollo.store.QueueRecord
import org.apache.activemq.apollo.dto.{PointToPointBindingDTO, BindingDTO}
import path.{PathFilter, PathMap}
import scala.collection.immutable.List

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Router extends Log {
  val TOPIC_DOMAIN = new AsciiBuffer("topic");
  val QUEUE_DOMAIN = new AsciiBuffer("queue");
  val TEMP_TOPIC_DOMAIN = new AsciiBuffer("temp-topic");
  val TEMP_QUEUE_DOMAIN = new AsciiBuffer("temp-queue");

  val QUEUE_KIND = new AsciiBuffer("queue");
  val DEFAULT_QUEUE_PATH = new AsciiBuffer("default");
}

/**
 * Provides a non-blocking concurrent producer to consumer
 * routing implementation.
 *
 * DeliveryProducers create a route object for each destination
 * they will be producing to.  Once the route is
 * connected to the router, the producer can use
 * the route.targets list without synchronization to
 * get the current set of consumers that are bound
 * to the destination. 
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Router(val host:VirtualHost) extends DispatchLogging {

  override protected def log = Router

  import Router._

  val destination_id_counter = new LongCounter

  protected def dispatchQueue:DispatchQueue = host.dispatchQueue

  var queues = HashMap[Binding, Queue]()

  // Only stores simple paths, used for wild card lookups.
  var destinations = new PathMap[RoutingNode]()
  // Can store consumers on wild cards paths
  val broadcast_consumers = new PathMap[DeliveryConsumer]()
  // Can store bindings on wild cards paths
  val bindings = new PathMap[Queue]()

  private def is_topic(destination:Destination) = {
    destination.getDomain match {
      case TOPIC_DOMAIN => true
      case TEMP_TOPIC_DOMAIN => true
      case _ => false
    }
  }

  def routing_nodes:Iterable[RoutingNode] = JavaConversions.asIterable(destinations.get(PathFilter.ANY_DESCENDENT))
  
  def create_destination_or(destination:AsciiBuffer)(func:(RoutingNode)=>Unit):RoutingNode = {

    // We can't create a wild card destination.. only wild card subscriptions.
    assert( !PathFilter.containsWildCards(destination) )

    var rc = destinations.chooseValue( destination )
    if( rc == null ) {

      // A new destination is being created...
      rc = new RoutingNode(this, destination )
      destinations.put(destination, rc)

      // bind any matching wild card subs
      import JavaConversions._
      broadcast_consumers.get( destination ).foreach { c=>
        rc.add_broadcast_consumer(c)
      }
      bindings.get( destination ).foreach { queue=>
        rc.add_queue(queue)
      }

    } else {
      func(rc)
    }
    rc
  }

  def get_destination_matches(destination:AsciiBuffer) = {
    import JavaConversions._
    asIterable(destinations.get( destination ))
  }

  def _create_queue(id:Long, binding:Binding):Queue = {
    val queue = new Queue(host, id, binding)
    queue.start
    queues.put(binding, queue)

    // Not all queues are bound to destinations.
    val name = binding.destination
    if( name!=null ) {
      bindings.put(name, queue)
      // make sure the destination is created if this is not a wild card sub
      if( !PathFilter.containsWildCards(name) ) {
        create_destination_or(name) { node=>
          node.add_queue(queue)
        }
      } else {
        get_destination_matches(name).foreach( node=>
          node.add_queue(queue)
        )
      }

    }
    queue
  }

  def create_queue(record:QueueRecord) = {
    _create_queue(record.key, BindingFactory.create(record.binding_kind, record.binding_data))
  }

  /**
   * Returns the previously created queue if it already existed.
   */
  def _create_queue(dto: BindingDTO): Some[Queue] = {
    val binding = BindingFactory.create(dto)
    val queue = queues.get(binding) match {
      case Some(queue) => Some(queue)
      case None => Some(_create_queue(-1, binding))
    }
    queue
  }

  def create_queue(dto:BindingDTO)(cb: (Option[Queue])=>Unit) = ^{
    cb(_create_queue(dto))
  } >>: dispatchQueue

  /**
   * Returns true if the queue no longer exists.
   */
  def destroy_queue(dto:BindingDTO)(cb: (Boolean)=>Unit) = ^{
    val binding = BindingFactory.create(dto)
    val queue = queues.get(binding) match {
      case Some(queue) =>
        val name = binding.destination
        if( name!=null ) {
          get_destination_matches(name).foreach( node=>
            node.remove_queue(queue)
          )
        }
        queue.stop
        true
      case None =>
        true
    }
    cb(queue)
  } >>: dispatchQueue

  /**
   * Gets an existing queue.
   */
  def get_queue(dto:BindingDTO)(cb: (Option[Queue])=>Unit) = ^{
    val binding = BindingFactory.create(dto)
    cb(queues.get(binding))
  } >>: dispatchQueue

  def bind(destination:Destination, consumer:DeliveryConsumer, on_complete:Runnable = ^{} ) = retaining(consumer) {

    assert( is_topic(destination) )

    val name = destination.getName

    // make sure the destination is created if this is not a wild card sub
    if( !PathFilter.containsWildCards(name) ) {
      val node = create_destination_or(name) { node=> }
    }

    get_destination_matches(name).foreach( node=>
      node.add_broadcast_consumer(consumer)
    )
    broadcast_consumers.put(name, consumer)

    on_complete.run
    
  } >>: dispatchQueue

  def unbind(destination:Destination, consumer:DeliveryConsumer) = releasing(consumer) {
    assert( is_topic(destination) )
    val name = destination.getName
    broadcast_consumers.remove(name, consumer)
    get_destination_matches(name).foreach{ node=>
      node.remove_broadcast_consumer(consumer)
    }
  } >>: dispatchQueue


  def connect(destination:Destination, producer:DeliveryProducer)(completed: (DeliveryProducerRoute)=>Unit) = {

    val route = new DeliveryProducerRoute(this, destination, producer) {
      override def on_connected = {
        completed(this);
      }
    }

    dispatchQueue {

      val topic = is_topic(destination)

      // Looking up the queue will cause it to get created if it does not exist.
      val queue = if( !topic ) {
        val dto = new PointToPointBindingDTO
        dto.destination = destination.getName.toString
        _create_queue(dto)
      } else {
        None
      }

      val node = create_destination_or(destination.getName) { node=> }
      if( node.unified || topic ) {
        node.add_broadcast_producer( route )
      } else {
        route.bind( queue.toList )
      }

      route.connected()
    }
  }

  def disconnect(route:DeliveryProducerRoute) = releasing(route) {

    val topic = is_topic(route.destination)
    val node = create_destination_or(route.destination.getName) { node=> }
    if( node.unified || topic ) {
      node.remove_broadcast_producer(route)
    }
    route.disconnected()

  } >>: dispatchQueue

}


/**
 * Tracks state associated with a destination name.
 */
class RoutingNode(val router:Router, val name:AsciiBuffer) {

  val id = router.destination_id_counter.incrementAndGet

  var broadcast_producers = ListBuffer[DeliveryProducerRoute]()
  var broadcast_consumers = ListBuffer[DeliveryConsumer]()
  var queues = ListBuffer[Queue]()

  // TODO: extract the node's config from the host config object
  def unified = false

  def add_broadcast_consumer (consumer:DeliveryConsumer) = {
    broadcast_consumers += consumer

    val list = consumer :: Nil
    broadcast_producers.foreach({ r=>
      r.bind(list)
    })
  }

  def remove_broadcast_consumer (consumer:DeliveryConsumer) = {
    broadcast_consumers = broadcast_consumers.filterNot( _ == consumer )

    val list = consumer :: Nil
    broadcast_producers.foreach({ r=>
      r.unbind(list)
    })
  }

  def add_broadcast_producer (producer:DeliveryProducerRoute) = {
    broadcast_producers += producer
    producer.bind(broadcast_consumers.toList)
  }

  def remove_broadcast_producer (producer:DeliveryProducerRoute) = {
    broadcast_producers = broadcast_producers.filterNot( _ == producer )
    producer.unbind(broadcast_consumers.toList)
  }

  def add_queue (queue:Queue) = {
    queue.binding.bind(this, queue)
    queues += queue
  }

  def remove_queue (queue:Queue) = {
    queues = queues.filterNot( _ == queue )
    queue.binding.unbind(this, queue)
  }

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Route extends Retained {

  def dispatchQueue:DispatchQueue
  val metric = new AtomicLong();

  def bind(targets:List[DeliveryConsumer]):Unit
  def unbind(targets:List[DeliveryConsumer]):Unit
  
  def connected():Unit
  def disconnected():Unit

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class DeliveryProducerRoute(val router:Router, val destination:Destination, val producer:DeliveryProducer) extends BaseRetained with Route with Sink[Delivery] with DispatchLogging {

  override protected def log = Router
  override def dispatchQueue = producer.dispatchQueue

  // Retain the queue while we are retained.
  dispatchQueue.retain
  setDisposer(^{
    dispatchQueue.release
  })

  var targets = List[DeliverySession]()

  def connected() = ^{
    on_connected
  } >>: dispatchQueue

  def bind(targets:List[DeliveryConsumer]) = retaining(targets) {
    internal_bind(targets)
  } >>: dispatchQueue

  private def internal_bind(values:List[DeliveryConsumer]) = {
    values.foreach{ x=>
      debug("producer route attaching to conusmer.")
      val target = x.connect(producer);
      target.refiller = drainer
      targets ::= target
    }
  }

  def unbind(targets:List[DeliveryConsumer]) = releasing(targets) {
    this.targets = this.targets.filterNot { x=>
      val rc = targets.contains(x.consumer)
      if( rc ) {
        debug("producer route detaching from conusmer.")
        x.close
      }
      rc
    }
  } >>: dispatchQueue

  def disconnected() = ^ {
    this.targets.foreach { x=>
      debug("producer route detaching from conusmer.")
      x.close
      x.consumer.release
    }    
  } >>: dispatchQueue

  protected def on_connected = {}
  protected def on_disconnected = {}

  //
  // Sink trait implementation.  This Sink overflows
  // by 1 value.  It's only full when overflowed.  It overflows
  // when one of the down stream sinks cannot accept the offered
  // Dispatch.
  //

  var overflow:Delivery=null
  var overflowSessions = List[DeliverySession]()
  var refiller:Runnable=null

  def full = overflow!=null

  def offer(delivery:Delivery) = {
    if( full ) {
      false
    } else {

      // Do we need to store the message if we have a matching consumer?
      var storeOnMatch = delivery.message.persistent && router.host.store!=null
      delivery.message.retain

      targets.foreach { target=>

        // only deliver to matching consumers
        if( target.consumer.matches(delivery) ) {

          if( storeOnMatch ) {
            if( delivery.uow==null ) {
              delivery.uow = router.host.store.createStoreUOW
            } else {
              delivery.uow.retain
            }
            delivery.storeKey = delivery.uow.store(delivery.createMessageRecord)
            storeOnMatch = false
          }


          if( !target.offer(delivery) ) {
            overflowSessions ::= target
          }
        }
      }

      if( overflowSessions!=Nil ) {
        overflow = delivery
      } else {
        delivered(delivery)
      }
      true
    }
  }

  private def delivered(delivery: Delivery): Unit = {
    if (delivery.ack != null) {
      if (delivery.uow != null) {
        delivery.uow.setDisposer(^ {delivery.ack(null)})
      } else {
        delivery.ack(null)
      }
    }
    if (delivery.uow != null) {
      delivery.uow.release
    }
    delivery.message.release
  }

  val drainer = ^{
    if( overflow!=null ) {
      val original = overflowSessions;
      overflowSessions = Nil
      original.foreach { target=>
        if( !target.offer(overflow) ) {
          overflowSessions ::= target
        }
      }
      if( overflowSessions==Nil ) {
        delivered(overflow)
        overflow = null
        refiller.run
      }
    }
  }


}
