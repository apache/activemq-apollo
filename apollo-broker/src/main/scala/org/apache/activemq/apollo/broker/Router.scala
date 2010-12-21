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
import org.fusesource.hawtdispatch._
import _root_.org.fusesource.hawtdispatch.ScalaDispatchHelpers._

import collection.JavaConversions
import org.apache.activemq.apollo.util._
import collection.mutable.{ListBuffer, HashMap}
import scala.collection.immutable.List
import org.apache.activemq.apollo.store.{StoreUOW, QueueRecord}
import Buffer._
import org.apache.activemq.apollo.util.path.{Path, Part, PathMap, PathParser}
import java.util.ArrayList
import org.apache.activemq.apollo.dto._
import security.SecurityContext

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Router extends Log {
  val TOPIC_DOMAIN = ascii("topic");
  val QUEUE_DOMAIN = ascii("queue");
  val TEMP_TOPIC_DOMAIN = ascii("temp-topic");
  val TEMP_QUEUE_DOMAIN = ascii("temp-queue");

  val QUEUE_KIND = ascii("queue");
  val DEFAULT_QUEUE_PATH = ascii("default");
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

  var queue_bindings = HashMap[Binding, Queue]()
  var queues = HashMap[Long, Queue]()

  // Only stores simple paths, used for wild card lookups.
  var destinations = new PathMap[RoutingNode]()
  // Can store consumers on wild cards paths
  val broadcast_consumers = new PathMap[DeliveryConsumer]()
  // Can store bindings on wild cards paths
  val bindings = new PathMap[Queue]()

  private def is_topic(destination:Destination) = {
    destination.domain match {
      case TOPIC_DOMAIN => true
      case TEMP_TOPIC_DOMAIN => true
      case _ => false
    }
  }

  private val ALL = new Path({
    val rc = new ArrayList[Part](1)
    rc.add(Part.ANY_DESCENDANT)
    rc
  })

  def routing_nodes:Iterable[RoutingNode] = JavaConversions.asScalaIterable(destinations.get(ALL))
  
  def _get_or_create_destination(path:Path, security:SecurityContext) = {
    // We can't create a wild card destination.. only wild card subscriptions.
    assert( !PathParser.containsWildCards(path) )
    var rc = destinations.chooseValue( path )
    if( rc == null ) {
      _create_destination(path, security)
    } else {
      Success(rc)
    }
  }

  def _get_destination(path:Path) = {
    Option(destinations.chooseValue( path ))
  }

  def _create_destination(path:Path, security:SecurityContext):Result[RoutingNode,String] = {

    // We can't create a wild card destination.. only wild card subscriptions.
    assert( !PathParser.containsWildCards(path) )

    // A new destination is being created...
    val config = host.destination_config(path).getOrElse(new DestinationDTO)

    if(  host.authorizer!=null && security!=null && !host.authorizer.can_create(security, host, config)) {
      return new Failure("Not authorized to create the destination")
    }

    val rc = new RoutingNode(this, path, config)
    destinations.put(path, rc)

    // bind any matching wild card subs
    import JavaConversions._
    broadcast_consumers.get( path ).foreach { c=>
      rc.add_broadcast_consumer(c)
    }
    bindings.get( path ).foreach { queue=>
      rc.add_queue(queue)
    }
    Success(rc)
  }

  def get_destination_matches(path:Path) = {
    import JavaConversions._
    asScalaIterable(destinations.get( path ))
  }

  def _create_queue(id:Long, binding:Binding, security:SecurityContext):Result[Queue,String] = {

    val config = host.queue_config(binding).getOrElse(new QueueDTO)
    if( host.authorizer!=null && security!=null && !host.authorizer.can_create(security, host, config) ) {
      return Failure("Not authorized to create the queue")
    }

    var qid = id
    if( qid == -1 ) {
      qid = host.queue_id_counter.incrementAndGet
    }

    val queue = new Queue(host, qid, binding, config)
    if( queue.tune_persistent && id == -1 ) {

      val record = new QueueRecord
      record.key = qid
      record.binding_data = binding.binding_data
      record.binding_kind = binding.binding_kind

      host.store.addQueue(record) { rc => Unit }

    }
    queue.start
    queue_bindings.put(binding, queue)
    queues.put(queue.id, queue)

    // Not all queues are bound to destinations.
    val name = binding.destination
    if( name!=null ) {
      bindings.put(name, queue)
      // make sure the destination is created if this is not a wild card sub
      if( !PathParser.containsWildCards(name) ) {
        _get_destination(name) match {
          case Some(node)=>
            node.add_queue(queue)
          case None=>
            _create_destination(name, null)
        }
      } else {
        get_destination_matches(name).foreach( node=>
          node.add_queue(queue)
        )
      }

    }
    Success(queue)

  }

  def create_queue(record:QueueRecord, security:SecurityContext) = {
    _create_queue(record.key, BindingFactory.create(record.binding_kind, record.binding_data), security)
  }

  /**
   * Returns the previously created queue if it already existed.
   */
  def _get_or_create_queue(dto: BindingDTO, security:SecurityContext): Result[Queue, String] = {
    val binding = BindingFactory.create(dto)
    val queue = queue_bindings.get(binding) match {
      case Some(queue) => Success(queue)
      case None => _create_queue(-1, binding, security)
    }
    queue
  }

  def get_or_create_queue(id:BindingDTO, security:SecurityContext) = dispatchQueue ! {
    _get_or_create_queue(id, security)
  }

  /**
   * Returns true if the queue no longer exists.
   */
  def destroy_queue(dto:BindingDTO, security:SecurityContext) = dispatchQueue ! { _destroy_queue(dto, security) }

  def _destroy_queue(dto:BindingDTO, security:SecurityContext):Result[Zilch, String] = {
    queue_bindings.get(BindingFactory.create(dto)) match {
      case Some(queue) =>
        _destroy_queue(queue, security)
      case None =>
        Failure("Does not exist")
    }
  }

  /**
   * Returns true if the queue no longer exists.
   */
  def destroy_queue(id:Long, security:SecurityContext) = dispatchQueue ! { _destroy_queue(id,security) }

  def _destroy_queue(id:Long, security:SecurityContext):Result[Zilch, String] = {
    queues.get(id) match {
      case Some(queue) =>
        _destroy_queue(queue,security)
      case None =>
        Failure("Does not exist")
    }
  }

  def _destroy_queue(queue:Queue, security:SecurityContext):Result[Zilch, String] = {

    if( security!=null && queue.config.acl!=null ) {
      if( !host.authorizer.can_destroy(security, host, queue.config) ) {
        return Failure("Not authorized to destroy")
      }
    }

    queue_bindings.remove(queue.binding)
    queues.remove(queue.id)

    val name = queue.binding.destination
    if( name!=null ) {
      get_destination_matches(name).foreach( node=>
        node.remove_queue(queue)
      )
    }
    queue.stop
    if( queue.tune_persistent ) {
      queue.dispatchQueue ^ {
        host.store.removeQueue(queue.id){x=> Unit}
      }
    }
    Success(Zilch)
  }

  /**
   * Gets an existing queue.
   */
  def get_queue(dto:BindingDTO) = dispatchQueue ! {
    queue_bindings.get(BindingFactory.create(dto))
  }

  /**
   * Gets an existing queue.
   */
  def get_queue(id:Long) = dispatchQueue ! {
    queues.get(id)
  }

  def bind(destination:Destination, consumer:DeliveryConsumer, security:SecurityContext) = {
    consumer.retain
    dispatchQueue ! {

      def do_bind:Result[Zilch, String] = {
        assert( is_topic(destination) )
        val name = destination.name

        // A new destination is being created...
        def config = host.destination_config(name).getOrElse(new DestinationDTO)

        if( host.authorizer!=null && security!=null && !host.authorizer.can_receive_from(security, host, config) ) {
          return new Failure("Not authorized to receive from the destination")
        }

        // make sure the destination is created if this is not a wild card sub
        if( !PathParser.containsWildCards(name) ) {
          val rc = _get_or_create_destination(name, security)
          if( rc.failed ) {
            return rc.map_success(_=> Zilch);
          }
        }

        get_destination_matches(name).foreach{ node=>
          node.add_broadcast_consumer(consumer)
        }
        broadcast_consumers.put(name, consumer)
        Success(Zilch)
      }

      do_bind

    }
  }

  def unbind(destination:Destination, consumer:DeliveryConsumer) = releasing(consumer) {
    assert( is_topic(destination) )
    val name = destination.name
    broadcast_consumers.remove(name, consumer)
    get_destination_matches(name).foreach{ node=>
      node.remove_broadcast_consumer(consumer)
    }
  } >>: dispatchQueue


  def connect(destination:Destination, producer:DeliveryProducer, security:SecurityContext)(completed: (Result[DeliveryProducerRoute,String])=>Unit) = {

    val route = new DeliveryProducerRoute(this, destination, producer) {
      override def on_connected = {
        completed(Success(this));
      }
    }

    def do_connect:Result[Zilch, String] = {
      val topic = is_topic(destination)


      var destination_security = security
      // Looking up the queue will cause it to get created if it does not exist.
      val queue = if( topic ) {

        def config = host.destination_config(destination.name).getOrElse(new DestinationDTO)
        if( host.authorizer!=null && security!=null && !host.authorizer.can_send_to(security, host, config)) {
          return new Failure("Not authorized to send to the destination")
        }
        None

      } else {

        val dto = new QueueBindingDTO
        dto.name = DestinationParser.encode_path(destination.name)

        // Can we send to the queue?
        def config = host.queue_config(dto).getOrElse(new QueueDTO)
        if( host.authorizer!=null && security!=null && !host.authorizer.can_send_to(security, host, config) ) {
          return Failure("Not authorized to send to the queue")
        }

        destination_security = null
        val rc = _get_or_create_queue(dto, security)
        if( rc.failed ) {
          return rc.map_success(_=>Zilch)
        }
        Some(rc.success)
      }

      _get_or_create_destination(destination.name, security) match {
        case Success(node)=>
          if( node.unified || topic ) {
            node.add_broadcast_producer( route )
          } else {
            route.bind( queue.toList )
          }
          route.connected()
          Success(Zilch)

        case Failure(reason)=>
          Failure(reason)
      }
    }

    dispatchQueue {
      do_connect.failure_option.foreach(x=> producer.dispatchQueue { completed(Failure(x)) } )
    }

  }

  def disconnect(route:DeliveryProducerRoute) = releasing(route) {
    _get_destination(route.destination.name).foreach { node=>
      val topic = is_topic(route.destination)
      if( node.unified || topic ) {
        node.remove_broadcast_producer(route)
      }
    }
    route.disconnected()

  } >>: dispatchQueue

}

/**
 * Tracks state associated with a destination name.
 */
class RoutingNode(val router:Router, val name:Path, val config:DestinationDTO) {

  val id = router.destination_id_counter.incrementAndGet

  var broadcast_producers = ListBuffer[DeliveryProducerRoute]()
  var broadcast_consumers = ListBuffer[DeliveryConsumer]()
  var queues = ListBuffer[Queue]()

  import OptionSupport._

  def unified = config.unified.getOrElse(false)
  def slow_consumer_policy = config.slow_consumer_policy.getOrElse("block")

  var consumer_proxies = Map[DeliveryConsumer, DeliveryConsumer]()

  def add_broadcast_consumer (consumer:DeliveryConsumer) = {

    var target = consumer
    slow_consumer_policy match {
      case "queue" =>

        // create a temp queue so that it can spool
        val queue = router._create_queue(-1, new TempBinding(consumer), null).success
        queue.dispatchQueue.setTargetQueue(consumer.dispatchQueue)
        queue.bind(List(consumer))

        consumer_proxies += consumer->queue
        target = queue

      case "block" =>
        // just have dispatcher dispatch directly to them..
    }

    broadcast_consumers += target
    val list = target :: Nil
    broadcast_producers.foreach({ r=>
      r.bind(list)
    })
  }

  def remove_broadcast_consumer (consumer:DeliveryConsumer) = {

    var target = consumer_proxies.get(consumer).getOrElse(consumer)

    broadcast_consumers = broadcast_consumers.filterNot( _ == target )

    val list = target :: Nil
    broadcast_producers.foreach({ r=>
      r.unbind(list)
    })

    target match {
      case queue:Queue=>
        val binding = new TempBinding(consumer)
        if( queue.binding == binding ) {
          queue.unbind(List(consumer))
          router._destroy_queue(queue.id, null)
        }
      case _ =>
    }
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

  def connected() = dispatchQueue {
    on_connected
  }

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
        if( !overflowSessions.isEmpty ) {
          overflowSessions = overflowSessions.filterNot( _ == x )
          if( overflowSessions.isEmpty ) {
            drainer.run
          }
        }
        x.close
      }
      rc
    }
  } >>: dispatchQueue

  def disconnected() = dispatchQueue {
    this.targets.foreach { x=>
      debug("producer route detaching from conusmer.")
      x.close
      x.consumer.release
    }    
  }

  protected def on_connected = {}
  protected def on_disconnected = {}

  //
  // Sink trait implementation.  This Sink overflows
  // by 1 value.  It's only full when overflowed.  It overflows
  // when one of the down stream sinks cannot accept the offered
  // Dispatch.
  //

  var pendingAck: (Boolean, StoreUOW)=>Unit = null
  var overflow:Delivery=null
  var overflowSessions = List[DeliverySession]()
  var refiller:Runnable=null

  def full = overflow!=null

  def offer(delivery:Delivery) = {
    if( full ) {
      false
    } else {

      // Do we need to store the message if we have a matching consumer?
      pendingAck = delivery.ack
      val copy = delivery.copy
      copy.message.retain

      targets.foreach { target=>

        // only deliver to matching consumers
        if( target.consumer.matches(copy) ) {

          if( copy.storeKey == -1L && target.consumer.is_persistent && copy.message.persistent ) {
            if( copy.uow==null ) {
              copy.uow = router.host.store.createStoreUOW
            } else {
              copy.uow.retain
            }
            copy.storeKey = copy.uow.store(copy.createMessageRecord)
          }

          if( !target.offer(copy) ) {
            overflowSessions ::= target
          }
        }
      }

      if( overflowSessions!=Nil ) {
        overflow = copy
      } else {
        delivered(copy)
      }
      true
    }
  }

  private def delivered(delivery: Delivery): Unit = {
    if (pendingAck != null) {
      if (delivery.uow != null) {
        val ack = pendingAck
        delivery.uow.setDisposer(^ {
          ack(true, null)
        })

      } else {
        pendingAck(true, null)
      }
      pendingAck==null
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
