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
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.broker.store.QueueRecord
import path._
import path.PathParser.PathException
import java.util.concurrent.TimeUnit
import scala.Array
import collection.{Iterable, JavaConversions}
import security.SecuredResource.{TopicKind, QueueKind}
import security.{SecuredResource, SecurityContext}
import org.apache.activemq.apollo.dto._
import scala.collection.mutable.{HashSet, HashMap, LinkedHashMap}
import java.util.concurrent.atomic.AtomicInteger

object DestinationMetricsSupport {

  def clear_non_counters(metrics:DestMetricsDTO) = {
    metrics.queue_items = 0
    metrics.queue_size = 0
    metrics.producer_count = 0
    metrics.consumer_count = 0
    metrics.swapped_in_size_max = 0
    metrics.swapped_in_size = 0
    metrics.swapped_in_items = 0
    metrics.swapping_in_size = 0
    metrics.swapping_out_size = 0;
    metrics.swapping_out_size = 0;
  }

  def add_destination_metrics(to:DestMetricsDTO, from:DestMetricsDTO) = {
    to.enqueue_item_counter += from.enqueue_item_counter
    to.enqueue_size_counter += from.enqueue_size_counter
    to.enqueue_ts = to.enqueue_ts max from.enqueue_ts

    to.dequeue_item_counter += from.dequeue_item_counter
    to.dequeue_size_counter += from.dequeue_size_counter
    to.dequeue_ts = to.dequeue_ts max from.dequeue_ts

    to.producer_counter += from.producer_counter
    to.consumer_counter += from.consumer_counter
    to.producer_count += from.producer_count
    to.consumer_count += from.consumer_count

    to.nack_item_counter += from.nack_item_counter
    to.nack_size_counter += from.nack_size_counter
    to.nack_ts = to.nack_ts max from.nack_ts

    to.expired_item_counter += from.expired_item_counter
    to.expired_size_counter += from.expired_size_counter
    to.expired_ts = to.expired_ts max from.expired_ts

    to.queue_size += from.queue_size
    to.queue_items += from.queue_items

    to.swap_out_item_counter += from.swap_out_item_counter
    to.swap_out_size_counter += from.swap_out_size_counter
    to.swap_in_item_counter += from.swap_in_item_counter
    to.swap_in_size_counter += from.swap_in_size_counter

    to.swapping_in_size += from.swapping_in_size
    to.swapping_out_size += from.swapping_out_size

    to.swapped_in_items += from.swapped_in_items
    to.swapped_in_size += from.swapped_in_size
    to.swapped_in_size_max += from.swapped_in_size_max
  }

}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait RouterListener {
  def on_create(destination:DomainDestination, security:SecurityContext)
  def on_destroy(destination:DomainDestination, security:SecurityContext)

  def on_connect(destination:DomainDestination, producer:BindableDeliveryProducer, security:SecurityContext)
  def on_disconnect(destination:DomainDestination, producer:BindableDeliveryProducer)

  def on_bind(destination:DomainDestination, consumer:DeliveryConsumer, security:SecurityContext)
  def on_unbind(destination:DomainDestination, consumer:DeliveryConsumer, persistent:Boolean)

  def close
}

trait RouterListenerFactory {
  def create(router:Router):RouterListener
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object RouterListenerFactory {

  val finder = new ClassFinder[RouterListenerFactory]("META-INF/services/org.apache.activemq.apollo/router-listener-factory.index",classOf[RouterListenerFactory])

  def create(router:Router):List[RouterListener] = {
    finder.singletons.map(_.create(router))
  }
}

case class BrowseResult(first_seq:Long, last_seq:Long, total_entries:Long, entries:Array[(EntryStatusDTO, Delivery)])

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DomainDestination extends SecuredResource {

  def address:DestinationAddress
  def id = address.id
  def virtual_host:VirtualHost


  def browse(from_seq:Long, to:Option[Long], max:Long)(func: (BrowseResult)=>Unit):Unit


  def bind (bind_address:BindAddress, consumer:DeliveryConsumer, on_bind:()=>Unit):Unit
  def unbind (consumer:DeliveryConsumer, persistent:Boolean):Unit

  def connect (connect_address:ConnectAddress, producer:BindableDeliveryProducer):Unit
  def disconnect (producer:BindableDeliveryProducer):Unit

  def update(on_completed:Task):Unit

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object LocalRouter extends Log {

  val destination_parser = new DestinationParser

  def is_wildcard_destination(id:String) = {
    if( id == null ) {
      true
    } else {
      val path = destination_parser.decode_path(id)
      PathParser.containsWildCards(path)
    }
  }

  class ConsumerContext[D <: DomainDestination](val consumer:DeliveryConsumer) {
    val bind_addresses = HashSet[BindAddress]()
    val matched_destinations = HashSet[D]()
    var security:SecurityContext = _

    override def hashCode: Int = consumer.hashCode
    override def equals(obj: Any): Boolean = {
      obj match {
        case x:ConsumerContext[_] => x.consumer == consumer
        case _ => false
      }
    }
  }

  class ProducerContext(val connect_address:ConnectAddress, val producer:BindableDeliveryProducer, val security:SecurityContext) {
    override def hashCode: Int = producer.hashCode

    override def equals(obj: Any): Boolean = {
      obj match {
        case x:ProducerContext=> x.producer == producer
        case _ => false
      }
    }
  }
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
class LocalRouter(val virtual_host:VirtualHost) extends BaseService with Router with Dispatched {
  import LocalRouter._

  val router_listeners = RouterListenerFactory.create(this)

  def dispatch_queue:DispatchQueue = virtual_host.dispatch_queue

  def auto_create_destinations = {
    import OptionSupport._
    virtual_host.config.auto_create_destinations.getOrElse(true)
  }

  private val ALL = new Path(List(AnyDescendantPart))

  def authorizer = virtual_host.authorizer
  
  def is_temp(address:DestinationAddress) = {
    address.domain != "dsub" && address.id.startsWith("temp.")
  }
  
  def temp_owner(address:DestinationAddress) = {
    if( address.path.parts.length < 3 ) {
      None
    } else {
      try {
        val broker = address.path.parts(1).asInstanceOf[LiteralPart]
        val owner = address.path.parts(2).asInstanceOf[LiteralPart]
        Some((broker.value, owner.value))
      } catch {
        case _:Throwable => None
      }
    }
  }

  trait Domain[D <: DomainDestination] {

    // holds all the destinations in the domain by id
    var destination_by_id = LinkedHashMap[String, D]()
    // holds all the destinations in the domain by path
    var destination_by_path = new PathMap[D]()
    // Can store consumers on wild cards paths

    val consumers = HashMap[DeliveryConsumer, ConsumerContext[D]]()
    val consumers_by_path = new PathMap[(ConsumerContext[D], BindAddress)]()
    val producers_by_path = new PathMap[ProducerContext]()

    def destinations:Iterable[D] = JavaConversions.collectionAsScalaIterable(destination_by_path.get(ALL))

    def get_destination_matches(path:Path) = {
      import JavaConversions._
      collectionAsScalaIterable(destination_by_path.get( path ))
    }

    def apply_update(traker:LoggingTracker) = {
      destinations.foreach { dest=>
        dest.update(traker.task("update "+dest))
      }
    }

    def auto_create_on_connect = auto_create_destinations
    def auto_create_on_bind = auto_create_destinations

    def can_create_destination(address:DestinationAddress, security:SecurityContext):Option[String]
    def create_destination(address:DestinationAddress, security:SecurityContext):Result[D,String]


    def get_or_create_destination(address:DestinationAddress, security:SecurityContext):Result[D,String] = {
      Option(destination_by_path.chooseValue(address.path)).
      map(Success(_)).
      getOrElse(create_destination(address, security))
    }

    var add_destination = (path:Path, dest:D) => {
      destination_by_path.put(path, dest)
      destination_by_id.put(dest.id, dest)

      // binds any matching wild card subs and producers...
      import JavaConversions._
      consumers_by_path.get( path ).foreach { case (consumer_context, bind_address)=>
        if( authorizer.can(consumer_context.security, bind_action(consumer_context.consumer), dest) ) {
          consumer_context.matched_destinations += dest
          dest.bind(bind_address, consumer_context.consumer, ()=>{})
        }
      }
      producers_by_path.get( path ).foreach { x=>
        if( authorizer.can(x.security, "send", dest) ) {
          dest.connect(x.connect_address, x.producer)
        }
      }
    }

    var remove_destination = (path:Path, dest:D) => {
      destination_by_path.remove(path, dest)
      destination_by_id.remove(dest.id)
    }

    def can_destroy_destination(address:DestinationAddress, security:SecurityContext):Option[String] = {
      if( security==null ) {
        return None
      }

      for(dest <- get_destination_matches(address.path)) {
        if( is_temp(address) ) {
          val owner = temp_owner(address).get
          if( security.session_id !=null ) {
            if( (virtual_host.broker.id, security.session_id) != owner ) {
              return Some("Not authorized to destroy the temp %s '%s'. Principals=%s".format(dest.resource_kind.id, dest.id, security.principal_dump))
            }
          }
        }

        if( !authorizer.can(security, "destroy", dest) ) {
          return Some("Not authorized to destroy the %s '%s'. Principals=%s".format(dest.resource_kind.id, dest.id, security.principal_dump))
        }
      }
      None
    }
    
    def destroy_destination(address:DestinationAddress, security: SecurityContext):Unit

    def bind_action(consumer:DeliveryConsumer):String

    def can_bind_all(bind_address:BindAddress, consumer:DeliveryConsumer, security:SecurityContext):Option[String] = {
      if( security==null ) {
        return None
      }

      // Only allow the owner to bind.
      if( is_temp(bind_address) ) {
        temp_owner(bind_address) match {
          case Some(owner) =>
            if( security.session_id!=null) {
              if( (virtual_host.broker.id, security.session_id) != owner ) {
                return Some("Not authorized to receive from the temporary destination. Principals=%s".format(security.principal_dump))
              }
            }
          case None =>
            return Some("Invalid temp destination name. Owner id missing")
        }
      }

      val wildcard = PathParser.containsWildCards(bind_address.path)
      var matches = get_destination_matches(bind_address.path)

      // Should we attempt to auto create the destination?
      if( !wildcard ) {
        if ( matches.isEmpty && auto_create_on_bind ) {
          val rc = create_destination(bind_address, security)
          if( rc.failed ) {
            return Some(rc.failure)
          }
          matches = get_destination_matches(bind_address.path)
        }
        if( matches.isEmpty ) {
          return Some("The destination does not exist.")
        }

        matches.foreach { dest =>
          var action: String = bind_action(consumer)
          if (!authorizer.can(security, action, dest)) {
            return Some("Not authorized to %s from the %s '%s'. Principals=%s".format(action, dest.resource_kind.id, dest.id, security.principal_dump))
          }
        }
      }
      None
    }

    def bind(bind_address:BindAddress, consumer:DeliveryConsumer, security:SecurityContext, on_bind:()=>Unit):Unit = {

      val context = consumers.getOrElseUpdate(consumer, new ConsumerContext[D](consumer))
      context.security = security
      if( context.bind_addresses.add(bind_address) ) {

        consumers_by_path.put(bind_address.path, (context, bind_address))
        consumer.retain

        // Get the list of new destination matches..
        var matches = get_destination_matches(bind_address.path).toSet
        matches --= context.matched_destinations
        context.matched_destinations ++= matches

        val remaining = new AtomicInteger(1)
        var bind_release:()=>Unit = ()=> {
          if( remaining.decrementAndGet() == 0 ) {
            on_bind()
          }
        }

        matches.foreach { dest=>
          if( authorizer.can(security, bind_action(consumer), dest) ) {
            remaining.incrementAndGet()
            dest.bind(bind_address, consumer, bind_release)
            for( l <- router_listeners) {
              l.on_bind(dest, consumer, security)
            }
          }
        }
        bind_release()

      } else {
        on_bind()
      }

    }

    def unbind(bind_address:BindAddress, consumer:DeliveryConsumer, persistent:Boolean, security: SecurityContext) = {
      consumers.get(consumer) match {
        case None => // odd..
        case Some(context) =>
          if( context.bind_addresses.remove(bind_address) ) {

            // What did we match?
            var matches = context.matched_destinations.toSet

            // rebuild the set of what we still match..
            context.matched_destinations.clear
            context.bind_addresses.foreach { address =>
              context.matched_destinations ++= get_destination_matches(address.path)
            }

            // Take the diff to find out what we don't match anymore..
            matches --= context.matched_destinations

            matches.foreach{ dest=>
              dest.unbind(consumer, persistent)
              for( l <- router_listeners) {
                l.on_unbind(dest, consumer, persistent)
              }
            }

            consumer.release
            consumers_by_path.remove(bind_address.path, (context, bind_address))
            if(context.bind_addresses.isEmpty) {
              consumers.remove(consumer);
            }
          }
      }
    }

    def can_connect_all(address:DestinationAddress, producer:BindableDeliveryProducer, security:SecurityContext):Option[String] = {
      val wildcard = PathParser.containsWildCards(address.path)
      var matches = get_destination_matches(address.path)

      if( wildcard ) {

        // Wild card sends never fail authorization... since a destination
        // may get crated later which the user is authorized to use.
        None

      } else {

        // Should we attempt to auto create the destination?
        if ( matches.isEmpty && auto_create_on_connect ) {
          val rc = create_destination(address, security)
          if( rc.failed ) {
            return Some(rc.failure)
          }
          matches = get_destination_matches(address.path)
        }

        if( matches.isEmpty ) {
          return Some("The destination does not exist.")
        }

        // since this is not a wild card, we should have only matched one..
        assert( matches.size == 1 )
        for( dest <- matches ) {
          if( !authorizer.can(security, "send", dest) ) {
            return Some("Not authorized to send to the %s '%s'. Principals=%s".format(dest.resource_kind.id, dest.id, security.principal_dump))
          }
        }

        None
      }
    }

    def connect(connect_address:ConnectAddress, producer:BindableDeliveryProducer, security:SecurityContext):Unit = {
      get_destination_matches(connect_address.path).foreach { dest=>
        if( authorizer.can(security, "send", dest) ) {
          dest.connect(connect_address, producer)
          for( l <- router_listeners) {
            l.on_connect(dest, producer, security)
          }
        }
      }
      producers_by_path.put(connect_address.path, new ProducerContext(connect_address, producer, security))
    }

    def disconnect(connect_address:ConnectAddress, producer:BindableDeliveryProducer) = {
      producers_by_path.remove(connect_address.path, new ProducerContext(connect_address, producer, null))
      get_destination_matches(connect_address.path).foreach { dest=>
        dest.disconnect(producer)
        for( l <- router_listeners) {
          l.on_disconnect(dest, producer)
        }
      }
    }

  }


  class TopicDomain extends Domain[Topic] {

    def topic_config(name:Path):TopicDTO = {
      import collection.JavaConversions._
      import destination_parser._
      virtual_host.config.topics.find{ x=>
        x.id==null || decode_filter(x.id).matches(name)
      }.getOrElse(new TopicDTO)
    }

    def destroy_destination(address: DestinationAddress, security: SecurityContext): Unit = {
      val path:Path = address.path
      val matches = get_destination_matches(path)
      matches.foreach { dest =>
        for( l <- router_listeners) {
          l.on_destroy(dest, security)
        }

        // Disconnect the producers.
        dest.disconnect_producers

        // Disconnect the durable subs
        for( dsub <- dest.durable_subscriptions ) {
          dest.unbind(dsub, false)
        }

//        // Delete any consumer temp queues..
//        for( consumer <- dest.consumers ) {
//          consumer match {
//            case queue:Queue =>
//              _destroy_queue(queue)
//            case _ =>
//          }
//        }

        // Un-register the topic.
        remove_destination(path, dest)
      }
    }

    def can_create_destination(address:DestinationAddress, security:SecurityContext):Option[String] = {
      if (security==null) {
        return None;
      }

      // We can't create a wild card destination.. only wild card subscriptions.
      assert( !PathParser.containsWildCards(address.path) )
      // A new destination is being created...

      val resource = new SecuredResource() {
        def resource_kind = TopicKind
        def id = destination_parser.encode_path(address.path)
      }
      if( !authorizer.can(security, "create", resource)) {
        return Some("Not authorized to create the topic '%s'. Principals=%s".format(resource.id, security.principal_dump))
      } else {
        None
      }
    }

    def create_destination(address:DestinationAddress, security:SecurityContext):Result[Topic,String] = {
      // We can't create a wild card destination.. only wild card subscriptions.
      assert( !PathParser.containsWildCards(address.path) )

      // A new destination is being created...
      val dto = topic_config(address.path)

      val resource = new SecuredResource() {
        def resource_kind = TopicKind
        def id = address.id
      }
      if( !authorizer.can(security, "create", resource)) {
        return Failure("Not authorized to create the topic '%s'. Principals=%s".format(resource.id, security.principal_dump))
      }

      val topic = new Topic(LocalRouter.this, address, ()=>topic_config(address.path))
      add_destination(address.path, topic)

      for( l <- router_listeners) {
        l.on_create(topic, security)
      }
      Success(topic)
    }

    def bind_action(consumer:DeliveryConsumer):String = "receive"

  }

  class DsubDomain extends Domain[Queue] {

    override def auto_create_on_connect = false

    def bind(queue:Queue) = {
      assert_executing
      val address = queue.address.asInstanceOf[SubscriptionAddress]
      add_destination(address.path, queue)
      bind_topics(queue, address, address.topics)
    }

    def rebind(queue:Queue, binding:DurableSubscriptionQueueBinding) = {

      // Ok figure out what was added or removed.
      val prev:Set[BindAddress] = queue.address.asInstanceOf[SubscriptionAddress].topics.toSet
      val next:Set[BindAddress] = binding.address.topics.toSet
      val existing = prev.intersect(next)
      val added = next -- existing
      val removed = prev -- next

      if(!added.isEmpty) {
        bind_topics(queue, binding.address, added)
      }
      if(!removed.isEmpty) {
        unbind_topics(queue, removed)
      }

      // Make sure the update is visible in the queue's thread context..
      queue.binding = binding
      queue.dispatch_queue {
        queue.binding = binding
      }
    }

    def unbind(queue:Queue) = {
      assert_executing
      val address = queue.address.asInstanceOf[SubscriptionAddress]
      unbind_topics(queue, address.topics)
      remove_destination(address.path, queue)
    }

    def unbind_topics(queue: Queue, topics: Traversable[_ <: BindAddress]) {
      topics.foreach { topic:BindAddress =>
        topic_domain.unbind(topic, queue, false, null)
      }
    }

    def bind_topics(queue: Queue, address: SubscriptionAddress, topics: Traversable[_ <: BindAddress]) {
      topics.foreach { topic:BindAddress =>
        topic_domain.bind(topic, queue, null, ()=>{})
      }
    }

    def destroy_destination(address: DestinationAddress, security: SecurityContext): Unit = {
      destination_by_id.get(address.id).foreach { sub=>
        for( l <- router_listeners) {
          l.on_destroy(sub, security)
        }
        _destroy_queue(sub)
      }
    }

    def get_dsub_secured_resource(address: DestinationAddress):SecuredResource = {
      destination_by_id.get(address.id).getOrElse(new SecuredResource() {
        def resource_kind = SecuredResource.DurableSubKind
        def id = address.id
      })
    }

    def can_create_destination(address:DestinationAddress, security:SecurityContext):Option[String] = {
      address match {
        case address:SubscriptionAddress=>
          val resource = get_dsub_secured_resource(address)
          if( !authorizer.can(security, "create", resource)) {
            Some("Not authorized to create the dsub '%s'. Principals=%s".format(resource.id, security.principal_dump))
          } else {
            None
          }
        case _ =>
          // We can't create it.. not enough info.
          Some("Durable subscription does not exist")
      }
    }

    def create_destination(address:DestinationAddress, security:SecurityContext):Result[Queue,String] = {
      can_create_destination(address, security).map(Failure(_)).getOrElse {
        val dsub = _create_queue(DurableSubscriptionQueueBinding(address.asInstanceOf[SubscriptionAddress]))
        for( l <- router_listeners) {
          l.on_create(dsub, security)
        }
        Success(dsub)
      }
    }

    def bind_action(consumer:DeliveryConsumer):String = if(consumer.browser) {
      "receive"
    } else {
      "consume"
    }

    override def bind(bind_address: BindAddress, consumer: DeliveryConsumer, security: SecurityContext, on_bind: ()=>Unit) {
      destination_by_id.get(bind_address.id).foreach { queue =>

        // We may need to update the bindings...
        bind_address match {
          case bind_address:SubscriptionAddress=>
            if( queue.address!=bind_address && authorizer.can(security, "consume", queue)) {

              val binding = DurableSubscriptionQueueBinding(bind_address)
              if( queue.tune_persistent && queue.store_id == -1 ) {
                val record = QueueRecord(queue.store_id, binding.binding_kind, binding.binding_data)
                // Update the bindings
                virtual_host.store.add_queue(record) { rc => Unit }
              }

              // and then rebind the queue in the router.
              rebind(queue, binding)

            }
          case _ =>
        }

        val remaining = new AtomicInteger(1)
        var bind_release:()=>Unit = ()=> {
          if( remaining.decrementAndGet() == 0 ) {
            on_bind()
          }
        }

        if( authorizer.can(security, bind_action(consumer), queue) ) {
          remaining.incrementAndGet()
          queue.bind(bind_address, consumer, bind_release)
          for( l <- router_listeners) {
            l.on_bind(queue, consumer, security)
          }
        }
        bind_release();
      }
    }

    override def unbind(bind_address:BindAddress, consumer: DeliveryConsumer, persistent: Boolean, security: SecurityContext) = {
      destination_by_id.get(bind_address.id).foreach { queue =>
        queue.unbind(consumer, persistent)
        if( persistent ) {
          _destroy_queue(queue, security)
        }
        for( l <- router_listeners) {
          l.on_unbind(queue, consumer, persistent)
        }
      }
    }
  }

  class QueueDomain extends Domain[Queue] {

    def bind(queue:Queue) = {
      val path = queue.address.path
      assert( !PathParser.containsWildCards(path) )
      add_destination(path, queue)

      if( queue.mirrored ) {
        // hook up the queue to be a subscriber of the topic.
        val topic = local_topic_domain.get_or_create_destination(SimpleAddress("topic", path), null).success
        topic.bind(SimpleAddress("queue", path), queue, ()=>{})
      }
    }

    def unbind(queue:Queue) = {
      val path = queue.address.path
      remove_destination(path, queue)

      if( queue.mirrored ) {
        // unhook the queue from the topic
        val topic = local_topic_domain.get_or_create_destination(SimpleAddress("topic", path), null).success
        topic.unbind(queue, false)
      }
    }

    def destroy_destination(address: DestinationAddress, security: SecurityContext): Unit = {
      val matches = get_destination_matches(address.path)
      matches.foreach { queue =>
        for( l <- router_listeners) {
          l.on_destroy(queue, security)
        }
        _destroy_queue(queue)
      }
    }

    def can_create_destination(address:DestinationAddress, security: SecurityContext):Option[String] = {
      val resource = new SecuredResource() {
        def resource_kind = QueueKind
        def id = address.id
      }
      if( authorizer.can(security, "create", resource)) {
        None
      } else {
        Some("Not authorized to create the queue '%s'. Principals=%s".format(resource.id, security.principal_dump))
      }
    }

    def create_destination(address:DestinationAddress, security: SecurityContext) = {
      val binding = QueueDomainQueueBinding(address)
      val resource = new SecuredResource() {
        def resource_kind = QueueKind
        def id = address.id
      }
      if( authorizer.can(security, "create", resource)) {
        var queue = _create_queue(binding)
        for( l <- router_listeners) {
          l.on_create(queue, security)
        }
        Success(queue)
      } else {
        Failure("Not authorized to create the queue '%s'. Principals=%s".format(resource.id, security.principal_dump))
      }

    }
    def bind_action(consumer:DeliveryConsumer):String = if(consumer.browser) {
      "receive"
    } else {
      "consume"
    }

  }




  /////////////////////////////////////////////////////////////////////////////
  //
  // life cycle methods.
  //
  /////////////////////////////////////////////////////////////////////////////

  protected def create_configure_destinations {
    import collection.JavaConversions._
    def create_configured_dests(list: Traversable[String], d: Domain[_], to_address: (Path) => DestinationAddress) = {
      list.foreach { id =>
        if (id != null) {
          try {
            val path = destination_parser.decode_path(id)
            if (!PathParser.containsWildCards(path)) {
              d.get_or_create_destination(to_address(path), null)
            }
          } catch {
            case x:PathException => warn(x, "Invalid destination id '%s'", id)
          }
        }
      }
    }
    create_configured_dests(virtual_host.config.queues.map(_.id), local_queue_domain, (path) => SimpleAddress("queue", path))
    create_configured_dests(virtual_host.config.topics.map(_.id), local_topic_domain, (path) => SimpleAddress("topic", path))

    virtual_host.config.dsubs.foreach { dto =>
      if (dto.id != null && ( dto.topic!=null || !dto.topics.isEmpty) ) {
        val path = destination_parser.decode_path(dto.id)
        val id = DestinationAddress.encode_path(path)

        // We will create the durable sub if it does not exist yet..
        if( !PathParser.containsWildCards(path) &&
            !local_dsub_domain.destination_by_id.contains(id) ) {

          var topics = dto.topics.toList.map { n =>
            SimpleAddress("topic", destination_parser.decode_path(n))
          }
          if( dto.topic!=null ) {
            topics ::= SimpleAddress("topic", destination_parser.decode_path(dto.topic))
          }
          _create_queue(DurableSubscriptionQueueBinding(SubscriptionAddress(path, dto.selector, topics.toArray)))
        }
      }
    }
  }

  protected def _start(on_completed: Task) = {
    val tracker = new LoggingTracker("router startup", virtual_host.console_log)
    if( virtual_host.store!=null ) {
      val task = tracker.task("list_queues")
      virtual_host.store.list_queues { queue_keys =>
        for( queue_key <- queue_keys) {
          val task = tracker.task("load queue: "+queue_key)
          // Use a global queue to so we concurrently restore
          // the queues.
          globalQueue {
            virtual_host.store.get_queue(queue_key) { x =>
              x match {
                case Some(record)=>
                  if( record.binding_kind == TempQueueBinding.TEMP_KIND ) {
                    // These are temp queues create to topic subscriptions which
                    // avoid blocking producers.
                    virtual_host.store.remove_queue(queue_key){x=> task.run}
                  } else {
                    var binding = BindingFactory.create(record.binding_kind, record.binding_data)
                    if( is_temp(binding.address) ) {
                      // These are the temp queues clients create.
                      virtual_host.store.remove_queue(queue_key){x=> task.run}
                    } else {
                      dispatch_queue {
                        _create_queue(binding, queue_key)
                        task.run
                      }
                    }
                  }
                case _ => task.run
              }
            }
          }
        }
        task.run
      }
    }

    import OptionSupport._
    if(virtual_host.config.regroup_connections.getOrElse(false)) {
      schedule_connection_regroup
    }

    tracker.callback {
      dispatch_queue {
        // Now that we have restored persistent destinations,
        // make sure we create any NON-wildcard destinations
        // explicitly listed in the config.

        create_configure_destinations
        on_completed.run()
      }
    }

  }
  
  def remove_temp_destinations(active_connections:scala.collection.Set[String]) = {
    virtual_host.dispatch_queue.assertExecuting()
    val min_create_time = virtual_host.broker.now - 1000;

    // Auto delete temp destinations..
    local_queue_domain.destinations.filter(x=> is_temp(x.address)).foreach { queue=>
      val owner = temp_owner(queue.address).get
      if( owner._1==virtual_host.broker.id // are we the broker that owns the temp destination?
          && !active_connections.contains(owner._2) // Has the connection not around?
          && queue.service_state.since < min_create_time // It's not a recently created destination?
      ) {
        _destroy_queue(queue)
      }
    }
    local_topic_domain.destinations.filter(x=> is_temp(x.address)).foreach { topic =>
      val owner = temp_owner(topic.address).get
      if( owner._1==virtual_host.broker.id // are we the broker that owns the temp destination?
          && !active_connections.contains(owner._2) // Has the connection not around?
          && topic.created_at < min_create_time // It's not a recently created destination?
      ) {
        local_topic_domain.destroy_destination(topic.address, null)
      }
    }
  }

  protected def _stop(on_completed: Task) = {
    queues_by_store_id.valuesIterator.foreach { queue=>
      queue.stop(NOOP)
    }
    on_queues_destroyed_actions ::= ^{
      on_completed.run
    }
    check_on_queues_destroyed_actions
  }


  // Try to periodically re-balance connections so that consumers/producers
  // are grouped onto the same thread.
  def schedule_connection_regroup:Unit = dispatch_queue.after(1, TimeUnit.SECONDS) {
    if(service_state.is_started) {
      connection_regroup
      schedule_connection_regroup
    }
  }

  def connection_regroup = {
    // this should really be much more fancy.  It should look at the messaging
    // rates between producers and consumers, look for natural data flow partitions
    // and then try to equally divide the load over the available processing
    // threads/cores.



    // For the topics, just collocate the producers onto the first consumer's thread.
    local_topic_domain.destinations.foreach { node =>

      node.consumers.keys.headOption.foreach{ consumer =>
        node.producers.keys.foreach { r=>
          r.collocate(consumer.dispatch_queue)
        }
      }
    }


    local_queue_domain.destinations.foreach { queue=>
      queue.dispatch_queue {

        // Collocate the queue's with the first consumer
        // TODO: change this so it collocates with the fastest consumer.

        queue.all_subscriptions.headOption.map( _._1 ).foreach { consumer=>
          queue.collocate( consumer.dispatch_queue )
        }

        // Collocate all the producers with the queue..

        queue.inbound_sessions.foreach { session =>
          session.producer.collocate( queue.dispatch_queue )
        }
      }

    }
  }

  /////////////////////////////////////////////////////////////////////////////
  //
  // destination/domain management methods.
  //
  /////////////////////////////////////////////////////////////////////////////
  final val local_queue_domain = new QueueDomain
  final val local_topic_domain = new TopicDomain
  final val local_dsub_domain = new DsubDomain

  def queue_domain: Domain[_ <: DomainDestination] = local_queue_domain
  def topic_domain:Domain[_ <: DomainDestination] = local_topic_domain
  def dsub_domain:Domain[_ <: DomainDestination] = local_dsub_domain

  def bind(addresses: Array[_ <: BindAddress], consumer: DeliveryConsumer, security: SecurityContext)(cb: (Option[String])=>Unit):Unit = {
    dispatch_queue.assertExecuting()
    if(!virtual_host.service_state.is_started) {
      cb(Some("virtual host stopped."))
      return
    } else {
      try {
        val actions = addresses.map { address =>
          address.domain match {
            case "topic" =>
              val allowed = topic_domain.can_bind_all(address, consumer, security)
              def perform(on_bind:()=>Unit) = topic_domain.bind(address, consumer, security, on_bind)
              (allowed, perform _)
            case "queue" =>
              val allowed = queue_domain.can_bind_all(address, consumer, security)
              def perform(on_bind:()=>Unit) = queue_domain.bind(address, consumer, security, on_bind)
              (allowed, perform _)
            case "dsub" =>
              val allowed = dsub_domain.can_bind_all(address, consumer, security)
              def perform(on_bind:()=>Unit) = dsub_domain.bind(address, consumer, security, on_bind)
              (allowed, perform _)
            case _ => sys.error("Unknown domain: "+address.domain)
          }
        }

        val failures = actions.flatMap(_._1)
        if( !failures.isEmpty ) {
          cb(Some(failures.mkString("; ")))
          return
        } else {
          val remaining = new AtomicInteger(actions.length+1)
          var bind_release:()=>Unit = ()=> {
            if( remaining.decrementAndGet() == 0 ) {
              cb(None)
            }
          }
          actions.foreach(_._2(bind_release))
          bind_release()
          return
        }
      } catch {
        case x:PathException =>
          cb(Some(x.getMessage))
          return
      }
    }
  }

  def unbind(addresses: Array[_ <: BindAddress], consumer: DeliveryConsumer, persistent:Boolean, security: SecurityContext) = {
    dispatch_queue.assertExecuting()
    addresses.foreach { address=>
      address.domain match {
        case "topic" =>
          topic_domain.unbind(address, consumer, persistent, security)
        case "queue" =>
          queue_domain.unbind(address, consumer, persistent, security)
        case "dsub" =>
          dsub_domain.unbind(address, consumer, persistent, security)
        case _ => sys.error("Unknown domain: "+address.domain)
      }
    }
  }

  def connect(addresses: Array[_ <: ConnectAddress], producer: BindableDeliveryProducer, security: SecurityContext):Option[String] = {
    dispatch_queue.assertExecuting()
    if(!virtual_host.service_state.is_started) {
      return Some("virtual host stopped.")
    } else {
      val actions = addresses.map { address =>
        address.domain match {
          case "topic" =>
            val allowed = topic_domain.can_connect_all(address, producer, security)
            def perform() = topic_domain.connect(address, producer, security)
            (allowed, perform _)
          case "queue" =>
            val allowed = queue_domain.can_connect_all(address, producer, security)
            def perform() = queue_domain.connect(address, producer, security)
            (allowed, perform _)
          case "dsub" =>
            val allowed = dsub_domain.can_connect_all(address, producer, security)
            def perform() = dsub_domain.connect(address, producer, security)
            (allowed, perform _)
          case _ => sys.error("Unknown domain: "+address.domain)
        }
      }

      val failures = actions.flatMap(_._1)
      if( !failures.isEmpty ) {
        return Some(failures.mkString("; "))
      } else {
        actions.foreach(_._2())
        producer.connected()
        producer.retain()
        return None
      }
    }
  }

  def disconnect(addresses:Array[_ <: ConnectAddress], producer:BindableDeliveryProducer) = {
    dispatch_queue.assertExecuting()
    addresses.foreach { address=>
      address.domain match {
        case "topic" =>
          topic_domain.disconnect(address, producer)
        case "queue" =>
          queue_domain.disconnect(address, producer)
        case "dsub" =>
          dsub_domain.disconnect(address, producer)
        case _ => sys.error("Unknown domain: "+address.domain)
      }
    }
    producer.disconnected()
    producer.release()
  }

  def create(addresses:Array[_ <: DestinationAddress], security: SecurityContext):Option[String] = {
    dispatch_queue.assertExecuting()
    if(!virtual_host.service_state.is_started) {
      return Some("virtual host stopped.")
    } else {

      val actions = addresses.map { address =>
        address.domain match {
          case "topic" =>
            val allowed = topic_domain.can_create_destination(address, security)
            def perform() = topic_domain.create_destination(address, security)
            (allowed, perform _)
          case "queue" =>
            val allowed = queue_domain.can_create_destination(address, security)
            def perform() = queue_domain.create_destination(address, security)
            (allowed, perform _)
          case "dsub" =>
            val allowed = dsub_domain.can_create_destination(address, security)
            def perform() = dsub_domain.create_destination(address, security)
            (allowed, perform _)
          case _ => sys.error("Unknown domain: "+address.domain)
        }
      }

      val failures = actions.flatMap(_._1)
      if( !failures.isEmpty ) {
        return Some(failures.mkString("; "))
      } else {
        actions.foreach(_._2())
        return None
      }
    }
  }

  def delete(addresses:Array[_ <: DestinationAddress], security: SecurityContext):Option[String] = {
    dispatch_queue.assertExecuting()
    if(!virtual_host.service_state.is_started) {
      return Some("virtual host stopped.")
    } else {

      val actions = addresses.map { address =>
        address.domain match {
          case "topic" =>
            val allowed = topic_domain.can_destroy_destination(address, security)
            def perform() = topic_domain.destroy_destination(address, security)
            (allowed, perform _)
          case "queue" =>
            val allowed = queue_domain.can_destroy_destination(address, security)
            def perform() = queue_domain.destroy_destination(address, security)
            (allowed, perform _)
          case "dsub" =>
            val allowed = dsub_domain.can_destroy_destination(address, security)
            def perform() = dsub_domain.destroy_destination(address, security)
            (allowed, perform _)
          case _ => sys.error("Unknown domain: "+address.domain)
        }
      }

      val failures = actions.flatMap(_._1)
      if( !failures.isEmpty ) {
        return Some(failures.mkString("; "))
      } else {
        actions.foreach(_._2())
        return None
      }
    }
  }

  /**
   * Returns the previously created queue if it already existed.
   */
  def get_or_create_destination(address: DestinationAddress, security:SecurityContext): Result[DomainDestination, String] = {
    dispatch_queue.assertExecuting()
    address.domain match {
      case "queue" => queue_domain.get_or_create_destination(address, security)
      case "topic" => topic_domain.get_or_create_destination(address, security)
      case "dsub"  => dsub_domain.get_or_create_destination(address, security)
      case _       => sys.error("Unknown domain: "+address.domain)
    }
  }


  /////////////////////////////////////////////////////////////////////////////
  //
  // Queue management methods.  Queues are multi-purpose and get used by both
  // the queue domain and topic domain.
  //
  /////////////////////////////////////////////////////////////////////////////

//  var queues_by_binding = LinkedHashMap[Binding, Queue]()
  var queues_by_store_id = LinkedHashMap[Long, Queue]()

//  /**
//   * Gets an existing queue.
//   */
//  def get_queue(dto:DestinationDTO) = dispatch_queue ! {
//    queues_by_binding.get(BindingFactory.create(dto))
//  }

  /**
   * Gets an existing queue.
   */
  def get_queue(id:Long) = {
    dispatch_queue.assertExecuting()
    queues_by_store_id.get(id)
  }


  def _create_queue(binding:Binding, id:Long= -1):Queue = {

    var qid = id
    if( qid == -1 ) {
      qid = virtual_host.queue_id_counter.incrementAndGet
    }

    val config = binding.config(virtual_host)

    val queue = new Queue(this, qid, binding).configure(config)
    if( queue.tune_persistent && id == -1) {
      val record = QueueRecord(queue.store_id, binding.binding_kind, binding.binding_data)
      virtual_host.store.add_queue(record) { rc => Unit }
    }

    queue.start(NOOP)
//    queues_by_binding.put(binding, queue)
    queues_by_store_id.put(qid, queue)

    // this causes the queue to get registered in the right location in
    // the router.
    binding.bind(this, queue)
    queue
  }

  /**
   * Returns true if the queue no longer exists.
   */
  def destroy_queue(id:Long, security:SecurityContext)(cb: (Option[String])=>Unit) = dispatch_queue {
    cb(_destroy_queue(id,security))
  }

  def _destroy_queue(id:Long, security:SecurityContext):Option[String] = {
    queues_by_store_id.get(id) match {
      case Some(queue) =>
        _destroy_queue(queue,security)
      case None =>
        Some("Does not exist")
    }
  }

//  /**
//   * Returns true if the queue no longer exists.
//   */
//  def destroy_queue(dto:DestinationDTO, security:SecurityContext) = dispatch_queue ! { _destroy_queue(dto, security) }
//
//  def _destroy_queue(dto:DestinationDTO, security:SecurityContext):Option[String] = {
//    queues_by_binding.get(BindingFactory.create(dto)) match {
//      case Some(queue) =>
//        _destroy_queue(queue, security)
//      case None =>
//        Some("Does not exist")
//    }
//  }

  def _destroy_queue(queue:Queue, security:SecurityContext):Option[String] = {
    if( !authorizer.can(security, "destroy", queue) ) {
      return Some("Not authorized to destroy queue '%s'. Principals=%s".format(queue.id, security.principal_dump))
    }
    _destroy_queue(queue)
    None
  }

  var pending_queue_destroys = 0
  var on_queues_destroyed_actions = List[Runnable]()

  def on_queue_destroy_start = {
    dispatch_queue.assertExecuting()
    pending_queue_destroys += 1
  }

  def on_queue_destroy_end = {
    dispatch_queue.assertExecuting()
    assert(pending_queue_destroys > 0)
    pending_queue_destroys -= 1
    check_on_queues_destroyed_actions
  }

  def check_on_queues_destroyed_actions = {
    if( pending_queue_destroys==0 && !on_queues_destroyed_actions.isEmpty) {
      val actions = on_queues_destroyed_actions
      on_queues_destroyed_actions = Nil
      for( action <- actions ) {
        action.run()
      }
    }
  }

  def _destroy_queue(queue: Queue) {
    assert(service_state.is_starting_or_started, "Can't destroy.. already stopped")
    on_queue_destroy_start
    queue.stop(^{
      var metrics = queue.get_queue_metrics
      dispatch_queue {

        queue.binding.unbind(this, queue)

        for ( aggreator <- queue.binding match {
          case d:DurableSubscriptionQueueBinding => Some(virtual_host.dead_dsub_metrics)
          case t:TempQueueBinding => None
          case _ => Some(virtual_host.dead_queue_metrics)
        }) {

          // Zero out all the NON counters since a removed queue is empty.
          DestinationMetricsSupport.clear_non_counters(metrics)
          DestinationMetricsSupport.add_destination_metrics(aggreator, metrics)
        }

        queues_by_store_id.remove(queue.store_id)
        if (queue.tune_persistent) {
          virtual_host.store.remove_queue(queue.store_id) { x =>
            dispatch_queue {
              debug("destroyed queue: " + queue.id)
              on_queue_destroy_end
            }
          }
        } else {
          debug("destroyed queue: " + queue.id)
          on_queue_destroy_end
        }
      }
    })
  }

  def apply_update(on_completed:Task) = {
    val tracker = new LoggingTracker("domain update", virtual_host.broker.console_log)
    local_topic_domain.apply_update(tracker)
    local_queue_domain.apply_update(tracker)
    local_dsub_domain.apply_update(tracker)
    // we may need to create some more destinations.
    create_configure_destinations
    tracker.callback(on_completed)
  }
}