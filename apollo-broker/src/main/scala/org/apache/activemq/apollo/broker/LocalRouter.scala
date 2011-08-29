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
import org.apache.activemq.apollo.dto._
import java.util.{Arrays, ArrayList}
import collection.mutable.{LinkedHashMap, HashMap}
import collection.{Iterable, JavaConversions}
import security.SecuredResource.{TopicKind, QueueKind}
import security.{SecuredResource, SecurityContext}

object DestinationMetricsSupport {

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

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DomainDestination extends SecuredResource {

  def id:String
  def virtual_host:VirtualHost

  def destination_dto:DestinationDTO

  def bind (destination:DestinationDTO, consumer:DeliveryConsumer)
  def unbind (consumer:DeliveryConsumer, persistent:Boolean)

  def connect (destination:DestinationDTO, producer:BindableDeliveryProducer)
  def disconnect (producer:BindableDeliveryProducer)

  def update(on_completed:Runnable):Unit

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object LocalRouter extends Log {

  val destination_parser = new DestinationParser

  val TOPIC_DOMAIN = "topic"
  val QUEUE_DOMAIN = "queue"
  val DSUB_DOMAIN = "dsub"

  val QUEUE_KIND = "queue"
  val DEFAULT_QUEUE_PATH = "default"

  def is_wildcard_config(dto:StringIdDTO) = {
    if( dto.id == null ) {
      true
    } else {
      val parts = destination_parser.parts(dto.id)
      val path = destination_parser.decode_path(parts)
      PathParser.containsWildCards(path)
    }
  }

  class ConsumerContext(val destination:DestinationDTO, val consumer:DeliveryConsumer, val security:SecurityContext) {
    override def hashCode: Int = consumer.hashCode

    override def equals(obj: Any): Boolean = {
      obj match {
        case x:ConsumerContext=> x.consumer == consumer
        case _ => false
      }
    }
  }

  class ProducerContext(val destination:DestinationDTO, val producer:BindableDeliveryProducer, val security:SecurityContext) {
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

  trait Domain[D <: DomainDestination] {

    // holds all the destinations in the domain by id
    var destination_by_id = LinkedHashMap[String, D]()
    // holds all the destinations in the domain by path
    var destination_by_path = new PathMap[D]()
    // Can store consumers on wild cards paths

    val consumers_by_path = new PathMap[ConsumerContext]()
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

    def can_create_destination(path:Path, destination:DestinationDTO, security:SecurityContext):Option[String]
    def create_destination(path:Path, destination:DestinationDTO, security:SecurityContext):Result[D,String]

    def get_or_create_destination(path:Path, destination:DestinationDTO, security:SecurityContext):Result[D,String] = {
      Option(destination_by_path.chooseValue(path)).
      map(Success(_)).
      getOrElse( create_destination(path, destination, security))
    }

    var add_destination = (path:Path, dest:D) => {
      destination_by_path.put(path, dest)
      destination_by_id.put(dest.id, dest)

      // binds any matching wild card subs and producers...
      import JavaConversions._
      consumers_by_path.get( path ).foreach { x=>
        if( authorizer.can(x.security, bind_action(x.consumer), dest) ) {
          dest.bind(x.destination, x.consumer)
        }
      }
      producers_by_path.get( path ).foreach { x=>
        if( authorizer.can(x.security, "send", dest) ) {
          dest.connect(x.destination, x.producer)
        }
      }
    }

    var remove_destination = (path:Path, dest:D) => {
      destination_by_path.remove(path, dest)
      destination_by_id.remove(dest.id)
    }

    def can_destroy_destination(path:Path, destination:DestinationDTO, security:SecurityContext):Option[String] = {
      if( security==null ) {
        return None
      }

      for(dest <- get_destination_matches(path)) {

        if( destination.temp_owner != null ) {
          for( connection <- security.connection_id) {
            if( connection != destination.temp_owner.longValue() ) {
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
    def destroy_destination(path:Path, destination:DestinationDTO, security: SecurityContext):Unit

    def bind_action(consumer:DeliveryConsumer):String

    def can_bind_all(path:Path, destination:DestinationDTO, consumer:DeliveryConsumer, security:SecurityContext):Option[String] = {
      if( security==null ) {
        return None
      }

      // Only allow the owner to bind.
      if( destination.temp_owner != null ) {
        for( connection <- security.connection_id) {
          if( connection != destination.temp_owner.longValue() ) {
            return Some("Not authorized to receive from the temporary destination. Principals=%s".format(security.principal_dump))
          }
        }
      }

      val wildcard = PathParser.containsWildCards(path)
      var matches = get_destination_matches(path)

      // Should we attempt to auto create the destination?
      if( !wildcard ) {
        if ( matches.isEmpty && auto_create_destinations ) {
          val rc = create_destination(path, destination, security)
          if( rc.failed ) {
            return Some(rc.failure)
          }
          matches = get_destination_matches(path)
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

    def bind(path:Path, destination:DestinationDTO, consumer:DeliveryConsumer, security:SecurityContext):Unit = {
      var matches = get_destination_matches(path)
      matches.foreach { dest=>
        if( authorizer.can(security, bind_action(consumer), dest) ) {
          dest.bind(destination, consumer)
          for( l <- router_listeners) {
            l.on_bind(dest, consumer, security)
          }
        }
      }
      consumer.retain
      consumers_by_path.put(path, new ConsumerContext(destination, consumer, security))
    }

    def unbind(destination:DestinationDTO, consumer:DeliveryConsumer, persistent:Boolean, security: SecurityContext) = {
      val path = destination_parser.decode_path(destination.path)
      if( consumers_by_path.remove(path, new ConsumerContext(destination, consumer, null) ) ) {
        get_destination_matches(path).foreach{ dest=>
          dest.unbind(consumer, persistent)
          for( l <- router_listeners) {
            l.on_unbind(dest, consumer, persistent)
          }
        }
        consumer.release
      }
    }

    def can_connect_all(path:Path, destination:DestinationDTO, producer:BindableDeliveryProducer, security:SecurityContext):Option[String] = {

      val wildcard = PathParser.containsWildCards(path)
      var matches = get_destination_matches(path)

      if( wildcard ) {

        // Wild card sends never fail authorization... since a destination
        // may get crated later which the user is authorized to use.
        None

      } else {

        // Should we attempt to auto create the destination?
        if ( matches.isEmpty && auto_create_destinations ) {
          val rc = create_destination(path, destination, security)
          if( rc.failed ) {
            return Some(rc.failure)
          }
          matches = get_destination_matches(path)
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

    def connect(path:Path, destination:DestinationDTO, producer:BindableDeliveryProducer, security:SecurityContext):Unit = {
      get_destination_matches(path).foreach { dest=>
        if( authorizer.can(security, "send", dest) ) {
          dest.connect(destination, producer)
          for( l <- router_listeners) {
            l.on_connect(dest, producer, security)
          }
        }
      }
      producers_by_path.put(path, new ProducerContext(destination, producer, security))
    }

    def disconnect(destination:DestinationDTO, producer:BindableDeliveryProducer) = {
      val path = destination_parser.decode_path(destination.path)
      producers_by_path.remove(path, new ProducerContext(destination, producer, null))
      get_destination_matches(path).foreach { dest=>
        dest.disconnect(producer)
        for( l <- router_listeners) {
          l.on_disconnect(dest, producer)
        }
      }
    }

  }
  val topic_domain = new TopicDomain
  class TopicDomain extends Domain[Topic] {

    // Stores durable subscription queues.
    val durable_subscriptions_by_path = new PathMap[Queue]()
    val durable_subscriptions_by_id = HashMap[String, Queue]()

    def get_or_create_durable_subscription(destination:DurableSubscriptionDestinationDTO):Queue = {
      val key = destination.subscription_id
      durable_subscriptions_by_id.get( key ).getOrElse {
        val queue = _create_queue(BindingFactory.create(destination))
        durable_subscriptions_by_id.put(key, queue)
        queue
      }
    }

    def destroy_durable_subscription(queue:Queue):Unit = {
      val destination = queue.binding.binding_dto.asInstanceOf[DurableSubscriptionDestinationDTO]
      if( durable_subscriptions_by_id.remove( destination.subscription_id ).isDefined ) {
        val path = queue.binding.destination
        durable_subscriptions_by_path.remove(path, queue)
        var matches = get_destination_matches(path)
        matches.foreach( _.unbind_durable_subscription(destination, queue) )
        _destroy_queue(queue)
      }
    }

    def dsub_config(subid:String) = DurableSubscriptionQueueBinding.dsub_config(virtual_host, subid)

    def topic_config(name:Path):TopicDTO = {
      import collection.JavaConversions._
      import destination_parser._
      virtual_host.config.topics.find{ x=>
        x.id==null || decode_filter(x.id).matches(name)
      }.getOrElse(new TopicDTO)
    }

    override def connect(path:Path, destination:DestinationDTO, producer:BindableDeliveryProducer, security:SecurityContext):Unit = {
      destination match {
        case destination:DurableSubscriptionDestinationDTO =>

          // Connects a producer directly to a durable subscription..
          durable_subscriptions_by_id.get(destination.subscription_id).foreach { dest=>
            dest.connect(destination, producer)
            for( l <- router_listeners) {
              l.on_connect(dest, producer, security)
            }
          }

        case _ => super.connect(path, destination, producer, security)
      }
    }

    override def disconnect(destination:DestinationDTO, producer:BindableDeliveryProducer) = {
      destination match {
        case destination:DurableSubscriptionDestinationDTO =>
          durable_subscriptions_by_id.get(destination.subscription_id).foreach { dest=>
            dest.disconnect(producer)
            for( l <- router_listeners) {
              l.on_disconnect(dest, producer)
            }
          }
        case _ => super.disconnect(destination, producer)
      }
    }

    def destroy_destination(path:Path, destination: DestinationDTO, security: SecurityContext): Unit = {
      val matches = get_destination_matches(path)
      matches.foreach { dest =>
        for( l <- router_listeners) {
          l.on_destroy(dest, security)
        }

        // Disconnect the producers.
        dest.disconnect_producers

        // Delete the durable subs which
        for( queue <- dest.durable_subscriptions ) {
          // we delete the durable sub if it's not wildcard'ed
          if( !PathParser.containsWildCards(queue.binding.destination) ) {
            _destroy_queue(queue)
          }
        }

        for( consumer <- dest.consumers ) {
          consumer match {
            case queue:Queue =>
              // Delete any attached queue consumers..
              _destroy_queue(queue)
          }
        }

        // Un-register the topic.
        remove_destination(path, dest)
      }
    }

    def can_create_destination(path:Path, destination:DestinationDTO, security:SecurityContext):Option[String] = {
      if (security==null) {
        return None;
      }

      // We can't create a wild card destination.. only wild card subscriptions.
      assert( !PathParser.containsWildCards(path) )
      // A new destination is being created...

      val resource = new SecuredResource() {
        def resource_kind = TopicKind
        def id = destination_parser.encode_path(path)
      }
      if( !authorizer.can(security, "create", resource)) {
        return Some("Not authorized to create the topic '%s'. Principals=%s".format(resource.id, security.principal_dump))
      } else {
        None
      }
    }

    def create_destination(path:Path, destination:DestinationDTO, security:SecurityContext):Result[Topic,String] = {
      // We can't create a wild card destination.. only wild card subscriptions.
      assert( !PathParser.containsWildCards(path) )

      // A new destination is being created...
      val dto = topic_config(path)

      val resource = new SecuredResource() {
        def resource_kind = TopicKind
        def id = destination_parser.encode_path(path)
      }
      if( !authorizer.can(security, "create", resource)) {
        return Failure("Not authorized to create the topic '%s'. Principals=%s".format(resource.id, security.principal_dump))
      }

      val topic = new Topic(LocalRouter.this, destination.asInstanceOf[TopicDestinationDTO], ()=>topic_config(path), path.toString(destination_parser), path)
      add_destination(path, topic)

      for( l <- router_listeners) {
        l.on_create(topic, security)
      }
      Success(topic)
    }

    def bind_action(consumer:DeliveryConsumer):String = "receive"

    def bind_dsub(queue:Queue) = {
      assert_executing
      val destination = queue.binding.binding_dto.asInstanceOf[DurableSubscriptionDestinationDTO]
      val path = queue.binding.destination
      val wildcard = PathParser.containsWildCards(path)
      var matches = get_destination_matches(path)

      // We may need to create the topic...
      if( !wildcard && matches.isEmpty ) {
        create_destination(path, destination, null)
        matches = get_destination_matches(path)
      }

      durable_subscriptions_by_path.put(path, queue)
      durable_subscriptions_by_id.put(destination.subscription_id, queue)

      matches.foreach( _.bind_durable_subscription(destination, queue) )
    }

    def unbind_dsub(queue:Queue) = {
      assert_executing
      val destination = queue.destination_dto.asInstanceOf[DurableSubscriptionDestinationDTO]
      val path = queue.binding.destination
      var matches = get_destination_matches(path)

      durable_subscriptions_by_path.remove(path, queue)
      durable_subscriptions_by_id.remove(destination.subscription_id)

      matches.foreach( _.unbind_durable_subscription(destination, queue) )
    }

    override def bind(path: Path, destination: DestinationDTO, consumer: DeliveryConsumer, security: SecurityContext) {
      destination match {
        case destination:DurableSubscriptionDestinationDTO =>

          val key = destination.subscription_id
          val queue = durable_subscriptions_by_id.get( key ) match {
            case Some(queue) =>
              // We may need to update the bindings...
              if( queue.destination_dto != destination) {

                val binding = BindingFactory.create(destination)
                if( queue.tune_persistent && queue.store_id == -1 ) {
                  val record = QueueRecord(queue.store_id, binding.binding_kind, binding.binding_data)
                  // Update the bindings
                  virtual_host.store.add_queue(record) { rc => Unit }
                }

                // and then rebind the queue in the router.
                unbind_dsub(queue)
                queue.binding = binding
                bind_dsub(queue)

                // Make sure the update is visible in the queue's thread context..
                queue.dispatch_queue {
                  queue.binding = binding
                }
              }
              queue
            case None =>
              _create_queue(BindingFactory.create(destination))
          }


          // Typically durable subs are only consumed by one connection at a time. So collocate the
          // queue onto the consumer's dispatch queue.
          queue.dispatch_queue.setTargetQueue(consumer.dispatch_queue)
          queue.bind(destination, consumer)

          for( l <- router_listeners) {
            l.on_bind(queue, consumer, security)
          }

        case _ =>
          super.bind(path, destination, consumer, security)
      }
    }

    override def unbind(destination: DestinationDTO, consumer: DeliveryConsumer, persistent: Boolean, security: SecurityContext) = {
      destination match {
        case destination:DurableSubscriptionDestinationDTO =>
          durable_subscriptions_by_id.get(destination.subscription_id).foreach { queue =>
            queue.unbind(consumer, persistent)
            if( persistent ) {
              _destroy_queue(queue, security)
            }
            for( l <- router_listeners) {
              l.on_unbind(queue, consumer, persistent)
            }
          }
        case _ =>
          super.unbind( destination, consumer, persistent, security)
      }
    }

    override def can_bind_all(path: Path, destination: DestinationDTO, consumer: DeliveryConsumer, security: SecurityContext) = {
      destination match {
        case destination:DurableSubscriptionDestinationDTO =>
          val config = dsub_config(destination.subscription_id)
          if( !path.parts.isEmpty ) {
            super.can_bind_all(path, destination, consumer, security) orElse {
              if( !durable_subscriptions_by_id.contains(destination.subscription_id) ) {
                can_create_dsub(config, security)
              } else {
                None
              } orElse {
                can_bind_dsub(config, consumer, security)
              }
            }
          } else {
            // User is trying to directly receive from a durable subscription.. has to allready exist.
            if( !durable_subscriptions_by_id.contains(destination.subscription_id) ) {
              Some("Durable subscription does not exist")
            } else {
              can_bind_dsub(config, consumer, security)
            }
          }
        case _ =>
          super.can_bind_all(path, destination, consumer, security)
      }
    }


    override def can_connect_all(path: Path, destination: DestinationDTO, producer: BindableDeliveryProducer, security: SecurityContext) = {
      destination match {
        case destination:DurableSubscriptionDestinationDTO =>
          val config = dsub_config(destination.subscription_id)

            // User is trying to directly send to a durable subscription.. has to allready exist.
          if( !durable_subscriptions_by_id.contains(destination.subscription_id) ) {
            Some("Durable subscription does not exist")
          } else {
            can_connect_dsub(config, security)
          }
        case _ =>
          super.can_connect_all(path, destination, producer, security)
      }
    }


    def get_dsub_secured_resource(config: DurableSubscriptionDTO):SecuredResource = {
      durable_subscriptions_by_id.get(config.id).getOrElse(new SecuredResource() {
        def resource_kind = SecuredResource.DurableSubKind
        def id = config.id
      })
    }

    def can_create_dsub(config:DurableSubscriptionDTO, security:SecurityContext) = {
      val resource = get_dsub_secured_resource(config)
      if( !authorizer.can(security, "create", resource) ) {
        Some("Not authorized to create the durable subscription '%s'. Principals=%s".format(resource.id, security.principal_dump))
      } else {
        None
      }
    }

    def can_connect_dsub(config:DurableSubscriptionDTO, security:SecurityContext):Option[String] = {
      val resource = get_dsub_secured_resource(config)
      if( !authorizer.can(security, "send", resource) ) {
        Some("Not authorized to send to durable subscription '%s'. Principals=%s".format(resource.id, security.principal_dump))
      } else {
        None
      }
    }

    def can_bind_dsub(config:DurableSubscriptionDTO, consumer:DeliveryConsumer, security:SecurityContext):Option[String] = {
      val resource = get_dsub_secured_resource(config)
      val action = if ( consumer.browser ) "receive" else "consume"
      if( !authorizer.can(security, action, resource) ) {
        Some("Not authorized to %s from durable subscription '%s'. Principals=%s".format(action, resource.id, security.principal_dump))
      } else {
        None
      }
    }
  }

  val queue_domain = new QueueDomain
  class QueueDomain extends Domain[Queue] {

    def bind(queue:Queue) = {
      val path = queue.binding.destination
      assert( !PathParser.containsWildCards(path) )
      add_destination(path, queue)

      import OptionSupport._
      if( queue.config.unified.getOrElse(false) ) {
        // hook up the queue to be a subscriber of the topic.

        val topic = topic_domain.get_or_create_destination(path, new TopicDestinationDTO(queue.binding.binding_dto.path), null).success
        topic.bind(null, queue)
      }
    }

    def unbind(queue:Queue) = {
      val path = queue.binding.destination
      remove_destination(path, queue)

      import OptionSupport._
      if( queue.config.unified.getOrElse(false) ) {
        // unhook the queue from the topic
        val topic = topic_domain.get_or_create_destination(path, new TopicDestinationDTO(queue.binding.binding_dto.path), null).success
        topic.unbind(queue, false)
      }
    }

    def destroy_destination(path:Path, destination: DestinationDTO, security: SecurityContext): Unit = {
      val matches = get_destination_matches(path)
      matches.foreach { queue =>
        for( l <- router_listeners) {
          l.on_destroy(queue, security)
        }
        _destroy_queue(queue)
      }
    }

    def can_create_destination(path: Path, destination:DestinationDTO, security: SecurityContext):Option[String] = {
      val resource = new SecuredResource() {
        def resource_kind = QueueKind
        def id = destination_parser.encode_path(path)
      }
      if( authorizer.can(security, "create", resource)) {
        None
      } else {
        Some("Not authorized to create the queue '%s'. Principals=%s".format(resource.id, security.principal_dump))
      }
    }

    def create_destination(path: Path, destination:DestinationDTO, security: SecurityContext) = {
      val dto = new QueueDestinationDTO
      dto.path.addAll(destination.path)
      val binding = QueueDomainQueueBinding.create(dto)

      val resource = new SecuredResource() {
        def resource_kind = QueueKind
        def id = destination_parser.encode_path(path)
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
    def create_configured_dests(list: ArrayList[_ <: StringIdDTO], d: Domain[_], f: (Array[String]) => DestinationDTO) = {
      list.foreach { dto =>
        if (dto.id != null) {
          try {
            val parts = destination_parser.parts(dto.id)
            val path = destination_parser.decode_path(parts)
            if (!PathParser.containsWildCards(path)) {
              d.get_or_create_destination(path, f(parts), null)
            }
          } catch {
            case x:PathException => warn(x, "Invalid destination id '%s'", dto.id)
          }
        }
      }
    }
    create_configured_dests(virtual_host.config.queues, queue_domain, (parts) => new QueueDestinationDTO(parts))
    create_configured_dests(virtual_host.config.topics, topic_domain, (parts) => new TopicDestinationDTO(parts))

    virtual_host.config.dsubs.foreach { dto =>
      if (dto.id != null && dto.topic!=null ) {

        // We will create the durable sub if it does not exist yet..
        if( !topic_domain.durable_subscriptions_by_id.contains(dto.id) ) {
          val destination = new DurableSubscriptionDestinationDTO()
          destination.subscription_id = dto.id
          destination.path = Arrays.asList(destination_parser.parts(dto.topic) : _ *)
          destination.selector = dto.selector
          _create_queue(BindingFactory.create(destination))
        }

      }
    }
  }

  protected def _start(on_completed: Runnable) = {
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
                    if( binding.binding_dto.temp_owner != null ) {
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
      // Now that we have restored persistent destinations,
      // make sure we create any NON-wildcard destinations
      // explicitly listed in the config.

      create_configure_destinations
      on_completed.run()
    }
  }

  protected def _stop(on_completed: Runnable) = {
//    val tracker = new LoggingTracker("router shutdown", virtual_host.console_log, dispatch_queue)
    queues_by_store_id.valuesIterator.foreach { queue=>
      queue.stop
//      tracker.stop(queue)
    }
//    tracker.callback(on_completed)
    on_completed.run
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
    topic_domain.destinations.foreach { node =>

      node.consumers.keys.headOption.foreach{ consumer =>
        node.producers.keys.foreach { r=>
          r.collocate(consumer.dispatch_queue)
        }
      }
    }


    queue_domain.destinations.foreach { queue=>
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

  def domain(destination: DestinationDTO):Domain[_ <: DomainDestination] = destination match {
    case x:TopicDestinationDTO => topic_domain
    case x:QueueDestinationDTO => queue_domain
    case _ => throw new RuntimeException("Unknown domain type: "+destination.getClass)
  }

  def bind(destination: Array[DestinationDTO], consumer: DeliveryConsumer, security: SecurityContext) = {
    if(!virtual_host.service_state.is_started) {
      Some("virtual host stopped.")
    } else {
      consumer.retain
      val paths = destination.map(x=> (destination_parser.decode_path(x.path), x) )
      dispatch_queue ! {
        val failures = paths.flatMap(x=> domain(x._2).can_bind_all(x._1, x._2, consumer, security) )
        val rc = if( !failures.isEmpty ) {
          Some(failures.mkString("; "))
        } else {
          paths.foreach { x=>
            domain(x._2).bind(x._1, x._2, consumer, security)
          }
          None
        }
        consumer.release
        rc
      }
    }
  }

  def unbind(destinations: Array[DestinationDTO], consumer: DeliveryConsumer, persistent:Boolean, security: SecurityContext) = {
    consumer.retain
    dispatch_queue {
      destinations.foreach { destination=>
        domain(destination).unbind(destination, consumer, persistent, security)
      }
      consumer.release
    }
  }

  def connect(destinations: Array[DestinationDTO], producer: BindableDeliveryProducer, security: SecurityContext) = {
    if(!virtual_host.service_state.is_started) {
      Some("virtual host stopped.")
    } else {
      producer.retain
      val paths = destinations.map(x=> (destination_parser.decode_path(x.path), x) )
      dispatch_queue ! {

        val failures = paths.flatMap(x=> domain(x._2).can_connect_all(x._1, x._2, producer, security) )
        if( !failures.isEmpty ) {
          producer.release
          Some(failures.mkString("; "))
        } else {
          paths.foreach { x=>
            domain(x._2).connect(x._1, x._2, producer, security)
          }
          producer.connected()
          None
        }
      }
    }
  }

  def disconnect(destinations:Array[DestinationDTO], producer:BindableDeliveryProducer) = {
    dispatch_queue {
      destinations.foreach { destination=>
        domain(destination).disconnect(destination, producer)
      }
      producer.disconnected()
      producer.release()
    }
  }

  def create(destinations:Array[DestinationDTO], security: SecurityContext) = dispatch_queue ! {
    if(!virtual_host.service_state.is_started) {
      Some("virtual host stopped.")
    } else {
      val paths = destinations.map(x=> (destination_parser.decode_path(x.path), x) )
      val failures = paths.flatMap(x=> domain(x._2).can_create_destination(x._1, x._2, security) )
      if( !failures.isEmpty ) {
        Some(failures.mkString("; "))
      } else {
        paths.foreach { x=>
          domain(x._2).create_destination(x._1, x._2, security)
        }
        None
      }
    }
  }

  def delete(destinations:Array[DestinationDTO], security: SecurityContext) = dispatch_queue ! {
    if(!virtual_host.service_state.is_started) {
      Some("virtual host stopped.")
    } else {
      val paths = destinations.map(x=> (destination_parser.decode_path(x.path), x) )
      val failures = paths.flatMap(x=> domain(x._2).can_destroy_destination(x._1, x._2, security) )
      if( !failures.isEmpty ) {
        Some(failures.mkString("; "))
      } else {
        paths.foreach { x=>
          domain(x._2).destroy_destination(x._1, x._2, security)
        }
        None
      }
    }
  }


  def get_or_create_destination(id: DestinationDTO, security: SecurityContext) = dispatch_queue ! {
    _get_or_create_destination(id, security)
  }

  /**
   * Returns the previously created queue if it already existed.
   */
  def _get_or_create_destination(dto: DestinationDTO, security:SecurityContext): Result[DomainDestination, String] = {
    val path = destination_parser.decode_path(dto.path)
    domain(dto).get_or_create_destination(path, dto, security)
  }


  /////////////////////////////////////////////////////////////////////////////
  //
  // Queue management methods.  Queues are multi-purpose and get used by both
  // the queue domain and topic domain.
  //
  /////////////////////////////////////////////////////////////////////////////

  var queues_by_binding = LinkedHashMap[Binding, Queue]()
  var queues_by_store_id = LinkedHashMap[Long, Queue]()

  /**
   * Gets an existing queue.
   */
  def get_queue(dto:DestinationDTO) = dispatch_queue ! {
    queues_by_binding.get(BindingFactory.create(dto))
  }

  /**
   * Gets an existing queue.
   */
  def get_queue(id:Long) = dispatch_queue ! {
    queues_by_store_id.get(id)
  }


  def _create_queue(binding:Binding, id:Long= -1):Queue = {

    var qid = id
    if( qid == -1 ) {
      qid = virtual_host.queue_id_counter.incrementAndGet
    }

    val config = binding.config(virtual_host)

    val queue = new Queue(this, qid, binding, config)

    queue.start
    queues_by_binding.put(binding, queue)
    queues_by_store_id.put(qid, queue)

    // this causes the queue to get registered in the right location in
    // the router.
    binding.bind(this, queue)
    queue
  }

  /**
   * Returns true if the queue no longer exists.
   */
  def destroy_queue(id:Long, security:SecurityContext) = dispatch_queue ! { _destroy_queue(id,security) }

  def _destroy_queue(id:Long, security:SecurityContext):Option[String] = {
    queues_by_store_id.get(id) match {
      case Some(queue) =>
        _destroy_queue(queue,security)
      case None =>
        Some("Does not exist")
    }
  }

  /**
   * Returns true if the queue no longer exists.
   */
  def destroy_queue(dto:DestinationDTO, security:SecurityContext) = dispatch_queue ! { _destroy_queue(dto, security) }

  def _destroy_queue(dto:DestinationDTO, security:SecurityContext):Option[String] = {
    queues_by_binding.get(BindingFactory.create(dto)) match {
      case Some(queue) =>
        _destroy_queue(queue, security)
      case None =>
        Some("Does not exist")
    }
  }

  def _destroy_queue(queue:Queue, security:SecurityContext):Option[String] = {
    if( !authorizer.can(security, "destroy", queue) ) {
      return Some("Not authorized to destroy queue '%s'. Principals=%s".format(queue.id, security.principal_dump))
    }
    _destroy_queue(queue)
    None
  }


  def _destroy_queue(queue: Queue) {
    queue.stop(dispatch_queue.runnable{

      queue.binding.unbind(this, queue)
      queues_by_binding.remove(queue.binding)
      queues_by_store_id.remove(queue.store_id)
      if (queue.tune_persistent) {
        queue.dispatch_queue {
          virtual_host.store.remove_queue(queue.store_id) {
            x => Unit
          }
        }
      }
    })
  }

  def apply_update(on_completed:Runnable) = {
    val tracker = new LoggingTracker("domain update", virtual_host.broker.console_log)
    topic_domain.apply_update(tracker)
    queue_domain.apply_update(tracker)
    // we may need to create some more destinations.
    create_configure_destinations
    tracker.callback(on_completed)
  }
}