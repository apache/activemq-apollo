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
import security.SecurityContext
import java.util.concurrent.TimeUnit
import scala.Array
import org.apache.activemq.apollo.dto._
import java.util.{Arrays, ArrayList}
import collection.mutable.{LinkedHashMap, HashMap}
import collection.{Iterable, JavaConversions}

trait DomainDestination {

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

  val TEMP_TOPIC_DOMAIN = "temp-topic"
  val TEMP_QUEUE_DOMAIN = "temp-queue"

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

  def dispatch_queue:DispatchQueue = virtual_host.dispatch_queue

  def auto_create_destinations = {
    import OptionSupport._
    virtual_host.config.auto_create_destinations.getOrElse(true)
  }

  private val ALL = new Path(List(AnyDescendantPart))

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

    def can_destroy_destination(path:Path, destination:DestinationDTO, security:SecurityContext):Option[String]
    def destroy_destination(path:Path, destination:DestinationDTO):Unit

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
        if( can_bind_one(path, x.destination, x.consumer, x.security) ) {
          dest.bind(x.destination, x.consumer)
        }
      }
      producers_by_path.get( path ).foreach { x=>
        if( can_connect_one(path, x.destination, x.producer, x.security) ) {
          dest.connect(x.destination, x.producer)
        }
      }
    }

    var remove_destination = (path:Path, dest:D) => {
      destination_by_path.remove(path, dest)
      destination_by_id.remove(dest.id)
    }

    def can_bind_one(path:Path, destination:DestinationDTO, consumer:DeliveryConsumer, security:SecurityContext):Boolean
    def can_bind_all(path:Path, destination:DestinationDTO, consumer:DeliveryConsumer, security:SecurityContext):Option[String] = {

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
          if( !can_bind_one(path, destination, consumer, security) ) {
            return Some("Not authorized to receive from the destination.")
          }
        }
      }
      None
    }

    def bind(path:Path, destination:DestinationDTO, consumer:DeliveryConsumer, security:SecurityContext):Unit = {
      var matches = get_destination_matches(path)
      matches.foreach { dest=>
        if( can_bind_one(path, destination, consumer, security) ) {
          dest.bind(destination, consumer)
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
        }
        consumer.release
      }

//      if( persistent ) {
//          destroy_queue(consumer.binding, security_context).failure_option.foreach{ reason=>
//            async_die(reason)
//          }
//      }

    }

    def can_connect_one(path:Path, destination:DestinationDTO, producer:BindableDeliveryProducer, security:SecurityContext):Boolean

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
        if( !can_connect_one(path, destination, producer, security) ) {
          return Some("Not authorized to send to the destination.")
        }

        None
      }
    }

    def connect(path:Path, destination:DestinationDTO, producer:BindableDeliveryProducer, security:SecurityContext):Unit = {
      get_destination_matches(path).foreach { dest=>
        if( can_connect_one(path, destination, producer, security) ) {
          dest.connect(destination, producer)
        }
      }
      producers_by_path.put(path, new ProducerContext(destination, producer, security))
    }

    def disconnect(destination:DestinationDTO, producer:BindableDeliveryProducer) = {
      val path = destination_parser.decode_path(destination.path)
      producers_by_path.remove(path, new ProducerContext(destination, producer, null))
      get_destination_matches(path).foreach { dest=>
        dest.disconnect(producer)
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
        val queue = _create_queue(QueueBinding.create(destination))
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
        _destroy_queue(queue.id, null)
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
          }

        case _ => super.connect(path, destination, producer, security)
      }
    }

    override def disconnect(destination:DestinationDTO, producer:BindableDeliveryProducer) = {
      destination match {
        case destination:DurableSubscriptionDestinationDTO =>
          durable_subscriptions_by_id.get(destination.subscription_id).foreach { dest=>
            dest.disconnect(producer)
          }
        case _ => super.disconnect(destination, producer)
      }
    }

    def can_destroy_destination(path:Path, destination: DestinationDTO, security: SecurityContext): Option[String] = {
      val matches = get_destination_matches(path)
      val rc = matches.foldLeft(None:Option[String]) { case (rc,dest) =>
        rc.orElse {
          if( virtual_host.authorizer!=null && security!=null && !virtual_host.authorizer.can_destroy(security, virtual_host, dest.config)) {
            Some("Not authorized to destroy topic: %s".format(dest.id))
          } else {
            None
          }
        }
      }

      // TODO: destroy not yet supported on topics..  Need to disconnect all
      // clients and destroy remove any durable subs on the topic.
      Some("Topic destroy not yet implemented.")
    }

    def destroy_destination(path:Path, destination: DestinationDTO): Unit = {
      val matches = get_destination_matches(path)
//        matches.foreach { dest =>
//          remove_destination(dest.path, dest)
//        }
    }

    def can_create_destination(path:Path, destination:DestinationDTO, security:SecurityContext):Option[String] = {
      // We can't create a wild card destination.. only wild card subscriptions.
      assert( !PathParser.containsWildCards(path) )
      // A new destination is being created...
      val dto = topic_config(path)

      if(  virtual_host.authorizer!=null && security!=null && !virtual_host.authorizer.can_create(security, virtual_host, dto)) {
        Some("Not authorized to create the destination")
      } else {
        None
      }
    }

    def create_destination(path:Path, destination:DestinationDTO, security:SecurityContext):Result[Topic,String] = {
      // We can't create a wild card destination.. only wild card subscriptions.
      assert( !PathParser.containsWildCards(path) )

      // A new destination is being created...
      val dto = topic_config(path)

      if(  virtual_host.authorizer!=null && security!=null && !virtual_host.authorizer.can_create(security, virtual_host, dto)) {
        return new Failure("Not authorized to create the destination")
      }

      val topic = new Topic(LocalRouter.this, destination.asInstanceOf[TopicDestinationDTO], ()=>topic_config(path), path.toString(destination_parser), path)
      add_destination(path, topic)
      Success(topic)
    }

    def can_bind_one(path:Path, destination:DestinationDTO, consumer:DeliveryConsumer, security:SecurityContext):Boolean = {
      val config = topic_config(path)
      val authorizer = virtual_host.authorizer
      if( authorizer!=null && security!=null && !authorizer.can_receive_from(security, virtual_host, config) ) {
        return false;
      }
      true
    }

    def can_connect_one(path:Path, destination:DestinationDTO, producer:BindableDeliveryProducer, security:SecurityContext):Boolean = {
      val config = topic_config(path)
      val authorizer = virtual_host.authorizer
      !(authorizer!=null && security!=null && !authorizer.can_send_to(security, virtual_host, config) )
    }

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

                val binding = QueueBinding.create(destination)
                if( queue.tune_persistent && queue.store_id == -1 ) {

                  val record = new QueueRecord
                  record.key = queue.store_id
                  record.binding_data = binding.binding_data
                  record.binding_kind = binding.binding_kind

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
              _create_queue(QueueBinding.create(destination))
          }


          // Typically durable subs are only consumed by one connection at a time. So collocate the
          // queue onto the consumer's dispatch queue.
          queue.dispatch_queue.setTargetQueue(consumer.dispatch_queue)
          queue.bind(destination, consumer)

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


    def can_create_dsub(config:DurableSubscriptionDTO, security:SecurityContext) = {
      val authorizer = virtual_host.authorizer
      if( authorizer!=null && security!=null && !authorizer.can_create(security, virtual_host, config) ) {
        Some("Not authorized to create the durable subscription.")
      } else {
        None
      }
    }

    def can_connect_dsub(config:DurableSubscriptionDTO, security:SecurityContext):Option[String] = {
      val authorizer = virtual_host.authorizer
      if( authorizer!=null && security!=null && !authorizer.can_send_to(security, virtual_host, config) ) {
        Some("Not authorized to send to the durable subscription.")
      } else {
        None
      }
    }

    def can_bind_dsub(config:DurableSubscriptionDTO, consumer:DeliveryConsumer, security:SecurityContext):Option[String] = {
      val authorizer = virtual_host.authorizer
      if( authorizer!=null && security!=null ) {
        if ( consumer.browser ) {
          if( !authorizer.can_receive_from(security, virtual_host, config) ) {
            Some("Not authorized to receive from the durable subscription.")
          } else {
            None
          }
        } else {
          if( !authorizer.can_consume_from(security, virtual_host, config) ) {
            Some("Not authorized to consume from the durable subscription.")
          } else {
            None
          }
        }
      } else {
        None
      }
    }
  }

  val queue_domain = new QueueDomain
  class QueueDomain extends Domain[Queue] {

    def can_create_queue(config:QueueDTO, security:SecurityContext) = {
      if( virtual_host.authorizer==null || security==null) {
        true
      } else {
        virtual_host.authorizer.can_create(security, virtual_host, config)
      }
    }

    def can_destroy_queue(config:QueueDTO, security:SecurityContext) = {
      if( virtual_host.authorizer==null || security==null) {
        true
      } else {
        virtual_host.authorizer.can_destroy(security, virtual_host, config)
      }
    }

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

    def can_destroy_destination(path:Path, destination: DestinationDTO, security: SecurityContext): Option[String] = {
      val matches = get_destination_matches(path)
      matches.foldLeft(None:Option[String]) { case (rc,dest) =>
        rc.orElse {
          if( can_destroy_queue(dest.config, security) ) {
            None
          } else {
            Some("Not authorized to destroy queue: %s".format(dest.id))
          }
        }
      }
    }

    def destroy_destination(path:Path, destination: DestinationDTO): Unit = {
      val matches = get_destination_matches(path)
      matches.foreach { dest =>
        _destroy_queue(dest)
      }
    }

    def can_create_destination(path: Path, destination:DestinationDTO, security: SecurityContext):Option[String] = {
      val dto = new QueueDestinationDTO
      dto.path.addAll(destination.path)
      val binding = QueueDomainQueueBinding.create(dto)
      val config = binding.config(virtual_host)
      if( can_create_queue(config, security) ) {
        None
      } else {
        Some("Not authorized to create the queue")
      }
    }

    def create_destination(path: Path, destination:DestinationDTO, security: SecurityContext) = {
      val dto = new QueueDestinationDTO
      dto.path.addAll(destination.path)

      val binding = QueueDomainQueueBinding.create(dto)
      val config = binding.config(virtual_host)
      if( can_create_queue(config, security) ) {
        Success(_create_queue(binding))
      } else {
        Failure("Not authorized to create the queue")
      }

    }

    def can_bind_one(path:Path, dto:DestinationDTO, consumer:DeliveryConsumer, security: SecurityContext):Boolean = {
      val binding = QueueDomainQueueBinding.create(dto)
      val config = binding.config(virtual_host)
      if(  virtual_host.authorizer!=null && security!=null ) {
        if( consumer.browser ) {
          if( !virtual_host.authorizer.can_receive_from(security, virtual_host, config) ) {
            return false;
          }
        } else {
          if( !virtual_host.authorizer.can_consume_from(security, virtual_host, config) ) {
            return false
          }
        }
      }
      return true;
    }

    def can_connect_one(path:Path, dto:DestinationDTO, producer:BindableDeliveryProducer, security:SecurityContext):Boolean = {
      val binding = QueueDomainQueueBinding.create(dto)
      val config = binding.config(virtual_host)
      val authorizer = virtual_host.authorizer
      !( authorizer!=null && security!=null && !authorizer.can_send_to(security, virtual_host, config) )
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
          _create_queue(QueueBinding.create(destination))
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
                    // Drop temp queues on restart..
                    virtual_host.store.remove_queue(queue_key){x=> task.run}
                  } else {
                    dispatch_queue {
                      _create_queue(QueueBinding.create(record.binding_kind, record.binding_data), queue_key)
                      task.run
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
    queues_by_id.valuesIterator.foreach { queue=>
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

      node.consumers.headOption.foreach{ consumer =>
        node.producers.foreach { r=>
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

  def delete(destinations:Array[DestinationDTO], security: SecurityContext) = dispatch_queue ! {
    val paths = destinations.map(x=> (destination_parser.decode_path(x.path), x) )
    val failures = paths.flatMap(x=> domain(x._2).can_destroy_destination(x._1, x._2, security) )
    if( !failures.isEmpty ) {
      Some(failures.mkString("; "))
    } else {
      paths.foreach { x=>
        domain(x._2).destroy_destination(x._1, x._2)
      }
      None
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

  var queues_by_binding = LinkedHashMap[QueueBinding, Queue]()
  var queues_by_id = LinkedHashMap[String, Queue]()

  /**
   * Gets an existing queue.
   */
  def get_queue(dto:DestinationDTO) = dispatch_queue ! {
    queues_by_binding.get(QueueBinding.create(dto))
  }

  /**
   * Gets an existing queue.
   */
  def get_queue(id:String) = dispatch_queue ! {
    queues_by_id.get(id)
  }


  def _create_queue(binding:QueueBinding, id:Long= -1):Queue = {

    var qid = id
    if( qid == -1 ) {
      qid = virtual_host.queue_id_counter.incrementAndGet
    }

    val config = binding.config(virtual_host)

    val queue = new Queue(this, qid, binding, config)

    queue.start
    queues_by_binding.put(binding, queue)
    queues_by_id.put(queue.id, queue)

    // this causes the queue to get registered in the right location in
    // the router.
    binding.bind(this, queue)
    queue
  }

  /**
   * Returns true if the queue no longer exists.
   */
  def destroy_queue(id:String, security:SecurityContext) = dispatch_queue ! { _destroy_queue(id,security) }

  def _destroy_queue(id:String, security:SecurityContext):Option[String] = {
    queues_by_id.get(id) match {
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
    queues_by_binding.get(QueueBinding.create(dto)) match {
      case Some(queue) =>
        _destroy_queue(queue, security)
      case None =>
        Some("Does not exist")
    }
  }

  def _destroy_queue(queue:Queue, security:SecurityContext):Option[String] = {

    if( security!=null && queue.config.acl!=null ) {
      if( !virtual_host.authorizer.can_destroy(security, virtual_host, queue.config) ) {
        return Some("Not authorized to destroy")
      }
    }
    _destroy_queue(queue)
    None
  }


  def _destroy_queue(queue: Queue) {
    queue.stop(dispatch_queue.runnable{

      queue.binding.unbind(this, queue)
      queues_by_binding.remove(queue.binding)
      queues_by_id.remove(queue.id)
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