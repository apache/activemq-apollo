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

import org.apache.activemq.apollo.util._
import path.Path
import scala.collection.immutable.List
import org.apache.activemq.apollo.dto._
import java.util.concurrent.TimeUnit
import org.fusesource.hawtdispatch._
import collection.mutable.{HashSet, HashMap, ListBuffer}
import java.lang.Long
import security.SecuredResource
/**
 * <p>
 * A logical messaging topic
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Topic(val router:LocalRouter, val destination_dto:TopicDestinationDTO, var config_updater: ()=>TopicDTO, val id:String, path:Path) extends DomainDestination with SecuredResource {

  var enqueue_item_counter = 0L
  var enqueue_size_counter = 0L
  var enqueue_ts = now

  var dequeue_item_counter = 0L
  var dequeue_size_counter = 0L
  var dequeue_ts = now

  val resource_kind =SecuredResource.TopicKind

  var proxy_sessions = new HashSet[TopicDeliverySession]()

  implicit def from_link(from:LinkDTO):(Long,Long,Long)=(from.enqueue_item_counter, from.enqueue_size_counter, from.enqueue_ts)
  implicit def from_session(from:TopicDeliverySession):(Long,Long,Long)=(from.enqueue_item_counter, from.enqueue_size_counter, from.enqueue_ts)

  def add_counters(to:LinkDTO, from:(Long,Long,Long)):Unit = {
    to.enqueue_item_counter += from._1
    to.enqueue_size_counter += from._2
    to.enqueue_ts = to.enqueue_ts max from._3
  }
  def add_counters(to:Topic, from:(Long,Long,Long)):Unit = {
    to.enqueue_item_counter += from._1
    to.enqueue_size_counter += from._2
    to.enqueue_ts = to.enqueue_ts max from._3
  }

  case class TopicDeliverySession(session:DeliverySession) extends DeliverySession with SessionSinkFilter[Delivery] {
    def downstream = session

    dispatch_queue {
      proxy_sessions.add(this)
    }

    def close = {
      session.close
      dispatch_queue {
        proxy_sessions.remove(this)
        consumers.get(session.consumer) match {
          case Some(proxy) => add_counters(proxy.link, this)
          case _          => add_counters(Topic.this, this)
        }
        producers.get(session.producer.asInstanceOf[BindableDeliveryProducer]) match {
          case Some(link) => add_counters(link, this)
          case _          => add_counters(Topic.this, this)
        }
      }
    }

    def producer = session.producer
    def consumer = session.consumer

    def offer(value: Delivery) = downstream.offer(value)
  }

  case class ProxyDeliveryConsumer(consumer:DeliveryConsumer, link:LinkDTO) extends DeliveryConsumer {
    def retained() = consumer.retained()
    def retain() = consumer.retain()
    def release() = consumer.release()
    def matches(message: Delivery) = consumer.matches(message)
    def is_persistent = consumer.is_persistent
    def dispatch_queue = consumer.dispatch_queue
    def connect(producer: DeliveryProducer) = {
      new TopicDeliverySession(consumer.connect(producer))
    }
  }

  val producers = HashMap[BindableDeliveryProducer, LinkDTO]()
  val consumers = HashMap[DeliveryConsumer, ProxyDeliveryConsumer]()
  var durable_subscriptions = ListBuffer[Queue]()
  var consumer_queues = HashMap[DeliveryConsumer, Queue]()
  var idled_at = 0L
  val created_at = now
  var auto_delete_after = 0
  var producer_counter = 0L
  var consumer_counter = 0L

  var config:TopicDTO = _

  refresh_config

  import OptionSupport._

  override def toString = destination_dto.toString

  def virtual_host: VirtualHost = router.virtual_host
  def now = virtual_host.broker.now
  def dispatch_queue = virtual_host.dispatch_queue

  def slow_consumer_policy = config.slow_consumer_policy.getOrElse("block")

  def status:TopicStatusDTO = {
    dispatch_queue.assertExecuting()

    val rc = new TopicStatusDTO
    rc.id = this.id
    rc.state = "STARTED"
    rc.state_since = this.created_at
    rc.config = this.config

    rc.metrics.producer_counter = producer_counter
    rc.metrics.consumer_counter = consumer_counter
    rc.metrics.producer_count = producers.size
    rc.metrics.consumer_count = consumers.size

    this.durable_subscriptions.foreach { q =>
      rc.dsubs.add(q.id)
    }

    def copy(o:LinkDTO) = {
      val rc = new LinkDTO()
      rc.id = o.id
      rc.kind = o.kind
      rc.label = o.label
      rc.enqueue_ts = o.enqueue_ts
      add_counters(rc, o);
      rc
    }

    // build the list of producer and consumer links..
    val producer_links = HashMap[BindableDeliveryProducer, LinkDTO]()
    val consumers_links = HashMap[DeliveryConsumer, LinkDTO]()
    this.producers.foreach { case (producer, link) =>
      val o = copy(link)
      producer_links.put(producer, o)
      rc.producers.add(o)
    }
    this.consumers.foreach { case (consumer, proxy) =>
      val o = copy(proxy.link)
      consumers_links.put(consumer, o)
      rc.consumers.add(o)
    }

    // Add in the counters from the live sessions..
    proxy_sessions.foreach{ session =>
      val stats = from_session(session)
      for( link <- producer_links.get(session.producer.asInstanceOf[BindableDeliveryProducer]) ) {
        add_counters(link, stats)
      }
      for( link <- consumers_links.get(session.consumer) ) {
        add_counters(link, stats)
      }
    }

    // Now update the topic counters..
    rc.metrics.enqueue_item_counter = enqueue_item_counter
    rc.metrics.enqueue_size_counter = enqueue_size_counter
    rc.metrics.enqueue_ts = enqueue_ts

    rc.metrics.dequeue_item_counter = dequeue_item_counter
    rc.metrics.dequeue_size_counter = dequeue_size_counter
    rc.metrics.dequeue_ts = dequeue_ts
    consumers_links.values.foreach { link =>
      rc.metrics.enqueue_item_counter += link.enqueue_item_counter
      rc.metrics.enqueue_size_counter += link.enqueue_size_counter
      rc.metrics.enqueue_ts = rc.metrics.enqueue_ts max link.enqueue_ts
    }
    producer_links.values.foreach { link =>
      rc.metrics.dequeue_item_counter += link.enqueue_item_counter
      rc.metrics.dequeue_size_counter += link.enqueue_size_counter
      rc.metrics.dequeue_ts = rc.metrics.dequeue_ts max link.enqueue_ts
    }

    // Add in any queue metrics that the topic may own.
    for(queue <- consumer_queues.values) {
      val metrics = queue.get_queue_metrics
      metrics.enqueue_item_counter = 0
      metrics.enqueue_size_counter = 0
      metrics.enqueue_ts = 0
      DestinationMetricsSupport.add_destination_metrics(rc.metrics, metrics)
    }
    rc
  }


  def update(on_completed:Runnable) = {
    refresh_config
    on_completed.run
  }

  def refresh_config = {
    import OptionSupport._

    config = config_updater()
    auto_delete_after = config.auto_delete_after.getOrElse(60*5)
    if( auto_delete_after!= 0 ) {
      // we don't auto delete explicitly configured destinations.
      if( !LocalRouter.is_wildcard_config(config) ) {
        auto_delete_after = 0
      }
    }
    check_idle
  }

  def check_idle {
    if (producers.isEmpty && consumers.isEmpty && durable_subscriptions.isEmpty) {
      if (idled_at==0) {
        val previously_idle_at = now
        idled_at = previously_idle_at
        if( auto_delete_after!=0 ) {
          dispatch_queue.after(auto_delete_after, TimeUnit.SECONDS) {
            if( previously_idle_at == idled_at ) {
              router.topic_domain.remove_destination(path, this)
            }
          }
        }
      }
    } else {
      idled_at = 0
    }
  }

  def bind (destination: DestinationDTO, consumer:DeliveryConsumer) = {

    val target = destination match {
      case null=>
        // this is the unified queue case..
        consumer
      case destination:TopicDestinationDTO=>
        var target = consumer
        slow_consumer_policy match {
          case "queue" =>

            // create a temp queue so that it can spool
            val queue = router._create_queue(new TempQueueBinding(consumer, path, destination_dto))
            queue.dispatch_queue.setTargetQueue(consumer.dispatch_queue)
            queue.bind(List(consumer))
            consumer_queues += consumer->queue
            queue

          case "block" =>
            // just have dispatcher dispatch directly to them..
            consumer
        }
    }

    val link = new LinkDTO()
    link.kind = "unknown"
    link.label = "unknown"
    link.enqueue_ts = now
    target match {
      case queue:Queue =>
        queue.binding match {
          case x:TempQueueBinding =>
            link.kind = "topic-queue"
            link.id = queue.store_id.toString()
            x.key match {
              case target:DeliveryConsumer=>
                for(connection <- target.connection) {
                  link.label = connection.transport.getRemoteAddress.toString
                }
              case _ =>
            }
          case x:QueueDomainQueueBinding =>
            link.kind = "queue"
            link.id = queue.id
            link.label = queue.id
        }
      case _ =>
        for(connection <- target.connection) {
          link.kind = "connection"
          link.id = connection.id.toString
          link.label = connection.transport.getRemoteAddress.toString
        }
    }

    val proxy = ProxyDeliveryConsumer(target, link)
    consumers.put(consumer, proxy)
    consumer_counter += 1
    val list = proxy :: Nil
    producers.keys.foreach({ r=>
      r.bind(list)
    })
    check_idle
  }

  def unbind (consumer:DeliveryConsumer, persistent:Boolean) = {

    for(proxy <- consumers.remove(consumer)) {
      add_counters(Topic.this, proxy.link)
      val list = consumer_queues.remove(consumer) match {
        case Some(queue) =>
          queue.unbind(List(consumer))
          queue.binding match {
            case x:TempQueueBinding =>
              router._destroy_queue(queue)
          }
          List(queue)
        case None =>
          List(consumer)
      }
      producers.keys.foreach({ r=>
        r.unbind(list)
      })
    }
    check_idle
  }

  def bind_durable_subscription(destination: DurableSubscriptionDestinationDTO, queue:Queue)  = {
    if( !durable_subscriptions.contains(queue) ) {
      durable_subscriptions += queue
      val list = List(queue)
      producers.keys.foreach({ r=>
        r.bind(list)
      })
      consumer_queues.foreach{case (consumer, q)=>
        if( q==queue ) {
          bind(destination, consumer)
        }
      }
    }
    check_idle
  }

  def unbind_durable_subscription(destination: DurableSubscriptionDestinationDTO, queue:Queue)  = {
    if( durable_subscriptions.contains(queue) ) {
      durable_subscriptions -= queue
      val list = List(queue)
      producers.keys.foreach({ r=>
        r.unbind(list)
      })
      consumer_queues.foreach{case (consumer, q)=>
        if( q==queue ) {
          unbind(consumer, false)
        }
      }
    }
    check_idle
  }

  def connect (destination:DestinationDTO, producer:BindableDeliveryProducer) = {
    val link = new LinkDTO()
    producer.connection match {
      case Some(connection) =>
        link.kind = "connection"
        link.id = connection.id.toString
        link.label = connection.transport.getRemoteAddress.toString
      case _ =>
        link.kind = "unknown"
        link.label = "unknown"
    }
    producers.put(producer, link)
    producer_counter += 1
    producer.bind(consumers.values.toList ::: durable_subscriptions.toList)
    check_idle
  }

  def disconnect (producer:BindableDeliveryProducer) = {
    for(link <- producers.remove(producer) ) {
      add_counters(this, link)
    }
    check_idle
  }

  def disconnect_producers:Unit ={
    for( (_, link) <- producers ) {
      add_counters(this, link)
    }
    producers.clear
    check_idle
  }

}
