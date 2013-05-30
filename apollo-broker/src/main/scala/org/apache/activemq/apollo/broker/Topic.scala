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
import scala.collection.immutable.List
import org.apache.activemq.apollo.dto._
import java.util.concurrent.TimeUnit
import org.fusesource.hawtdispatch._
import collection.mutable.{HashSet, HashMap, ListBuffer}
import security.SecuredResource
import java.util.concurrent.atomic.AtomicInteger

/**
 * <p>
 * A logical messaging topic
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Topic(val router:LocalRouter, val address:DestinationAddress, var config_updater: ()=>TopicDTO) extends DomainDestination with SecuredResource {

  val topic_metrics = new DestMetricsDTO
  topic_metrics.enqueue_ts = now
  topic_metrics.dequeue_ts = now

  val resource_kind =SecuredResource.TopicKind
  var proxy_sessions = new HashSet[DeliverySession]()
  var topic_queue_consumers = new HashMap[DeliveryConsumer, DeliveryConsumer]()

  @transient
  var retained_message: Delivery = _

  import language.implicitConversions
  implicit def from_link(from:LinkDTO):(Long,Long,Long)=(from.enqueue_item_counter, from.enqueue_size_counter, from.enqueue_ts)
  implicit def from_session(from:DeliverySession):(Long,Long,Long)=(from.enqueue_item_counter, from.enqueue_size_counter, from.enqueue_ts)

  def add_link_counters(to:LinkDTO, from:(Long,Long,Long)):Unit = {
    to.enqueue_item_counter += from._1
    to.enqueue_size_counter += from._2
    to.enqueue_ts = to.enqueue_ts max from._3
  }
  def add_enqueue_counters(to:DestMetricsDTO, from:(Long,Long,Long)):Unit = {
    to.enqueue_item_counter += from._1
    to.enqueue_size_counter += from._2
    to.enqueue_ts = to.enqueue_ts max from._3
  }
  def add_dequeue_counters(to:DestMetricsDTO, from:(Long,Long,Long)):Unit = {
    to.dequeue_item_counter += from._1
    to.dequeue_size_counter += from._2
    to.dequeue_ts = to.enqueue_ts max from._3
  }

  val producer_tracker = new DeliveryConsumer {
    def retained() = 0
    def retain() {}
    def release() {}

    def matches(message: Delivery) = true
    def is_persistent = false
    def dispatch_queue = null
    def connect(producer: DeliveryProducer) = new ProxyProducerSession(producer)

  }


  class ProxyProducerSession(val producer:DeliveryProducer) extends DeliverySession {


    dispatch_queue {
      proxy_sessions.add(this)
    }

    override def toString: String = "ProxyProducerSession(topic="+id+")"

    def remaining_capacity = 1
    var enqueue_ts = 0L
    var enqueue_size_counter = 0L
    var enqueue_item_counter = 0L
    var refiller:Task = null


    def offer(value: Delivery) = {
      enqueue_item_counter += 1
      enqueue_size_counter += value.size
      enqueue_ts = now
      value.retain match {
        case RetainSet =>
          // TODO: perhaps persist so that we can recall what was
          // retained across broker restarts.
          retained_message = value;
        case RetainRemove =>
          retained_message = null;
        case _ =>
      }
      if( value.ack != null ) {
        value.ack(Consumed, value.uow)
      }
      true
    }

    def close = {
      dispatch_queue {
        proxy_sessions.remove(this)
        producers.get(producer.asInstanceOf[BindableDeliveryProducer]) match {
          case Some(link) => add_link_counters(link, this)
          case _          => add_enqueue_counters(topic_metrics, this)
        }
      }
    }

    def full = false
    def consumer = producer_tracker
  }

  case class ProxyConsumerSession(proxy:ProxyDeliveryConsumer, session:DeliverySession) extends DeliverySession with SessionSinkFilter[Delivery] {

    override def toString = ""+address+"->"+session

    def downstream = session

    dispatch_queue {
      proxy_sessions.add(this)
    }

    def close = {
      session.close
      dispatch_queue {
        proxy_sessions.remove(this)
        consumers.get(proxy.registered) match {
          case Some(proxy) => add_link_counters(proxy.link, this)
          case _ =>
            proxy.consumer match {
              case queue:Queue =>
              case _ =>
                add_dequeue_counters(topic_metrics, this)
            }
        }
      }
    }

    def producer = session.producer
    def consumer = session.consumer

    val ack_pass_through = proxy.link.kind == "dsub"

    def offer(value: Delivery) = {
      val copy = value.copy();
      copy.sender ::= address
      if ( ack_pass_through ) {
        copy.ack = value.ack
        copy.uow = value.uow
      }

      val accepted = downstream.offer(copy)

      // If we don't ack now, then the sender's ack will
      // wait for the consumers ack which might be a nice option to give folks.
      if( accepted && !ack_pass_through && value.ack!=null ) {
        value.ack(Consumed, value.uow)
      }
      accepted
    }
  }

  case class ProxyDeliveryConsumer(consumer:DeliveryConsumer, link:LinkDTO, registered:DeliveryConsumer) extends DeliveryConsumerFilter(consumer) {
    override def connect(producer: DeliveryProducer) = {
      new ProxyConsumerSession(this, next.connect(producer))
    }
  }

  val producers = HashMap[BindableDeliveryProducer, LinkDTO]()
  val consumers = HashMap[DeliveryConsumer, ProxyDeliveryConsumer]()
  var durable_subscriptions = ListBuffer[Queue]()
  var idled_at = 0L
  val created_at = now
  var auto_delete_after = 0

  var config:TopicDTO = _

  refresh_config

  import OptionSupport._

  override def toString = address.toString

  def virtual_host: VirtualHost = router.virtual_host
  def now = virtual_host.broker.now
  def dispatch_queue = virtual_host.dispatch_queue

  def slow_consumer_policy = config.slow_consumer_policy.getOrElse("block")

  def status(show_producers:Boolean, show_consumers:Boolean): FutureResult[TopicStatusDTO] = {
    val rc = FutureResult[TopicStatusDTO]()
    status(show_producers, show_consumers, x => rc.set(Success(x)))
    rc
  }

  var state = "STARTED"

  def status(show_producers:Boolean, show_consumers:Boolean, on_complete:(TopicStatusDTO)=>Unit):Unit = {
    dispatch_queue.assertExecuting()

    val rc = new TopicStatusDTO
    rc.id = this.id
    rc.state = state
    rc.state_since = this.created_at
    rc.config = this.config

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
      add_link_counters(rc, o);
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
      consumers_links.put(proxy.consumer, o)
      rc.consumers.add(o)
    }

    if( topic_queue !=null ) {
      val link = new LinkDTO()
      link.kind = "topic-queue"
      link.id = topic_queue.store_id.toString()
      link.label = "shared queue"
      link.enqueue_ts = now
      rc.consumers.add(link)
    }

    // Add in the counters from the live sessions..
    proxy_sessions.foreach{ session =>
      val stats = from_session(session)
      session match {
        case session:ProxyProducerSession =>
          for( link <- producer_links.get(session.producer.asInstanceOf[BindableDeliveryProducer]) ) {
            add_link_counters(link, stats)
          }
        case session:ProxyConsumerSession =>
          for( link <- consumers_links.get(session.consumer) ) {
            add_link_counters(link, stats)
          }
      }
    }

    // Now update the topic counters..
    rc.metrics.current_time = now
    DestinationMetricsSupport.add_destination_metrics(rc.metrics, topic_metrics)
    producer_links.values.foreach { link =>
      add_enqueue_counters(rc.metrics, link)
    }

    if( retained_message!=null ) {
      rc.retained = 1
    }

    if( !show_producers ) {
      rc.producers = null
    }
    if( !show_consumers ) {
      rc.consumers = null
    }

    var futures = List[Future[(TopicStatusDTO)=>Unit]]()

    if ( topic_queue!=null ) {
      val future = Future[(TopicStatusDTO)=>Unit]()
      futures ::= future
      topic_queue.dispatch_queue {
        val metrics = topic_queue.get_queue_metrics
        metrics.enqueue_item_counter = 0
        metrics.enqueue_size_counter = 0
        metrics.enqueue_ts = 0
        metrics.producer_counter = 0
        metrics.producer_count = 0
//        metrics.consumer_counter = 0
//        metrics.consumer_count = 0
        future.set((rc)=>{
          DestinationMetricsSupport.add_destination_metrics(rc.metrics, metrics)
        })
      }
    }

    consumers_links.foreach { case (consumer, link) =>
      consumer match {
        case queue:Queue =>
          // aggregate the queue stats instead of the link stats.
          val future = Future[(TopicStatusDTO)=>Unit]()
          futures ::= future
          queue.dispatch_queue {
            val metrics = queue.get_queue_metrics
            metrics.enqueue_item_counter = 0
            metrics.enqueue_size_counter = 0
            metrics.enqueue_ts = 0
            metrics.producer_counter = 0
            metrics.producer_count = 0
            metrics.consumer_counter = 0
            metrics.consumer_count = 0
            future.set((rc)=>{
              DestinationMetricsSupport.add_destination_metrics(rc.metrics, metrics)
            })
          }
        case _ =>
          // plain link, add it's ats.
          add_dequeue_counters(rc.metrics, link)
      }
    }

    Future.all(futures).onComplete{ data=>
      data.foreach(_(rc))
      on_complete(rc)
    }
  }

  def browse(from_seq:Long, to:Option[Long], max:Long)(func: (BrowseResult)=>Unit):Unit = {
    val msg = retained_message
    if ( msg==null ) {
      func(BrowseResult(0, 0, 0, Array()))
    } else {
      val status = new EntryStatusDTO()
      status.seq = retained_message.seq
      status.size = retained_message.size
      status.state = "loaded"
      status.is_prefetched = true;
      func(BrowseResult(status.seq, status.seq, 1, Array((status, retained_message))))
    }
  }

  def update(on_completed:Task) = {
    refresh_config
    on_completed.run
  }

  def refresh_config = {
    import OptionSupport._

    config = config_updater()
    auto_delete_after = config.auto_delete_after.getOrElse(30)
    if( auto_delete_after!= 0 ) {
      // we don't auto delete explicitly configured destinations.
      if( !LocalRouter.is_wildcard_destination(config.id) ) {
        auto_delete_after = 0
      }
    }
    check_idle
  }

  def delete:Option[String] = {
    dispatch_queue.assertExecuting()
    state match {
      case "STARTED" =>
        if (producers.isEmpty && consumers.isEmpty) {
          state = "DELETED"
          router.local_topic_domain.remove_destination(address.path, this)
          DestinationMetricsSupport.add_destination_metrics(router.virtual_host.dead_topic_metrics, topic_metrics)
          None
        } else {
          Some("Topic is in use.")
        }        
      case _ =>
        Some("Topic already deleted.")
    }
  }

  def check_idle {
    if (producers.isEmpty && consumers.isEmpty && topic_queue==null && retained_message==null) {
      if (idled_at==0) {
        val previously_idle_at = now
        idled_at = previously_idle_at
        if( auto_delete_after!=0 ) {
          dispatch_queue.after(auto_delete_after, TimeUnit.SECONDS) {
            if( previously_idle_at == idled_at ) {
              delete
            }
          }
        }
      }
    } else {
      idled_at = 0
    }
  }

  var topic_queue:Queue = null

  def bind(address: BindAddress, consumer:DeliveryConsumer, on_bind:()=>Unit):Unit = {

    val remaining = new AtomicInteger(1)
    var bind_release:()=>Unit = ()=> {
      if( remaining.decrementAndGet() == 0 ) {
        on_bind()
      }
    }

    def send_retained = {
      val r = retained_message
      if (r != null) {
        val copy = r.copy()
        copy.sender ::= address
        val producer = new  DeliveryProducerRoute(router) {
          refiller = NOOP
          def dispatch_queue = Topic.this.dispatch_queue
          override protected def on_connected = {
            copy.ack = (d,x) => consumer.dispatch_queue {
              unbind(consumer :: Nil)
            }
            offer(copy) // producer supports 1 message overflow.
          }
        }
        producer.bind(consumer :: Nil, ()=>{})
        producer.connected()
      }
    }

    val target = consumer match {

      // Consumer might be a durable sub.
      case queue:Queue =>
        if( !durable_subscriptions.contains(queue) ) {
          durable_subscriptions += queue
        }
        consumer

      case _ =>


        slow_consumer_policy match {
          case "queue" =>

            // create a temp queue so that it can spool
            if ( topic_queue==null ) {
              topic_queue = router._create_queue(new TempQueueBinding(id, Topic.this.address, Option(config.subscription).getOrElse(new QueueSettingsDTO)))
              producers.keys.foreach({ r=>
                remaining.incrementAndGet()
                r.bind(List(topic_queue), bind_release)
              })
            }
            val proxy = new DeliveryConsumerFilter(consumer) {
              // Make this consumer act like a continuous queue browser
              override def browser = true
              override def start_from_tail = true
              override def close_on_drain = false
              override def exclusive = false
            }
            topic_queue_consumers.put(consumer, proxy)
            topic_queue.bind(List(proxy), bind_release)
            send_retained
            return

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
            link.label = "shared queue"
          case x:QueueDomainQueueBinding =>
            link.kind = "queue"
            link.id = queue.id
            link.label = queue.id
          case x:DurableSubscriptionQueueBinding =>
            link.kind = "dsub"
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

    send_retained
    val proxy = ProxyDeliveryConsumer(target, link, consumer)
    consumers.put(consumer, proxy)
    topic_metrics.consumer_counter += 1
    val list = proxy :: Nil
    producers.keys.foreach({ r=>
      remaining.incrementAndGet()
      r.bind(list, bind_release)
    })
    bind_release()
    check_idle
  }

  def unbind (consumer:DeliveryConsumer, persistent:Boolean) = {
    val list = topic_queue_consumers.remove(consumer) match {
      case Some(consumer)=>
        topic_queue.unbind(List(consumer))

        // Once we don't have any subscribers.. delete the queue.
        if( topic_queue_consumers.isEmpty ) {
          val queue = topic_queue
          topic_queue = null

          queue.dispatch_queue {
            if( queue.all_subscriptions.isEmpty ) {
              val metrics = queue.get_queue_metrics
              router.dispatch_queue {
                if(router.service_state.is_started) {
                  router._destroy_queue(queue)
                }
              }
              dispatch_queue {
                topic_metrics.dequeue_item_counter += metrics.dequeue_item_counter
                topic_metrics.dequeue_size_counter += metrics.dequeue_size_counter
                topic_metrics.dequeue_ts = topic_metrics.dequeue_ts max metrics.dequeue_ts
                topic_metrics.nack_item_counter += metrics.nack_item_counter
                topic_metrics.nack_size_counter += metrics.nack_size_counter
                topic_metrics.nack_ts  = topic_metrics.nack_ts max metrics.nack_ts
                topic_metrics.expired_item_counter += metrics.expired_item_counter
                topic_metrics.expired_size_counter += metrics.expired_size_counter
                topic_metrics.expired_ts  = topic_metrics.expired_ts max metrics.expired_ts
              }
            }
          }
        }
        List()
      case None =>
        consumers.remove(consumer) match {
          case Some(consumer)=>
            add_dequeue_counters(topic_metrics, consumer.link)
            List(consumer.consumer)
          case None =>
            List()
        }
    }

    // consumer might be a durable sub..
    consumer match {
      case queue:Queue =>
        if( durable_subscriptions.contains(queue) ) {
          durable_subscriptions -= queue
        }
      case _ =>
    }

    for( producer <- producers.keys ) {
     producer.unbind(list)
    }
    check_idle
  }

  def connect (address:ConnectAddress, producer:BindableDeliveryProducer) = {
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
    topic_metrics.producer_counter += 1
    var targets:List[DeliveryConsumer] = producer_tracker :: consumers.values.toList
    if( topic_queue !=null ) {
      targets ::= topic_queue
    }
    producer.bind(targets, ()=>{})
    check_idle
  }

  def disconnect (producer:BindableDeliveryProducer) = {
    for(link <- producers.remove(producer) ) {
      add_enqueue_counters(topic_metrics, link)
    }
    var targets:List[DeliveryConsumer] = producer_tracker :: consumers.values.toList
    if( topic_queue !=null ) {
      targets ::= topic_queue
    }
    producer.unbind(targets)
    check_idle
  }

  def disconnect_producers:Unit ={
    for( (_, link) <- producers ) {
      add_enqueue_counters(topic_metrics, link)
    }
    producers.clear
    check_idle
  }

}
