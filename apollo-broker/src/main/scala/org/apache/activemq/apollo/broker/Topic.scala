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
import collection.mutable.{HashMap, ListBuffer}
import java.util.concurrent.TimeUnit
import org.fusesource.hawtdispatch._

/**
 * <p>
 * A logical messaging topic
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Topic(val router:LocalRouter, val destination_dto:TopicDestinationDTO, var config_updater: ()=>TopicDTO, val id:String, path:Path) extends DomainDestination {

  var producers = ListBuffer[BindableDeliveryProducer]()
  var consumers = ListBuffer[DeliveryConsumer]()
  var durable_subscriptions = ListBuffer[Queue]()
  var consumer_queues = HashMap[DeliveryConsumer, Queue]()
  var idled_at = 0L
  val created_at = System.currentTimeMillis()
  var auto_delete_after = 0
  var producer_counter = 0L
  var consumer_counter = 0L

  var config:TopicDTO = _

  refresh_config

  import OptionSupport._

  def virtual_host: VirtualHost = router.virtual_host

  def slow_consumer_policy = config.slow_consumer_policy.getOrElse("block")

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
        val now = System.currentTimeMillis()
        idled_at = now
        if( auto_delete_after!=0 ) {
          virtual_host.dispatch_queue.after(auto_delete_after, TimeUnit.SECONDS) {
            if( now == idled_at ) {
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
    destination match {
      case null=> // unified queue case

        consumers += consumer
        val list = List(consumer)
        producers.foreach({ r=>
          r.bind(list)
        })

      case destination:TopicDestinationDTO=>
        var target = consumer
        slow_consumer_policy match {
          case "queue" =>

            // create a temp queue so that it can spool
            val queue = router._create_queue(new TempQueueBinding(consumer))
            queue.dispatch_queue.setTargetQueue(consumer.dispatch_queue)
            queue.bind(List(consumer))

            consumer_queues += consumer->queue
            target = queue

          case "block" =>
            // just have dispatcher dispatch directly to them..
        }

        consumers += target
        consumer_counter += 1
        val list = target :: Nil
        producers.foreach({ r=>
          r.bind(list)
        })

    }
    check_idle
  }

  def unbind (consumer:DeliveryConsumer, persistent:Boolean) = {

    consumer_queues.remove(consumer) match {
      case Some(queue)=>

        queue.unbind(List(consumer))

        queue.binding match {
          case x:TempQueueBinding =>

            val list = List(queue)
            producers.foreach({ r=>
              r.unbind(list)
            })
            router._destroy_queue(queue.id, null)

        }

      case None=>

        // producers are directly delivering to the consumer..
        val original = consumers.size
        consumers -= consumer
        if( original!= consumers.size ) {
          val list = List(consumer)
          producers.foreach({ r=>
            r.unbind(list)
          })
        }
    }
    check_idle

  }

  def bind_durable_subscription(destination: DurableSubscriptionDestinationDTO, queue:Queue)  = {
    if( !durable_subscriptions.contains(queue) ) {
      durable_subscriptions += queue
      val list = List(queue)
      producers.foreach({ r=>
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
      producers.foreach({ r=>
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
    producers += producer
    producer_counter += 1
    producer.bind(consumers.toList ::: durable_subscriptions.toList)
    check_idle
  }

  def disconnect (producer:BindableDeliveryProducer) = {
    producers = producers.filterNot( _ == producer )
    producer.unbind(consumers.toList ::: durable_subscriptions.toList)
    check_idle
  }

}
