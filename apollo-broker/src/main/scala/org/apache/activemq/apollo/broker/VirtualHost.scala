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
package org.apache.activemq.apollo.broker;

import _root_.java.util.{ArrayList, HashMap}
import _root_.java.lang.{String}
import _root_.scala.collection.JavaConversions._
import org.fusesource.hawtdispatch._

import java.util.concurrent.TimeUnit
import org.apache.activemq.apollo.broker.store.{Store, StoreFactory}
import org.apache.activemq.apollo.util._
import path.PathFilter
import ReporterLevel._
import org.fusesource.hawtbuf.{Buffer, AsciiBuffer}
import collection.JavaConversions
import java.util.concurrent.atomic.AtomicLong
import org.apache.activemq.apollo.util.OptionSupport._
import org.apache.activemq.apollo.util.path.{Path, PathParser}
import org.apache.activemq.apollo.dto.{DestinationDTO, QueueDTO, BindingDTO, VirtualHostDTO}
import security.{AclAuthorizer, JaasAuthenticator, Authenticator, Authorizer}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object VirtualHost extends Log {

  /**
   * Creates a default a configuration object.
   */
  def default_config() = {
    val rc = new VirtualHostDTO
    rc.id = "default"
    rc.host_names.add("localhost")
    rc.store = null
    rc
  }

  /**
   * Validates a configuration object.
   */
  def validate(config: VirtualHostDTO, reporter:Reporter):ReporterLevel = {
     new Reporting(reporter) {

      if( config.host_names.isEmpty ) {
        error("Virtual host must be configured with at least one host name.")
      }

      result |= StoreFactory.validate(config.store, reporter)
       
    }.result
  }
  
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class VirtualHost(val broker: Broker, val id:Long) extends BaseService {
  import VirtualHost._
  
  override val dispatch_queue:DispatchQueue = createQueue("virtual-host") // getGlobalQueue(DispatchPriority.HIGH).createQueue("virtual-host")

  var config:VirtualHostDTO = _
  val router = new Router(this)

  var names:List[String] = Nil;

  var store:Store = null
  var direct_buffer_pool:DirectBufferPool = null
  val queue_id_counter = new LongCounter

  val session_counter = new AtomicLong(0)

  var authenticator:Authenticator = _
  var authorizer:Authorizer = _

  override def toString = if (config==null) "virtual-host" else "virtual-host: "+config.id

  /**
   * Validates and then applies the configuration.
   */
  def configure(config: VirtualHostDTO, reporter:Reporter) = ^{
    if ( validate(config, reporter) < ERROR ) {
      this.config = config

      if( service_state.is_started ) {
        // TODO: apply changes while he broker is running.
        reporter.report(WARN, "Updating virtual host configuration at runtime is not yet supported.  You must restart the broker for the change to take effect.")

      }
    }
  } |>>: dispatch_queue


  override protected def _start(on_completed:Runnable):Unit = {

    val tracker = new LoggingTracker("virtual host startup", dispatch_queue)

    if( config.authentication != null ) {
      if( config.authentication.enabled.getOrElse(true) ) {
        // Virtual host has it's own settings.
        authenticator = new JaasAuthenticator(config.authentication)
        authorizer = new AclAuthorizer(config.authentication.acl_principal_kinds().toList)
      } else {
        // Don't use security on this host.
        authenticator = null
        authorizer = null
      }
    } else {
      // use the broker's settings..
      authenticator = broker.authenticator
      authorizer = broker.authorizer
    }

    store = StoreFactory.create(config.store)

    //    val memory_pool_config: String = null
    var direct_buffer_pool_config: String = "hawtdb:activemq.tmp"

    if( direct_buffer_pool_config!=null &&  (store!=null && !store.supports_direct_buffers) ) {
      warn("The direct buffer pool will not be used because the configured store does not support them.")
      direct_buffer_pool_config = null
    }

    if( direct_buffer_pool_config!=null ) {
      direct_buffer_pool = DirectBufferPoolFactory.create(direct_buffer_pool_config)
      direct_buffer_pool.start
    }

    if( store!=null ) {
      store.configure(config.store, LoggingReporter(VirtualHost))
      val store_startup_done = tracker.task("store startup")
      store.start {

        val get_key_done = tracker.task("store get last queue key")
        store.get_last_queue_key{ key=>
          key match {
            case Some(x)=>
              queue_id_counter.set(key.get)
            case None =>
              warn("Could not get last queue key")
          }
          get_key_done.run
        }

        if( config.purge_on_startup.getOrElse(false) ) {
          store_startup_done.name = "store purge"
          store.purge {
            store_startup_done.run
          }
        } else {
          store_startup_done.name = "store recover queues"
          store.list_queues { queue_keys =>
            for( queue_key <- queue_keys) {
              val task = tracker.task("store load queue key: "+queue_key)
              // Use a global queue to so we concurrently restore
              // the queues.
              globalQueue {
                store.get_queue(queue_key) { x =>
                  x match {
                    case Some(record)=>
                    dispatch_queue ^{
                      router.create_queue(record, null)
                      task.run
                    }
                    case _ =>
                      task.run
                  }
                }
              }
            }
            store_startup_done.run
          }
        }
      }
    }

    tracker.callback(on_completed)

    if(config.regroup_connections.getOrElse(false)) {
      schedual_connection_regroup
    }
  }


  override protected def _stop(on_completed:Runnable):Unit = {

    val tracker = new LoggingTracker("virtual host shutdown", dispatch_queue)
    router.queues.valuesIterator.foreach { queue=>
      tracker.stop(queue)
    }
    if( direct_buffer_pool!=null ) {
      direct_buffer_pool.stop
      direct_buffer_pool = null
    }

    if( store!=null ) {
      tracker.stop(store);
    }
    tracker.callback(on_completed)
  }


  // Try to periodically re-balance connections so that consumers/producers
  // are grouped onto the same thread.
  def schedual_connection_regroup:Unit = {
    def connectionRegroup = {

      // this should really be much more fancy.  It should look at the messaging
      // rates between producers and consumers, look for natural data flow partitions
      // and then try to equally divide the load over the available processing
      // threads/cores.
      router.routing_nodes.foreach { node =>

        // For the topics, just collocate the producers onto the first consumer's
        // thread.
        node.broadcast_consumers.headOption.foreach{ consumer =>
          node.broadcast_producers.foreach { r=>
            r.producer.collocate(consumer.dispatch_queue)
          }
        }

        node.queues.foreach { queue=>

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
      schedual_connection_regroup
    }
    dispatch_queue.dispatchAfter(1, TimeUnit.SECONDS, ^{ if(service_state.is_started) { connectionRegroup } } )
  }

  def destination_config(name:Path):Option[DestinationDTO] = {
    import collection.JavaConversions._
    import DestinationParser.default._
    import AsciiBuffer._
    config.destinations.find( x=> parseFilter(ascii(x.name)).matches(name) )
  }

  def queue_config(binding:Binding):Option[QueueDTO] = {
    import collection.JavaConversions._
    config.queues.find{ config=>
      binding.matches(config)
    }
  }

  def queue_config(dto:BindingDTO):Option[QueueDTO] = {
    queue_config(BindingFactory.create(dto))
  }

}
