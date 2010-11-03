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

import org.apache.activemq.apollo.dto.{VirtualHostDTO}
import java.util.concurrent.TimeUnit
import org.apache.activemq.apollo.store.{Store, StoreFactory}
import org.apache.activemq.apollo.util._
import path.PathFilter
import ReporterLevel._
import org.fusesource.hawtbuf.{Buffer, AsciiBuffer}
import collection.JavaConversions
import java.util.concurrent.atomic.AtomicLong

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object VirtualHost extends Log {

  val destination_parser_options = new ParserOptions
  destination_parser_options.queuePrefix = new AsciiBuffer("queue:")
  destination_parser_options.topicPrefix = new AsciiBuffer("topic:")
  destination_parser_options.tempQueuePrefix = new AsciiBuffer("temp-queue:")
  destination_parser_options.tempTopicPrefix = new AsciiBuffer("temp-topic:")

  /**
   * Creates a default a configuration object.
   */
  def defaultConfig() = {
    val rc = new VirtualHostDTO
    rc.id = "default"
    rc.enabled = true
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
class VirtualHost(val broker: Broker, val id:Long) extends BaseService with DispatchLogging with LoggingReporter {
  import VirtualHost._
  
  override protected def log = VirtualHost
  override val dispatchQueue:DispatchQueue = org.fusesource.hawtdispatch.createQueue("virtual-host");

  var config:VirtualHostDTO = _
  val router = new Router(this)

  var names:List[String] = Nil;
  def setNamesArray( names:ArrayList[String]) = {
    this.names = names.toList
  }

  var store:Store = null
  var direct_buffer_pool:DirectBufferPool = null
  var transactionManager:TransactionManagerX = new TransactionManagerX
  val queue_id_counter = new LongCounter

  val session_counter = new AtomicLong(0)

  override def toString = if (config==null) "virtual-host" else "virtual-host: "+config.id

  /**
   * Validates and then applies the configuration.
   */
  def configure(config: VirtualHostDTO, reporter:Reporter) = ^{
    if ( validate(config, reporter) < ERROR ) {
      this.config = config

      if( serviceState.isStarted ) {
        // TODO: apply changes while he broker is running.
        reporter.report(WARN, "Updating virtual host configuration at runtime is not yet supported.  You must restart the broker for the change to take effect.")

      }
    }
  } |>>: dispatchQueue


  override protected def _start(onCompleted:Runnable):Unit = {

    val tracker = new LoggingTracker("virtual host startup", dispatchQueue)
    store = StoreFactory.create(config.store)

    //    val memory_pool_config: String = null
    var direct_buffer_pool_config: String = "hawtdb:activemq.tmp"

    if( direct_buffer_pool_config!=null &&  (store!=null && !store.supportsDirectBuffers) ) {
      warn("The direct buffer pool will not be used because the configured store does not support them.")
      direct_buffer_pool_config = null
    }

    if( direct_buffer_pool_config!=null ) {
      direct_buffer_pool = DirectBufferPoolFactory.create(direct_buffer_pool_config)
      direct_buffer_pool.start
    }

    if( store!=null ) {
      store.configure(config.store, this)
      val storeStartupDone = tracker.task("store startup")
      store.start {

        val getKeyDone = tracker.task("store get last queue key")
        store.getLastQueueKey{ key=>
          key match {
            case Some(x)=>
              queue_id_counter.set(key.get)
            case None =>
              warn("Could not get last queue key")
          }
          getKeyDone.run
        }

        if( config.purge_on_startup ) {
          storeStartupDone.name = "store purge"
          store.purge {
            storeStartupDone.run
          }
        } else {
          storeStartupDone.name = "store recover queues"
          store.listQueues { queueKeys =>
            for( queueKey <- queueKeys) {
              val task = tracker.task("store load queue key: "+queueKey)
              // Use a global queue to so we concurrently restore
              // the queues.
              globalQueue {
                store.getQueue(queueKey) { x =>
                  x match {
                    case Some(record)=>
                    dispatchQueue ^{
                      router.create_queue(record)
                      task.run
                    }
                    case _ =>
                      task.run
                  }
                }
              }
            }
            storeStartupDone.run
          }
        }
      }
    }


    //Recover transactions:
    transactionManager.virtualHost = this
    transactionManager.loadTransactions();

    tracker.callback(onCompleted)

    schedualConnectionRegroup
  }


  override protected def _stop(onCompleted:Runnable):Unit = {

    val tracker = new LoggingTracker("virtual host shutdown", dispatchQueue)
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
    tracker.callback(onCompleted)
  }


  // Try to periodically re-balance connections so that consumers/producers
  // are grouped onto the same thread.
  def schedualConnectionRegroup:Unit = {
    def connectionRegroup = {

      // this should really be much more fancy.  It should look at the messaging
      // rates between producers and consumers, look for natural data flow partitions
      // and then try to equally divide the load over the available processing
      // threads/cores.
      val nodes = router.destinations.get(PathFilter.ANY_DESCENDENT)

      JavaConversions.asIterable(nodes).foreach { node =>

        // For the topics, just collocate the producers onto the first consumer's
        // thread.
        node.broadcast_consumers.headOption.foreach{ consumer =>
          node.broadcast_producers.foreach { r=>
            r.producer.collocate(consumer.dispatchQueue)
          }
        }

        node.queues.foreach { queue=>

          queue.dispatchQueue {

            // Collocate the queue's with the first consumer
            // TODO: change this so it collocates with the fastest consumer.

            queue.all_subscriptions.headOption.map( _._1 ).foreach { consumer=>
              queue.collocate( consumer.dispatchQueue )
            }

            // Collocate all the producers with the queue..

            queue.inbound_sessions.foreach { session =>
              session.producer.collocate( queue.dispatchQueue )
            }
          }

        }
      }
      schedualConnectionRegroup
    }
    dispatchQueue.dispatchAfter(1, TimeUnit.SECONDS, ^{ if(serviceState.isStarted) { connectionRegroup } } )
  }

}
