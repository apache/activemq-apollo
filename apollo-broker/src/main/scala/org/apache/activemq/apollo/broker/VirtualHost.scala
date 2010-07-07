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
import _root_.org.fusesource.hawtdispatch.{ScalaDispatch, DispatchQueue}
import _root_.scala.collection.JavaConversions._
import path.PathFilter
import org.fusesource.hawtbuf.AsciiBuffer
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._

import org.apache.activemq.apollo.dto.{VirtualHostDTO}
import java.util.concurrent.TimeUnit
import org.apache.activemq.apollo.store.{Store, StoreFactory, QueueRecord}
import org.apache.activemq.apollo.util._
import ReporterLevel._

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
  override val dispatchQueue:DispatchQueue = ScalaDispatch.createQueue("virtual-host");

  var config:VirtualHostDTO = _
  val queues = new HashMap[AsciiBuffer, Queue]()
  val durableSubs = new HashMap[String, DurableSubscription]()
  val router = new Router(this)

  var names:List[String] = Nil;
  def setNamesArray( names:ArrayList[String]) = {
    this.names = names.toList
  }

  var store:Store = null
  var direct_buffer_pool:DirectBufferPool = null
  var transactionManager:TransactionManagerX = new TransactionManagerX
  val queue_id_counter = new LongCounter

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
                store.getQueueStatus(queueKey) { x =>
                  x match {
                    case Some(info)=>

                    dispatchQueue ^{
                      val dest = DestinationParser.parse(info.record.name, destination_parser_options)
                      val queue = new Queue(this, dest, queueKey)
                      queue.start
                      queues.put(dest.getName, queue)
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

//    TODO:
//      val tmp = new ArrayList[Queue](queues.values())
//      for (queue <-  tmp) {
//        queue.shutdown
//      }

// TODO:
//        ArrayList<IQueue<Long, MessageDelivery>> durableQueues = new ArrayList<IQueue<Long,MessageDelivery>>(queueStore.getDurableQueues());
//        done = new RunnableCountDownLatch(durableQueues.size());
//        for (IQueue<Long, MessageDelivery> queue : durableQueues) {
//            queue.shutdown(done);
//        }
//        done.await();

    if( direct_buffer_pool!=null ) {
      direct_buffer_pool.stop
      direct_buffer_pool = null
    }

    if( store!=null ) {
      store.stop(onCompleted);
    } else {
      onCompleted.run
    }
  }

  def getQueue(destination:Destination)(cb: (Queue)=>Unit ) = ^{
    if( !serviceState.isStarted ) {
      error("getQueue can only be called while the service is running.")
      cb(null)
    } else {
      var queue = queues.get(destination.getName);
      if( queue==null && config.auto_create_queues ) {
        addQueue(destination)(cb)
      } else  {
        cb(queue)
      }
    }
  } |>>: dispatchQueue



  // Try to periodically re-balance connections so that consumers/producers
  // are grouped onto the same thread.
  def schedualConnectionRegroup:Unit = {
    def connectionRegroup = {
      router.each { (destination, node)=>
        node match {
          case x:router.TopicDestinationNode=>

            // 1->1 is the easy case...
            if( node.targets.size==1 && node.routes.size==1 ) {
              // move the producer to the consumer thread.
              node.routes.head.producer.collocate( node.targets.head.dispatchQueue )
            } else {
              // we need to get fancy perhaps look at rates
              // to figure out how to be group the connections.
            }

          case x:router.QueueDestinationNode=>

            if( node.targets.size==1 ) {
              // move the queue to the consumer
              x.queue.collocate( node.targets.head.dispatchQueue )
            } else {
              // we need to get fancy perhaps look at rates
              // to figure out how to be group the connections.
            }

            if( node.routes.size==1 ) {
              // move the producer to the queue.
              node.routes.head.producer.collocate( x.queue.dispatchQueue )
            } else {
              // we need to get fancy perhaps look at rates
              // to figure out how to be group the connections.
            }
        }
      }
      schedualConnectionRegroup
    }
    dispatchQueue.dispatchAfter(1, TimeUnit.SECONDS, ^{ if(serviceState.isStarted) { connectionRegroup } } )
  }


  def addQueue(dest:Destination)(cb: (Queue)=>Unit ) = ^{
    val name = DestinationParser.toBuffer(dest, destination_parser_options)
    if( store!=null ) {
      val record = new QueueRecord
      record.name = name
      record.key = queue_id_counter.incrementAndGet

      store.addQueue(record) { rc =>
        rc match {
          case true =>
            dispatchQueue {
              val queue = new Queue(this, dest, record.key)
              queue.start()
              queues.put(dest.getName, queue)
              cb(queue)
            }
          case false => // store could not create
            cb(null)
        }
      }
    } else {
      val queue = new Queue(this, dest, queue_id_counter.incrementAndGet)
      queue.start()
      queues.put(dest.getName, queue)
      cb(queue)
    }

  } |>>: dispatchQueue

  def createSubscription(consumer:ConsumerContext):BrokerSubscription = {
      createSubscription(consumer, consumer.getDestination());
  }

  def createSubscription(consumer:ConsumerContext, destination:Destination):BrokerSubscription = {

      // First handle composite destinations..
      var destinations = destination.getDestinations();
      if (destinations != null) {
          var subs :List[BrokerSubscription] = Nil
          for (childDest <- destinations) {
              subs ::= createSubscription(consumer, childDest);
          }
          return new CompositeSubscription(destination, subs);
      }

      // If it's a Topic...
//      if ( destination.getDomain == TOPIC_DOMAIN || destination.getDomain == TEMP_TOPIC_DOMAIN ) {
//
//          // It might be a durable subscription on the topic
//          if (consumer.isDurable()) {
//              var dsub = durableSubs.get(consumer.getSubscriptionName());
//              if (dsub == null) {
////                    TODO:
////                    IQueue<Long, MessageDelivery> queue = queueStore.createDurableQueue(consumer.getSubscriptionName());
////                    queue.start();
////                    dsub = new DurableSubscription(this, destination, consumer.getSelectorExpression(), queue);
////                    durableSubs.put(consumer.getSubscriptionName(), dsub);
//              }
//              return dsub;
//          }
//
//          // return a standard subscription
////            TODO:
////            return new TopicSubscription(this, destination, consumer.getSelectorExpression());
//          return null;
//      }

      // It looks like a wild card subscription on a queue..
      if (PathFilter.containsWildCards(destination.getName())) {
          return new WildcardQueueSubscription(this, destination, consumer);
      }

      // It has to be a Queue subscription then..
      var queue = queues.get(destination.getName());
      if (queue == null) {
          if (consumer.autoCreateDestination()) {
//            TODO
//              queue = createQueue(destination);
          } else {
              throw new IllegalStateException("The queue does not exist: " + destination.getName());
          }
      }
//        TODO:
//        return new Queue.QueueSubscription(queue);
      return null;
  }


  val queueLifecyleListeners = new ArrayList[QueueLifecyleListener]();

  def addDestinationLifecyleListener(listener:QueueLifecyleListener):Unit= {
      queueLifecyleListeners.add(listener);
  }

  def removeDestinationLifecyleListener(listener:QueueLifecyleListener):Unit= {
      queueLifecyleListeners.add(listener);
  }
}
