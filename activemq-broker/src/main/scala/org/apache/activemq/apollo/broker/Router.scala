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
import _root_.org.fusesource.hawtdispatch.ScalaDispatch._

import path.PathMap
import collection.JavaConversions
import org.apache.activemq.apollo.util.LongCounter
import collection.mutable.HashMap

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Domain {
  val TOPIC_DOMAIN = new AsciiBuffer("topic");
  val QUEUE_DOMAIN = new AsciiBuffer("queue");
  val TEMP_TOPIC_DOMAIN = new AsciiBuffer("temp-topic");
  val TEMP_QUEUE_DOMAIN = new AsciiBuffer("temp-queue");
}

import Domain._
/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Domain {

  val targets = new PathMap[DeliveryConsumer]();

  def bind(name:AsciiBuffer, queue:DeliveryConsumer) = {
    targets.put(name, queue);
  }

  def unbind(name:AsciiBuffer, queue:DeliveryConsumer) = {
    targets.remove(name, queue);
  }

//
//  synchronized public Collection<DeliveryTarget> route(AsciiBuffer name, MessageDelivery delivery) {
//    return targets.get(name);
//  }

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Router extends Log {
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

  val destination_id_counter = new LongCounter

  override protected def log = Router
  protected def dispatchQueue:DispatchQueue = host.dispatchQueue

  trait DestinationNode {
    val destination:Destination
    val id = destination_id_counter.incrementAndGet
    var targets = List[DeliveryConsumer]()
    var routes = List[DeliveryProducerRoute]()

    def on_bind(x:List[DeliveryConsumer]):Unit
    def on_unbind(x:List[DeliveryConsumer]):Boolean
    def on_connect(route:DeliveryProducerRoute):Unit
    def on_disconnect(route:DeliveryProducerRoute):Boolean = {
      routes = routes.filterNot({r=> route==r})
      route.disconnected()
      routes == Nil && targets == Nil
    }
  }

  class TopicDestinationNode(val destination:Destination) extends DestinationNode {
    def on_bind(x:List[DeliveryConsumer]) =  {
      targets = x ::: targets
      routes.foreach({r=>
        r.bind(x)
      })
    }

    def on_unbind(x:List[DeliveryConsumer]):Boolean = {
      targets = targets.filterNot({t=>x.contains(t)})
      routes.foreach({r=>
        r.unbind(x)
      })
      routes == Nil && targets == Nil
    }

    def on_connect(route:DeliveryProducerRoute) = {
      routes = route :: routes
      route.connected(targets)
    }
  }

  class QueueDestinationNode(val destination:Destination) extends DestinationNode {
    var queue:Queue = null

    // once the queue is created.. connect it up with the producers and targets.
    host.getQueue(destination) { q =>
      dispatchQueue {
        queue = q;
        queue.bind(targets)
        routes.foreach({route=>
          route.connected(queue :: Nil)
        })
      }
    }

    def on_bind(x:List[DeliveryConsumer]) =  {
      targets = x ::: targets
      if( queue!=null ) {
        queue.bind(x)
      }
    }

    def on_unbind(x:List[DeliveryConsumer]):Boolean = {
      targets = targets.filterNot({t=>x.contains(t)})
      if( queue!=null ) {
        queue.unbind(x)
      }
      routes == Nil && targets == Nil
    }

    def on_connect(route:DeliveryProducerRoute) = {
      routes = route :: routes
      if( queue!=null ) {
        route.connected(queue :: Nil)
      }
    }
  }

  var destinations = new HashMap[Destination, DestinationNode]()

  private def get(destination:Destination):DestinationNode = {
    destinations.getOrElseUpdate(destination,
      if( isTopic(destination) ) {
        new TopicDestinationNode(destination)
      } else {
        new QueueDestinationNode(destination)
      }
    )
  }

  def bind(destination:Destination, targets:List[DeliveryConsumer]) = retaining(targets) {
      get(destination).on_bind(targets)
    } >>: dispatchQueue

  def unbind(destination:Destination, targets:List[DeliveryConsumer]) = releasing(targets) {
      if( get(destination).on_unbind(targets) ) {
        destinations.remove(destination)
      }
    } >>: dispatchQueue

  def connect(destination:Destination, producer:DeliveryProducer)(completed: (DeliveryProducerRoute)=>Unit) = {
    val route = new DeliveryProducerRoute(this, destination, producer) {
      override def on_connected = {
        completed(this);
      }
    }
    ^ {
      get(destination).on_connect(route)
    } >>: dispatchQueue
  }

  def isTopic(destination:Destination) = destination.getDomain == TOPIC_DOMAIN
  def isQueue(destination:Destination) = !isTopic(destination)

  def disconnect(route:DeliveryProducerRoute) = releasing(route) {
      get(route.destination).on_disconnect(route)
    } >>: dispatchQueue


   def each(proc:(Destination, DestinationNode)=>Unit) = dispatchQueue {
     import JavaConversions._
     for( (destination, node) <- destinations ) {
        proc(destination, node)
     }
   } 

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Route extends Retained {

  def destination:Destination
  def dispatchQueue:DispatchQueue
  val metric = new AtomicLong();

  def connected(targets:List[DeliveryConsumer]):Unit
  def bind(targets:List[DeliveryConsumer]):Unit
  def unbind(targets:List[DeliveryConsumer]):Unit
  def disconnected():Unit

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DeliveryProducerRoute(val router:Router, val destination:Destination, val producer:DeliveryProducer) extends BaseRetained with Route with Sink[Delivery] with DispatchLogging {

  override protected def log = Router
  override def dispatchQueue = producer.dispatchQueue

  // Retain the queue while we are retained.
  dispatchQueue.retain
  setDisposer(^{
    dispatchQueue.release
  })

  var targets = List[DeliverySession]()

  def connected(targets:List[DeliveryConsumer]) = retaining(targets) {
    internal_bind(targets)
    on_connected
  } >>: dispatchQueue

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
        x.close
      }
      rc
    }
  } >>: dispatchQueue

  def disconnected() = ^ {
    this.targets.foreach { x=>
      debug("producer route detaching from conusmer.")
      x.close
      x.consumer.release
    }    
  } >>: dispatchQueue

  protected def on_connected = {}
  protected def on_disconnected = {}

  //
  // Sink trait implementation.  This Sink overflows
  // by 1 value.  It's only full when overflowed.  It overflows
  // when one of the down stream sinks cannot accept the offered
  // Dispatch.
  //

  var overflow:Delivery=null
  var overflowSessions = List[DeliverySession]()
  var refiller:Runnable=null

  def full = overflow!=null

  def offer(delivery:Delivery) = {
    if( full ) {
      false
    } else {

      // Do we need to store the message if we have a matching consumer?
      var storeOnMatch = delivery.message.persistent && router.host.store!=null

      targets.foreach { target=>

        // only delivery to matching consumers
        if( target.consumer.matches(delivery) ) {
          
          if( storeOnMatch ) {
            delivery.uow = router.host.store.createStoreUOW
            delivery.storeKey = delivery.uow.store(delivery.createMessageRecord)
            storeOnMatch = false
          }

          if( !target.offer(delivery) ) {
            overflowSessions ::= target
          }
        }
      }

      if( overflowSessions!=Nil ) {
        overflow = delivery
      } else {
        delivered(delivery)
      }
      true
    }
  }

  private def delivered(delivery: Delivery): Unit = {
    if (delivery.ack != null) {
      if (delivery.uow != null) {
        delivery.uow.setDisposer(^ {delivery.ack(null)})
      } else {
        delivery.ack(null)
      }
    }
    if (delivery.uow != null) {
      delivery.uow.release
    }
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
