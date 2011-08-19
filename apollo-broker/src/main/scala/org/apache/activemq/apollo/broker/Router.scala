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
import scala.collection.immutable.List
import org.apache.activemq.apollo.dto._
import security.SecurityContext
import store.StoreUOW
import util.continuations._
import java.util.concurrent.atomic.{AtomicReference, AtomicLong}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Router extends Service {

  def virtual_host:VirtualHost

  def get_queue(dto:String):Option[Queue] @suspendable

  def bind(destinations:Array[DestinationDTO], consumer:DeliveryConsumer, security:SecurityContext) : Option[String] @suspendable

  def unbind(destinations:Array[DestinationDTO], consumer:DeliveryConsumer, persistent:Boolean, security:SecurityContext)

  def connect(destinations:Array[DestinationDTO], producer:BindableDeliveryProducer, security:SecurityContext): Option[String] @suspendable

  def disconnect(destinations:Array[DestinationDTO], producer:BindableDeliveryProducer)

  def delete(destinations:Array[DestinationDTO], security:SecurityContext): Option[String] @suspendable

  def create(destinations:Array[DestinationDTO], security:SecurityContext): Option[String] @suspendable

  def apply_update(on_completed:Runnable):Unit
}

/**
 * An object which produces deliveries to which allows new DeliveryConsumer
 * object to bind so they can also receive those deliveries.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait BindableDeliveryProducer extends DeliveryProducer with Retained {

  def dispatch_queue:DispatchQueue

  def bind(targets:List[DeliveryConsumer]):Unit
  def unbind(targets:List[DeliveryConsumer]):Unit

  def connected():Unit
  def disconnected():Unit

}

object DeliveryProducerRoute extends Log

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract class DeliveryProducerRoute(router:Router) extends BaseRetained with BindableDeliveryProducer with Sink[Delivery] {
  import DeliveryProducerRoute._

  var targets = List[DeliverySession]()
  val store = if(router!=null) {
    router.virtual_host.store
  } else {
    null
  }

  def connected() = dispatch_queue {
    on_connected
  }

  def bind(consumers:List[DeliveryConsumer]) = {
    consumers.foreach(_.retain)
    dispatch_queue {
      consumers.foreach{ x=>
        debug("producer route attaching to conusmer.")
        val target = x.connect(this);
        target.refiller = drainer
        targets ::= target
      }
    }
  }

  def unbind(targets:List[DeliveryConsumer]) = dispatch_queue {
    this.targets = this.targets.filterNot { x=>
      val rc = targets.contains(x.consumer)
      if( rc ) {
        debug("producer route detaching from conusmer.")
        if( !overflowSessions.isEmpty ) {
          overflowSessions = overflowSessions.filterNot( _ == x )
          if( overflowSessions.isEmpty ) {
            drainer.run
          }
        }
        x.close
      }
      rc
    }
    targets.foreach(_.release)
  }

  def disconnected() = dispatch_queue {
    this.targets.foreach { x=>
      debug("producer route detaching from conusmer.")
      x.close
    }
  }

  protected def on_connected = {}
  protected def on_disconnected = {}

  //
  // Sink trait implementation.  This Sink overflows
  // by 1 value.  It's only full when overflowed.  It overflows
  // when one of the down stream sinks cannot accept the offered
  // Dispatch.
  //

  var pendingAck: (DeliveryResult, StoreUOW)=>Unit = null
  var overflow:Delivery=null
  var overflowSessions = List[DeliverySession]()
  var refiller:Runnable=null

  def full = overflow!=null

  def offer(delivery:Delivery) = {
    if( full ) {
      false
    } else {

      // Do we need to store the message if we have a matching consumer?
      pendingAck = delivery.ack
      val copy = delivery.copy
      copy.message.retain

      targets.foreach { target=>

        // only deliver to matching consumers
        if( target.consumer.matches(copy) ) {

          if ( target.consumer.is_persistent && copy.message.persistent
                && copy.storeKey == -1L && store != null) {
            if (copy.uow == null) {
              copy.uow = store.create_uow
            } else {
              copy.uow.retain
            }
            copy.storeLocator = new AtomicReference[Array[Byte]]()
            copy.storeKey = copy.uow.store(copy.createMessageRecord)
          }

          if( !target.offer(copy) ) {
            overflowSessions ::= target
          }
        }
      }

      if( overflowSessions!=Nil ) {
        overflow = copy
      } else {
        delivered(copy)
      }
      true
    }
  }

  private def delivered(delivery: Delivery): Unit = {
    if (pendingAck != null) {
      if (delivery.uow != null) {
        val ack = pendingAck
        delivery.uow.on_complete {
          ack(Delivered, null)
        }

      } else {
        pendingAck(Delivered, null)
      }
      pendingAck==null
    }
    if (delivery.uow != null) {
      delivery.uow.release
    }
    delivery.message.release
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
