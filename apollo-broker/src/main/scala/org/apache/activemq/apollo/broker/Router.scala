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
import path._
import scala.collection.immutable.List
import security.SecurityContext
import store.StoreUOW
import java.util.concurrent.atomic.AtomicReference
import collection.mutable.ListBuffer
import java.util.regex.Pattern
import java.lang.String

object DestinationAddress {
  
  def encode_path(path:Path) = {
    val rc = new StringBuilder
    var first = true
    for (p <- path.parts) {
      if ( !first ) {
        rc.append(".")
      }
      first = false
      p match {
        case RootPart =>
        case AnyChildPart => rc.append("*")
        case AnyDescendantPart => rc.append("**")
        case p:RegexChildPart => rc.append("*"+escape(p.regex.pattern()))
        case p:LiteralPart => rc.append(escape(p.value))
      }
    }
    rc.toString
  }

  val DOT_PATTERN = Pattern.compile("\\.");

  def decode_path(value:String) = {
    val rc = ListBuffer[Part]()
    for (p <- DOT_PATTERN.split(value)) {
      rc += (if( p startsWith "*" ) {
        if( p.length()==1 ) {
          AnyChildPart
        } else if ( p=="**" ) {
          AnyDescendantPart
        } else {
          val regex_text = unescape(p.substring(1))
          RegexChildPart(Pattern.compile(regex_text))
        }
      } else {
        LiteralPart(unescape(p))
      })
    }
    new Path(rc.toList)
  }

  
  def escape(value:String) = {
    val rc = new StringBuffer(value.length())
    var i=0;
    while( i < value.length() ) {
      val c = value.charAt(i);
      if ( c== '\\' ) {
        rc.append("\\\\")
      }  else if( c == '\n' ) {
        rc.append("\\\n")
      }  else if( c == '\r' ) {
        rc.append("\\\r")
      }  else if( c == '\t' ) {
        rc.append("\\\t")
      }  else if( c == '\b' ) {
        rc.append("\\\b")
      }  else if( c == '*' ) {
        rc.append("\\w")
      }  else if( c == '.' ) {
        rc.append("\\d")
      } else if  ( c < ' ' || c > '~' ) {
        rc.append("\\u%04x".format(c.toInt))
      } else {
        rc.append(c)
      }
      i+=1
    }
    rc.toString
  }
  
  def unescape(value:String) = {
    val rc = new StringBuffer(value.length())
    var i=0
    while( i < value.length() ) {
      val c = value.charAt(i);
      if( c == '\\') {
        i+=1
        val c2 = value.charAt(i);
        rc.append(c2 match {
          case '\\' => '\\'
          case 'n' => '\n'
          case 'r' => '\r'
          case 't' => '\t'
          case 'b' => '\b'
          case 'w' => '*'
          case 'd' => '.'
          case 'u' => 
            i+=1
            val rc = Integer.parseInt(value.substring(i, i+4), 16).toChar
            i+=4
            rc
        })
      } else {
        rc.append(c)
      }      
      i+=1
    }
    rc.toString
  }

}
sealed trait DestinationAddress {
  def domain:String
  def path:Path
  def simple:SimpleAddress = SimpleAddress(domain, path)
  val id = DestinationAddress.encode_path(path)
  override def toString: String =  domain+":"+id
}
sealed trait ConnectAddress extends DestinationAddress
sealed trait BindAddress extends DestinationAddress
object SimpleAddress {
  def apply(value:String):SimpleAddress= {
    val p = value.indexOf(":")
    SimpleAddress(value.substring(0, p), DestinationAddress.decode_path(value.substring(p+1)))
  }
}
case class SimpleAddress(val domain:String, val path:Path) extends ConnectAddress with BindAddress {
  override def simple = this
}
case class SubscriptionAddress(val path:Path, val selector:String, topics:Array[_ <: BindAddress]) extends BindAddress {
  def domain = "dsub"
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Router extends Service {

  def virtual_host:VirtualHost

  def get_queue(dto:Long):Option[Queue]

  def bind(destinations:Array[_ <: BindAddress], consumer:DeliveryConsumer, security:SecurityContext)(cb: (Option[String])=>Unit)

  def unbind(destinations:Array[_ <: BindAddress], consumer:DeliveryConsumer, persistent:Boolean, security:SecurityContext)

  def connect(destinations:Array[_ <: ConnectAddress], producer:BindableDeliveryProducer, security:SecurityContext): Option[String]

  def disconnect(destinations:Array[_ <: ConnectAddress], producer:BindableDeliveryProducer)

  def delete(destinations:Array[_ <: DestinationAddress], security:SecurityContext): Option[String]

  def create(destinations:Array[_ <: DestinationAddress], security:SecurityContext): Option[String]

  def apply_update(on_completed:Task):Unit

  def remove_temp_destinations(active_connections:scala.collection.Set[String]):Unit
}

/**
 * An object which produces deliveries to which allows new DeliveryConsumer
 * object to bind so they can also receive those deliveries.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait BindableDeliveryProducer extends DeliveryProducer with Retained {

  def dispatch_queue:DispatchQueue

  def bind(targets:List[DeliveryConsumer], on_bind:()=>Unit):Unit
  def unbind(targets:List[DeliveryConsumer]):Unit

  def connected():Unit
  def disconnected():Unit

}

object DeliveryProducerRoute extends Log

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract class DeliveryProducerRoute(router:Router) extends AbstractOverflowSink[Delivery] with BindableDeliveryProducer with DeferringDispatched {
  import DeliveryProducerRoute._

  var last_send = Broker.now

  val reained_base = new BaseRetained
  def release = reained_base.release
  def retain = reained_base.retain
  def retained = reained_base.retained

  var targets = List[DeliverySession]()
  val store = if(router!=null) {
    router.virtual_host.store
  } else {
    null
  }
  var is_connected = false

  def connected() = defer {
    is_connected = true
    if( dispatch_delivery!=null ) {
      val t = dispatch_delivery
      dispatch_delivery = null
      _offer(t)
      if( downstream.refiller!=null && !full ) {
        downstream.refiller.run()
      }
    }
    on_connected
  }

  def bind(consumers:List[DeliveryConsumer], on_bind:()=>Unit) = {
    consumers.foreach(_.retain)
    dispatch_queue {
      consumers.foreach{ x=>
        debug("producer route attaching to consumer.")
        val target = connect(x);
        target.refiller = drainer
        targets ::= target
      }
      on_bind();
    }
  }

  def connect(x:DeliveryConsumer) = x.connect(this)

  def unbind(targets:List[DeliveryConsumer]) = defer {
    this.targets = this.targets.filterNot { x=>
      val rc = targets.contains(x.consumer)
      if( rc ) {
        debug("producer route detaching from consumer.")
        if( !dispatch_sessions.isEmpty ) {
          dispatch_sessions = dispatch_sessions.filterNot{ session =>
            if( session == x ) {
              dispatch_delivery.ack(Undelivered, null)
            }
            session == x
          }
          if( dispatch_sessions.isEmpty ) {
            drainer.run
          }
        }
        x.close
      }
      rc
    }
    targets.foreach(_.release)
  }

  def disconnected() = defer {
    this.targets.foreach { x=>
      debug("producer route detaching from consumer.")
      x.close
    }
    is_connected = false
  }

  protected def on_connected = {}
  protected def on_disconnected = {}

  //
  // Sink trait implementation.  This Sink overflows
  // by 1 value.  It's only full when overflowed.  It overflows
  // when one of the down stream sinks cannot accept the offered
  // Dispatch.
  //
  var dispatch_delivery:Delivery=null
  var dispatch_sessions = List[DeliverySession]()

  // This the sink that the overflow goes to.

  object downstream extends Sink[Delivery] {
    var refiller:Task=null
    def full = dispatch_delivery!=null

    def offer(delivery:Delivery):Boolean = {
      if( full ) {
        false
      } else {
        if ( !is_connected ) {
          dispatch_delivery = delivery
        } else {
          _offer(delivery)
        }
        return true
      }
    }
  }

  override def offer(delivery: Delivery): Boolean = {
    dispatch_queue.assertExecuting()
    if (delivery.uow != null) {
      delivery.uow.retain
    }
    super.offer(delivery)
  }

  private def _offer(delivery:Delivery):Boolean = {
    last_send = Broker.now

    // Do we need to store the message if we have a matching consumer?
    var matching_targets = 0
    val original_ack = delivery.ack
    val copy = delivery.copy
    copy.uow = delivery.uow

    if ( original_ack!=null ) {
      copy.ack = (result, uow)=> {
        defer {
          matching_targets -= 1
          if ( matching_targets<= 0 && copy.ack!=null ) {
            copy.ack = null
            if (delivery.uow != null) {
              delivery.uow.on_complete {
                defer {
                  original_ack(Consumed, null)
                }
              }
            } else {
              original_ack(Consumed, null)
            }
          }
        }
      }
    }

    if(copy.message!=null) {
      copy.message.retain
    }

    targets.foreach { target=>

      // only deliver to matching consumers
      if( target.consumer.matches(copy) ) {
        matching_targets += 1
        if ( target.consumer.is_persistent && copy.persistent && store != null) {

          if (copy.uow == null) {
            copy.uow = store.create_uow
          }

          if( copy.storeKey == -1L ) {
            copy.storeLocator = new AtomicReference[Object]()
            copy.storeKey = copy.uow.store(copy.createMessageRecord)
          }
        }

        if( !target.offer(copy) ) {
          dispatch_sessions ::= target
        }
      }
    }

    if ( matching_targets == 0 && original_ack!=null ) {
      original_ack(Consumed, null)
    }

    if( dispatch_sessions!=Nil ) {
      dispatch_delivery = copy
    } else {
      release(copy)
    }
    true
  }


  private def release(delivery: Delivery): Unit = {
    if (delivery.uow != null) {
      delivery.uow.release
    }
    if( delivery.message!=null ) {
      delivery.message.release
    }
  }

  val drainer = ^{
    dispatch_queue.assertExecuting()
    if( is_connected ) {
      if( dispatch_delivery!=null ) {
        val original = dispatch_sessions;
        dispatch_sessions = Nil
        original.foreach { target=>
          if( !target.offer(dispatch_delivery) ) {
            dispatch_sessions ::= target
          }
        }
        if( dispatch_sessions==Nil ) {
          release(dispatch_delivery)
          dispatch_delivery = null
          if(downstream.refiller!=null)
            downstream.refiller.run
        }
      } else if(downstream.refiller!=null) {
        downstream.refiller.run
      }
    }
  }

  override def toString = {
    "last_send: "+last_send+
    ", retained: "+reained_base.retained()+
    ", is_connected: "+is_connected+
    ", dispatch_delivery: "+dispatch_delivery+
    ", dispatch_sessions: "+dispatch_sessions+
    ", "+super.toString +
    ", targets: "+targets
  }

}
