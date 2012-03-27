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
import java.util.LinkedList
import org.fusesource.hawtdispatch.transport.Transport
import collection.mutable.HashSet

/**
 * <p>
 * Defines a simple trait to control the flow of data
 * between callers and implementations of this trait.
 * <p>
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
abstract class Sink[T] {

  /**
   * @return true if the sink is full
   */
  def full:Boolean

  /**
   * Attempts to add a value to the sink.  If the sink is full,
   * this method will typically return false.  The caller should
   * try to offer the value again once the refiller is exectuted.
   *
   * @return true if the value was added.
   */
  def offer(value:T):Boolean

  /**
   * Sets a refiller on the sink.  The refiller is executed
   * when the sink is interested in receiving more deliveries.
   */
  def refiller:Task
  def refiller_=(value:Task)

  def map[Y](func: Y=>T ):Sink[Y] = new SinkMapper[Y,T] {
    def passing(value: Y) = func(value)
    def downstream = Sink.this
  }

  def flatMap[Y](func: Y=>Option[T]):Sink[Y] = new Sink[Y] with SinkFilter[T] {
    def downstream = Sink.this
    def offer(value:Y) = {
      if( full ) {
        false
      } else {
        val opt = func(value)
        if( opt.isDefined ) {
          downstream.offer(opt.get)
        }
        true
      }
    }
  }

}

trait SinkFilter[T] {
  def downstream:Sink[T]
  def refiller:Task = downstream.refiller
  def refiller_=(value:Task) { downstream.refiller=value }
  def full: Boolean = downstream.full
}

trait SinkMapper[T,X] extends Sink[T] with SinkFilter[X] {
  def offer(value:T) = {
    if( full ) {
      false
    } else {
      downstream.offer(passing(value))
    }
  }
  def passing(value:T):X
}

/**
 * <p>
 * A delivery sink which is connected to a transport. It expects the caller's dispatch
 * queue to be the same as the transport's
 * <p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class TransportSink(val transport:Transport) extends Sink[AnyRef] {
  var refiller:Task = NOOP
  def full:Boolean = transport.full
  def offer(value:AnyRef) =  transport.offer(value)
}

/**
 * Implements a delivery sink which buffers the overflow of deliveries that
 * a 'down stream' sink cannot accept when it's full.  An overflow sink
 * always accepts offers even when it's full.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class OverflowSink[T](val downstream:Sink[T]) extends Sink[T] {

  var refiller:Task = NOOP

  val overflow = new LinkedList[T]()

  def overflowed = !overflow.isEmpty

  def full = overflowed || downstream.full

  def clear = overflow.clear

  downstream.refiller = ^{ drain }

  def removeFirst = {
    if( !overflow.isEmpty ) {
      var rc = overflow.removeFirst()
      onDelivered(rc)
      Some(rc)
    } else {
      None
    }
  }

  def removeLast = {
    if( !overflow.isEmpty ) {
      var rc = overflow.removeLast()
      onDelivered(rc)
      Some(rc)
    } else {
      None
    }
  }

  protected def drain:Unit = {
    while( overflowed ) {
      if( !downstream.offer(overflow.peekFirst()) ) {
        return
      } else {
        onDelivered(overflow.removeFirst())
      }
    }
    // request a refill once the overflow is empty...
    refiller.run
  }

  /**
   * @return true always even when full since those messages just get stored in a
   *         overflow list
   */
  def offer(value:T) = {
    if( overflowed || !downstream.offer(value)) {
      overflow.addLast(value)
    } else {
      onDelivered(value)
    }
    true
  }

  /**
   * Called for each value what is passed on to the down stream sink.
   */
  protected def onDelivered(value:T) = {
  }

}


/**
 * A sink that allows the downstream sink to set to an
 * optional sink.
 */
class MutableSink[T] extends Sink[T] {

  var refiller:Task = NOOP
  private var _downstream:Option[Sink[T]] = None

  def downstream_=(value: Option[Sink[T]]) {
    _downstream.foreach(d => d.refiller = NOOP )
    _downstream = value
    _downstream.foreach{d =>
      d.refiller = refiller
      refiller.run
    }
  }

  def downstream = _downstream

  /**
   * When the downstream sink is not set, then the sink
   * is considered full otherwise it's full if the downstream
   * sink is full.
   */
  def full = _downstream.map(_.full).getOrElse(true)

  /**
   * If the downstream is not set, then this returns false, otherwise
   * it true.
   */
  def offer(value:T) = _downstream.map(x=> x.offer(value)).getOrElse(false)
}


class SinkMux[T](val downstream:Sink[T]) {
  var sinks = HashSet[Sink[T]]()

  downstream.refiller = ^{
    sinks.foreach { sink =>
      sink.refiller.run()
    }
  }

  class ManagedSink extends Sink[T] {

    var rejection_handler:(T)=>Unit = _
    var refiller:Task = NOOP

    def full = downstream.full && rejection_handler==null

    def offer(value: T) = {
      if ( full ) {
        false
      } else {
        if( rejection_handler!=null ) {
          rejection_handler(value)
          true
        } else {
          downstream.offer(value)
        }
      }
    }
  }
  
  def open():Sink[T] = {
    val sink = new ManagedSink()
    sinks += sink
    sink
  }

  def close(sink:Sink[T], rejection_handler:(T)=>Unit):Unit = {
    sink match {
      case sink:ManagedSink =>
        assert(sink.rejection_handler==null)
        sink.rejection_handler = rejection_handler
        sink.refiller.run()
        sinks -= sink
      case _ =>
        sys.error("We did not open that sink")
    }
  }
}

class CreditWindowFilter[T](val downstream:Sink[T], val sizer:Sizer[T]) extends SinkMapper[T,T] {

  var byte_credits = 0
  var delivery_credits = 0
  var disabled = true

  override def full: Boolean = downstream.full || ( disabled && byte_credits <= 0 && delivery_credits <= 0 )

  def disable = {
    disabled = false
    refiller.run()
  }

  def passing(value: T) = {
    byte_credits -= sizer.size(value)
    delivery_credits -= 1
    value
  }

  def credit(byte_credits:Int, delivery_credits:Int) = {
    this.byte_credits += byte_credits
    this.delivery_credits += delivery_credits
    if( !full ) {
      refiller.run()
    }
  }
}

trait SessionSink[T] extends Sink[T] {
  /**
   * The number of elements accepted by this session.
   */
  def enqueue_item_counter:Long

  /**
   * The total size of the elements accepted by this session.
   */
  def enqueue_size_counter:Long

  /**
   * The total size of the elements accepted by this session.
   */
  def enqueue_ts:Long
  /**
   * An estimate of the capacity left in the session before it stops
   * accepting more elements.
   */
  def remaining_capacity:Int
}

trait SessionSinkFilter[T] extends SessionSink[T] with SinkFilter[T] {
  def downstream:SessionSink[T]
  def enqueue_item_counter = downstream.enqueue_item_counter
  def enqueue_size_counter = downstream.enqueue_size_counter
  def enqueue_ts = downstream.enqueue_ts
  def remaining_capacity = downstream.remaining_capacity
}

object SessionSinkMux {
  val default_session_max_credits = System.getProperty("apollo.default_session_max_credits", ""+(1024*32)).toInt
}

/**
 *  <p>
 * A SinkMux multiplexes access to a target sink so that multiple
 * producers can send data to it concurrently.  The SinkMux creates
 * a new session/sink for each connected producer.  The session
 * uses credit based flow control to cut down the cross thread
 * events issued.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class SessionSinkMux[T](val downstream:Sink[T], val consumer_queue:DispatchQueue, val sizer:Sizer[T]) {

  var sessions = HashSet[Session[T]]()

  val overflow = new OverflowSink[(Session[T],T)](downstream.map(_._2)) {
    // Once a value leaves the overflow, then we can credit the
    // session so that more messages can be accepted.
    override protected def onDelivered(event:(Session[T],T)) = {
      val session = event._1
      val value = event._2
      session.credit_adder.merge(sizer.size(value));
    }
  }

  // use a event aggregating source to coalesce multiple events from the same thread.
  // all the sessions send to the same source.
  val source = createSource(new ListEventAggregator[(Session[T],T)](), consumer_queue)
  source.setEventHandler(^{drain_source});
  source.resume

  def drain_source = {
    source.getData.foreach { event =>
      // overflow sinks can always accept more values.
      val f1 = overflow.full
      overflow.offer(event)
    }
  }

  def open(producer_queue:DispatchQueue, credits:Int=SessionSinkMux.default_session_max_credits):SessionSink[T] = {
    val session = new Session[T](producer_queue, 0, this)
    consumer_queue <<| ^{
      session.credit_adder.merge(credits);
      sessions += session
    }
    session
  }

  def close(session:Sink[T], rejection_handler:(T)=>Unit):Unit = {
    consumer_queue <<| ^{
      session match {
        case s:Session[T] =>
          sessions -= s
          s.producer_queue {
            s.close(rejection_handler)
          }
      }
    }
  }

  def time_stamp = 0L
}

/**
 * tracks one producer to consumer session / credit window.
 */
class Session[T](val producer_queue:DispatchQueue, var credits:Int, mux:SessionSinkMux[T]) extends SessionSink[T] {

  var refiller:Task = NOOP

  private def sizer = mux.sizer
  private def downstream = mux.source

  @volatile
  var enqueue_item_counter = 0L
  @volatile
  var enqueue_size_counter = 0L
  @volatile
  var enqueue_ts = mux.time_stamp

  // create a source to coalesce credit events back to the producer side...
  val credit_adder = createSource(EventAggregators.INTEGER_ADD , producer_queue)
  credit_adder.onEvent{
    add_credits(credit_adder.getData.intValue)
  }
  credit_adder.resume

  private var rejection_handler: (T)=>Unit = _
  
  private def add_credits(value:Int) = {
    credits += value;
    if( value > 0 && !_full ) {
      refiller.run
    }
  }

  ///////////////////////////////////////////////////
  // These members are used from the context of the
  // producer serial dispatch queue
  ///////////////////////////////////////////////////

  def remaining_capacity = credits

  override def full = {
    producer_queue.assertExecuting()
    _full
  }
  
  def _full = credits <= 0 && rejection_handler == null

  override def offer(value: T) = {
    producer_queue.assertExecuting()
    if( _full ) {
      false
    } else {
      if( rejection_handler!=null ) {
        rejection_handler(value)
      } else {
        val size = sizer.size(value)
  
        enqueue_item_counter += 1
        enqueue_size_counter += size
        enqueue_ts = mux.time_stamp
  
        add_credits(-size)
        downstream.merge((this, value))
      }
      true
    }
  }

  def close(rejection_handler:(T)=>Unit) = {
    producer_queue.assertExecuting()
    assert(this.rejection_handler==null)
    this.rejection_handler=rejection_handler
    refiller.run
  }

}


/**
 * A sizer can determine the size of other objects.
 */
trait Sizer[T] {
  def size(value:T):Int
}

/**
 * <p>
 * A delivery sink which buffers deliveries sent to it up to it's
 * maxSize settings after which it starts flow controlling the sender.
 * <p>
 *
 * <p>
 * It executes the drainer when it has queued values.  The drainer
 * should call poll to get the queued values and then ack the values
 * once they have been processed to allow additional values to be accepted.
 * The refiller is executed once the the queue is drained.
 * <p>
 *
 * <p>
 * This class should only be called from a single serial dispatch queue.
 * <p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class QueueSink[T](val sizer:Sizer[T], var maxSize:Int=1024*32) extends Sink[T] {

  var refiller:Task = NOOP

  var buffer = new LinkedList[T]()
  private var size = 0

  var drainer: Task = null

  def full = size >= maxSize
  def poll = buffer.poll
  def unpoll(value:T) = buffer.addFirst(value)
  def is_empty = buffer.isEmpty

  private def drain = drainer.run

  def offer(value:T):Boolean = {
    if( full ) {
      false
    } else {
      size += sizer.size(value)
      buffer.addLast(value)
      if( buffer.size == 1 ) {
        drain
      }
      true
    }
  }

  def ack(amount:Int) = {
    // When a message is delivered to the consumer, we release
    // used capacity in the outbound queue, and can drain the inbound
    // queue
    size -= amount
    if( !is_empty ) {
      drain
    } else {
      refiller.run
    }
  }

}
