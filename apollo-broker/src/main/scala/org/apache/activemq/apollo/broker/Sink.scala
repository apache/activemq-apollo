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
import collection.mutable.{ListBuffer, HashSet}
import org.apache.activemq.apollo.util.list.{LinkedNodeList, LinkedNode}
import java.util.concurrent.TimeUnit

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
    override def toString: String = downstream.toString
  }

  def flatMap[Y](func: Y=>Option[T]):Sink[Y] = new Sink[Y] with SinkFilter[T] {

    override def toString: String = downstream.toString

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

abstract class AbstractSinkFilter[Y, T <: Object] extends Sink[Y] {
  def downstream:Sink[T]
  def refiller:Task = downstream.refiller
  def refiller_=(value:Task) { downstream.refiller=value }
  def full: Boolean = downstream.full

  def offer(value:Y) = {
    if( full ) {
      false
    } else {
      val opt = filter(value)
      if( opt !=null ) {
        downstream.offer(opt)
      }
      true
    }
  }
  def filter(value:Y):T
}

case class FullSink[T]() extends Sink[T] {
  def refiller:Task = null
  def refiller_=(value:Task) = {}
  def full = true
  def offer(value: T) = false
}

case class BlackHoleSink[T]() extends Sink[T] {
  var refiller:Task = null
  def full = false
  def offer(value: T) = true
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
      val accepted:Boolean = downstream.offer(passing(value))
      assert(accepted, "The downstream sink violated it's contract, an offer was not accepted but it had told us it was not full")
      accepted
    }
  }
  def passing(value:T):X
}

abstract class AbstractSinkMapper[T,X] extends Sink[T] with SinkFilter[X] {
  def offer(value:T) = {
    if( full ) {
      false
    } else {
      val accepted:Boolean = downstream.offer(passing(value))
      assert(accepted, "The downstream sink violated it's contract, an offer was not accepted but it had told us it was not full")
      accepted
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
  override def toString: String = "TransportSink(full:"+full+")"
}

/**
 * Implements a delivery sink which buffers the overflow of deliveries that
 * a 'down stream' sink cannot accept when it's full.  An overflow sink
 * always accepts offers even when it's full.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class OverflowSink[T](val downstream:Sink[T]) extends AbstractOverflowSink[T] {
  override def toString: String = {
    "OverflowSink("+
      super.toString+
      ", "+downstream+
    ")"
  }
}

abstract class AbstractOverflowSink[T] extends Sink[T] {

  def downstream:Sink[T]

  var refiller:Task = NOOP

  val overflow = new LinkedList[T]()

  override def toString = {
    "overflow: "+overflow
    ", full: "+full
  }

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


    override def toString: String = {
      "ManagedSink("+
        "rejection_handler: "+(rejection_handler)+
        ", full: "+(full)+
        ", "+downstream+
      ")"
    }

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

  var delivery_credits = 0
  var byte_credits = 0
  var enabled = true


  override def toString: String = {
    "CreditWindowFilter("+
     "enabled:"+enabled+
      ", delivery_credits:"+delivery_credits+
      ", byte_credits:"+byte_credits+
      ", full:"+full+
      ", "+downstream+
    ")"
  }

  override def full: Boolean = downstream.full || ( enabled && byte_credits <= 0 && delivery_credits <= 0 )

  def disable = {
    enabled = false
    refiller.run()
  }

  def passing(value: T) = {
    delivery_credits -= 1
    byte_credits -= sizer.size(value)
    value
  }

  def credit(delivery_credits:Int, byte_credits:Int) = {
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
  def downstream: SessionSink[T]
  def enqueue_item_counter = downstream.enqueue_item_counter
  def enqueue_size_counter = downstream.enqueue_size_counter
  def enqueue_ts = downstream.enqueue_ts
  def remaining_capacity = downstream.remaining_capacity
}

abstract class AbstractSessionSinkFilter[T] extends SessionSink[T] with SinkFilter[T] {
  def downstream:Sink[T] = downstream_session_sink
  def downstream_session_sink: SessionSink[T]
  def enqueue_item_counter = downstream_session_sink.enqueue_item_counter
  def enqueue_size_counter = downstream_session_sink.enqueue_size_counter
  def enqueue_ts = downstream_session_sink.enqueue_ts
  def remaining_capacity = downstream_session_sink.remaining_capacity
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
class SessionSinkMux[T](val downstream:Sink[(Session[T], T)], val consumer_queue:DispatchQueue, val sizer:Sizer[T], var delivery_credits:Int, var size_credits:Int) {

  var sessions = HashSet[Session[T]]()
  var overflowed_sessions = new LinkedNodeList[SessionLinkedNode[T]]()

  override def toString: String = {
    "SessionSinkMux(" +
      "sessions: "+sessions.size+
      ", overflowed_sessions: "+overflowed_sessions.size+
      ", "+downstream+
    ")"
  }

  def open(producer_queue:DispatchQueue):SessionSink[T] = {
    val session = new Session[T](this, producer_queue)
    consumer_queue <<| ^{
      sessions += session
      val bonus = size_credits / sessions.size
      session.size_bonus = bonus
      session.credit(delivery_credits, 1+bonus);
      schedual_rebalance
    }
    session
  }

  def close(session:Sink[T], rejection_handler:(T)=>Unit):Unit = {
    consumer_queue <<| ^{
      session match {
        case s:Session[T] =>
          sessions -= s
          s.close(rejection_handler)
      }
    }
  }

  def resize(new_delivery_credits:Int, new_size_credits:Int) = consumer_queue {
    val delivery_credits_change = new_delivery_credits - delivery_credits
    if ( delivery_credits_change!=0 ) {
      for ( session <- sessions ) {
        session.credit(delivery_credits_change, 0);
      }
    }
    this.delivery_credits = new_delivery_credits
    this.size_credits = new_size_credits
    schedual_rebalance
  }

  def time_stamp = 0L

  var rebalance_schedualed = false
  def schedual_rebalance:Unit = {
    if ( !rebalance_schedualed ) {
      rebalance_schedualed  = true
      consumer_queue.after(550, TimeUnit.MILLISECONDS) {
        rebalance_schedualed = false
        rebalance_check
      }
    }
  }

  var last_rebalance_ts = time_stamp
  def rebalance_check:Unit = {
    // only re-balance periodically since it's a bit expensive.
    var now = time_stamp
    if ( (now - last_rebalance_ts) > 500 && sessions.size>0) {
      last_rebalance_ts = now
      rebalance
    }
  }

  def rebalance = {
    var stalled_sessions = List[Session[T]]()
    var total_stalls = 0L
    for ( session <- sessions ) {
      if ( session.stall_counter > 0) {
        total_stalls += session.stall_counter
        stalled_sessions ::= session
      }
    }

    for ( session <- stalled_sessions ) {
      var slice_percent = session.stall_counter.toFloat / total_stalls
      val new_size_bonus = (size_credits * slice_percent).toInt
      val change = new_size_bonus - session.size_bonus
      session.stall_counter = 0
      session.size_bonus += change
      session.credit(0, change)
    }
  }
  downstream.refiller = ^{ drain_overflow }

  def drain_overflow:Unit = {
    while( !overflowed_sessions.isEmpty) {
      if( !downstream.full ) {
        val accepted = downstream.offer(poll)
        assert(accepted)
      } else {
        return
      }
    }
  }

  def poll:(Session[T], T) = {
    consumer_queue.assertExecuting()
    if(overflowed_sessions.isEmpty) {
      null
    } else {
      val session = overflowed_sessions.getHead.session
      val value = session.overflow.removeFirst()
      if( session.stall_counter > 0 ) {
        schedual_rebalance
      }
      if( session.overflow.isEmpty ) {
        session.overflow_node.unlink()
      } else {
        // to fairly consume values from all sessions.
        overflowed_sessions.rotate()
      }
      (session, value)
    }
  }

  def delivered(session:Session[Delivery], size:Int) = {
    session.credit(1, size)
  }

}

case class SessionLinkedNode[T](session:Session[T]) extends LinkedNode[SessionLinkedNode[T]]

/**
 * tracks one producer to consumer session / credit window.
 */
class Session[T](mux:SessionSinkMux[T], val producer_queue:DispatchQueue) extends SessionSink[T] {

  // the following Session fields are mutated from the consumer dispatch queue...
  // we should think about field padding this object to avoid false sharing on the cache lines.
  val overflow = new LinkedList[T]()
  val overflow_node = SessionLinkedNode[T](Session.this)
  var stall_counter = 0
  var size_bonus = 0

  // use a event aggregating source to coalesce multiple events from the same thread.
  val source = createSource(new ListEventAggregator[(T, Boolean)](), mux.consumer_queue)
  source.setEventHandler(^{
    for( (value, stalled) <- source.getData ) {
      if( overflow.isEmpty ) {
        mux.overflowed_sessions.addLast(overflow_node);
      }
      overflow.add(value)
      if (stalled) {
        stall_counter += 1
      }
    }
    mux.drain_overflow
  });
  source.resume

  // the rest of the Session fields are mutated from the producer dispatch queue...
  var refiller:Task = NOOP
  var rejection_handler: (T)=>Unit = _

  private def sizer = mux.sizer
  var delivery_credits = 0
  var size_credits = 0

  @volatile
  var enqueue_item_counter = 0L
  @volatile
  var enqueue_size_counter = 0L
  @volatile
  var enqueue_ts = mux.time_stamp


  override def toString: String = {
    "Session("+
      "enqueue_item_counter:"+enqueue_item_counter+
      ", enqueue_size_counter:"+enqueue_size_counter+
      ", delivery_credits:"+delivery_credits+
      ", size_credits:"+size_credits+
      ", overflow:"+overflow.size()+
      ", stall_counter:"+stall_counter+
      ", size_bonus:"+size_bonus +
      ", full:"+full+
    ")"
  }

  def credit(delivery_credits:Int, size_credits:Int) = {
    if( delivery_credits!=0 || size_credits!=0 ) {
      credit_adder.merge((delivery_credits, size_credits))
    }
  }

  // create a source to coalesce credit events back to the producer side...
  val credit_adder = createSource(new EventAggregator[(Int, Int), (Int, Int)] {
    def mergeEvent(previous:(Int, Int), event:(Int, Int)) = {
      if( previous == null ) {
        event
      } else {
        mergeEvents(previous, event)
      }
    }
    def mergeEvents(previous:(Int, Int), event:(Int, Int)) = (previous._1+event._1, previous._2+event._2)
  }, producer_queue)

  credit_adder.onEvent {
    val (count, size) = credit_adder.getData
    add_credits(count, size)
    if( (size > 0 || count>0) && !_full ) {
      refiller.run
    }
  }
  credit_adder.resume


  private def add_credits(count:Int, size:Int) = {
    delivery_credits += count
    size_credits += size
  }

  ///////////////////////////////////////////////////
  // These members are used from the context of the
  // producer serial dispatch queue
  ///////////////////////////////////////////////////

  def remaining_capacity = size_credits

  override def full = {
    producer_queue.assertExecuting()
    _full
  }
  
  def _full = ( size_credits <= 0 || delivery_credits<=0 ) && rejection_handler == null

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
  
        add_credits(-1, -size)
        val stalled = size_credits <= 0 || delivery_credits<=0
        source.merge((value, stalled))
      }
      true
    }
  }

  def close(rejection_handler:(T)=>Unit) = {
    producer_queue {
      this.rejection_handler=rejection_handler
      refiller.run
    }
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
