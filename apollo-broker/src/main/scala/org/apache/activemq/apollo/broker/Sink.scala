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

import _root_.org.fusesource.hawtdispatch._
import org.fusesource.hawtdispatch._
import java.util.{LinkedList}
import org.apache.activemq.apollo.transport.Transport

/**
 * <p>
 * Defines a simple trait to control the flow of data
 * between callers and implementations of this trait.
 * <p>
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Sink[T] {

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
  var refiller:Runnable
}

/**
 * <p>
 * A delivery sink which is connected to a transport. It expects the caller's dispatch
 * queue to be the same as the transport's/
 * <p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class TransportSink(val transport:Transport) extends Sink[AnyRef] {
  def full:Boolean = transport.full
  def offer(value:AnyRef) =  transport.offer(value)
  var refiller:Runnable = null
}

/**
 * Implements a delivery sink which buffers the overflow of deliveries that
 * a 'down stream' sink cannot accept when it's full.
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class OverflowSink[T](val downstream:Sink[T]) extends Sink[T] {

  private var overflow = new LinkedList[T]()
  var refiller: Runnable = null

  downstream.refiller = ^{ drain }

  protected def drain:Unit = {
    while( overflowed ) {
      val delivery = overflow.removeFirst
      if( !downstream.offer(delivery) ) {
        overflow.addFirst(delivery)
        return
      } else {
        onDelivered(delivery)
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

  def overflowed = !overflow.isEmpty

  def full = overflowed || downstream.full
}

object MapSink {
  def apply[X,Y](downstream:Sink[X])(func: Y=>X ) = {
    new Sink[Y] {
      def refiller = downstream.refiller
      def refiller_=(value:Runnable) = downstream.refiller=value

      def full = downstream.full
      def offer(value:Y) = {
        if( full ) {
          false
        } else {
          downstream.offer(func(value))
        }
      }
    }
  }
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
class SinkMux[T](val target:Sink[T], val queue:DispatchQueue, val sizer:Sizer[T]) extends BaseRetained {

  var sessions = List[Session]()

  var session_min_credits = 1024*4;
  var session_credit_capacity = 1024*32
  var session_max_credits = session_credit_capacity;

  val overflow = new OverflowSink[(Session,T)](MapSink(target){_._2}) {

    // Once a value leaves the overflow, then we can credit the
    // session so that more messages can be accpted.
    override protected def onDelivered(event:(Session,T)) = {
      val session = event._1
      val value = event._2
      session.credit_adder.merge(sizer.size(value));
      session.credit_adder.release
    }
  }
  // As messages are delivered, and we credit the sessions,
  // that triggers the sessions to refill the overflow.  No
  // need to have a refiller action.
  overflow.refiller = ^ { }

  queue.retain
  setDisposer(^{
    source.release
    queue.release
  })

  // use a event aggregating source to coalesce multiple events from the same thread.
  // all the sessions send to the same source.
  val source = createSource(new ListEventAggregator[(Session,T)](), queue)
  source.setEventHandler(^{drain_source});
  source.resume

  def drain_source = {
    source.getData.foreach { event =>
      // overflow sinks can always accept more values.
      overflow.offer(event)
    }
  }

  /**
   * tracks one producer to consumer session / credit window.
   */
  class Session(val producer_queue:DispatchQueue) extends Sink[T] {

    // retain since the producer will be using this source to send messages
    // to the consumer
    source.retain

    ///////////////////////////////////////////////////
    // These members are used from the context of the
    // producer serial dispatch queue
    ///////////////////////////////////////////////////

    // create a source to coalesce credit events back to the producer side...
    val credit_adder = createSource(EventAggregators.INTEGER_ADD , producer_queue)
    credit_adder.setEventHandler(^{
      internal_credit(credit_adder.getData.intValue)
    });
    credit_adder.resume

    private var credits = 0;
    private var closed = false
    var refiller:Runnable = null
    var _full = false

    def close = {
      credit_adder.release
      source.release
      closed=true
    }

    override def full = _full

    override def offer(value: T) = {
      if( _full || closed ) {
        false
      } else {
        credit_adder.retain
        internal_credit(-sizer.size(value))
        source.merge((this, value))
        true
      }
    }

    def internal_credit(value:Int) = {
      credits += value;
      if( closed || credits <= 0 ) {
        _full = true
      } else if( credits==session_max_credits ) {
        // refill once we are empty.
        if( _full ) {
          _full  = false
          refiller.run
        }
      }
    }

    ///////////////////////////////////////////////////
    // These members are used from the context of the
    // consumer serial dispatch queue
    ///////////////////////////////////////////////////

    private var _capacity = 0

    def credit(value:Int) = ^{
      internal_credit(value)
    } >>: producer_queue

    def capacity(value:Int) = {
      val change = value - _capacity;
      _capacity = value;
      credit(change)
    }

  }

  def open(producer_queue:DispatchQueue):Sink[T] = {
    val session = createSession(producer_queue)
    sessions = session :: sessions
    session.capacity(session_max_credits)
    session
  }

  def close(session:Sink[T]) = {
    val s = session.asInstanceOf[SinkMux[T]#Session]
    s.producer_queue {
      s.close
    }
  }

  protected def createSession(producer_queue:DispatchQueue) = new Session(producer_queue)
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

  var buffer = new LinkedList[T]()
  private var size = 0

  var drainer: Runnable = null
  var refiller: Runnable = null

  def full = size >= maxSize
  def poll = buffer.poll
  def unpoll(value:T) = buffer.addFirst(value)
  def isEmpty = buffer.isEmpty

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
    val wasBlocking = full
    size -= amount
    if( !isEmpty ) {
      drain
    } else {
      refiller.run
    }
  }

}
