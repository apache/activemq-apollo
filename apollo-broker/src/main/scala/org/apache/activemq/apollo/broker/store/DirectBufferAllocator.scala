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
package org.apache.activemq.apollo.broker.store

import org.fusesource.hawtdispatch._
import java.nio.channels.{FileChannel, WritableByteChannel, ReadableByteChannel}
import java.nio.ByteBuffer
import java.io._
import org.apache.activemq.apollo.util._
/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DirectBufferAllocator {
  def alloc(size:Int):DirectBuffer
}

/**
 * <p>
 * A DirectBuffer is a reference counted buffer on
 * temp storage designed to be accessed with direct io.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DirectBuffer extends Retained {

  def size:Int

  def remaining(from_position: Int): Int

  def read(target: OutputStream):Unit

  def read(src: Int, target: WritableByteChannel): Int

  def write(src:ReadableByteChannel, target:Int): Int

  def write(src:ByteBuffer, target:Int):Int

  def link(target:File):Long
}

trait FileDirectBufferTrait extends DirectBuffer {

  def offset:Long
  def channel:FileChannel

  def remaining(pos: Int): Int = size-pos

  def read(src: Int, target: WritableByteChannel): Int = {
    assert(retained > 0)
    val count: Int = remaining(src)
    assert(count>=0)
    channel.transferTo(offset+src, count, target).toInt
  }

  def read(target: OutputStream): Unit = {
    assert(retained > 0)
    val b = ByteBuffer.allocate(size.min(1024*4))
    var pos = 0
    while( remaining(pos)> 0 ) {
      val count = channel.read(b, offset+pos)
      if( count == -1 ) {
        throw new EOFException()
      }
      target.write(b.array, 0, count)
      pos += count
      b.clear
    }
  }

  def write(src: ReadableByteChannel, target:Int): Int = {
    assert(retained > 0)
    val count: Int = remaining(target)
    assert(count>=0)
    channel.transferFrom(src, offset+target, count).toInt
  }

  def write(src: ByteBuffer, target: Int): Int = {
    assert(retained > 0)
    val diff = src.remaining - remaining(target)
    if( diff > 0 ) {
      src.limit(src.limit-diff)
    }
    try {
      channel.write(src, offset+target).toInt
    } finally {
      if( diff > 0 ) {
        src.limit(src.limit+diff)
      }
    }
  }

  def link(target: File): Long = {
    assert(retained > 0)
    // TODO: implement with a real file system hard link
    // to get copy on write goodness.
    import FileSupport._
    using(new FileOutputStream(target).getChannel) { target=>
      val count = channel.transferTo(offset, size, target)
      assert( count == size )
    }
    return 0;
  }
}

case class Allocation(size:Long, offset:Long) extends Ordered[Allocation] {

  var _free_func: (Allocation)=>Unit = _

  def free() = {
    _free_func(this)
  }

  def compare(that: Allocation): Int = {
    var rc = longWrapper(size).compareTo(that.size)
    if( rc!=0 ) {
      rc
    } else {
      longWrapper(offset).compareTo(that.offset)
    }
  }

  // split the allocation..
  def split(request:Long):(Allocation, Allocation) = {
    assert(request < size)
    var first = Allocation(offset, request)
    var second = Allocation(offset+request, size-request)
    (first, second)
  }

  // join the allocation..
  def join(that:Allocation):Allocation = {
    assert( that.offset == offset+size)
    Allocation(offset, size+that.size)
  }

}

trait Allocator {
  def alloc(request:Long):Allocation

  def chain(that:Allocator):Allocator = new Allocator() {
    def alloc(request: Long): Allocation = {
      val rc = Allocator.this.alloc(request)
      if( rc == null ) {
        that.alloc(request)
      } else {
        rc
      }
    }
  }
}

class TreeAllocator(range:Allocation) extends Allocator {

  // list of the free allocation areas.  Sorted by size then offset
  val free_by_size = new TreeMap[Allocation, Zilch]()
  // list of the free allocation areas sorted by offset.
  val free_by_offset = new TreeMap[Long, Allocation]()

  {
    val allocation = range.copy()
    free_by_offset.put(allocation.offset, allocation)
    free_by_size.put(allocation, null)
  }

  def alloc(request:Long):Allocation = {
    var spot_entry = free_by_size.ceilingEntry(Allocation(request, 0))
    if( spot_entry== null ) {
      return null
    }

    val allocation = spot_entry.getKey
    free_by_size.remove(allocation)

    // might be the perfect size
    if( allocation.size == request ) {
      allocation._free_func = free
      allocation
    } else {
      // split the allocation..
      var (first, second) = allocation.split(request)

      free_by_offset.remove(first.offset)
      free_by_offset.put(second.offset, second)

      // put the free part in the free map.
      free_by_size.put(second, null)

      first._free_func = free
      first
    }
  }

  def free(allocation:Allocation):Unit = {


    var prev_e = free_by_offset.floorEntry(allocation.offset)
    var next_e = if( prev_e!=null ) {
      prev_e.next
    } else {
      free_by_offset.ceilingEntry(allocation.offset)
    }

    val prev = Option(prev_e).map(_.getValue).map( a=> if(a.offset+a.size == allocation.offset) a else null ).getOrElse(null)
    val next = Option(prev_e).map(_.getValue).map( a=> if(allocation.offset+allocation.size == a.offset) a else null ).getOrElse(null)

    (prev, next) match {
      case (null, null)=>
        allocation._free_func = null
        free_by_size.put(allocation, null)
        free_by_offset.put(allocation.offset, allocation)

      case (prev, null)=>
        val joined = prev.join(allocation)
        free_by_size.remove(prev)
        free_by_size.put(joined, null)
        free_by_offset.put(joined.offset, joined)

      case (null, next)=>
        val joined = allocation.join(next)
        free_by_size.remove(next)
        free_by_size.put(joined, null)

        free_by_offset.remove(next.offset)
        free_by_offset.put(joined.offset, joined)

      case (prev, next)=>
        val joined = prev.join(allocation.join(next))
        free_by_size.remove(prev)
        free_by_size.remove(next)
        free_by_size.put(joined, null)

        free_by_offset.remove(next.offset)
        free_by_offset.put(joined.offset, joined)
    }
  }
}

/**
 * Helps minimize the active page set.
 */
class ActiveAllocator(val range:Allocation) extends Allocator {

  // the cold allocated start with all the free space..
  val inactive = new TreeAllocator(range)

  // the hot is clear of any free space.
  val active = new TreeAllocator(range)

  active.free_by_offset.clear
  active.free_by_size.clear

  // allocate out of the hot area first since
  // that should result in less vm swapping
  val chain = active.chain(inactive)

  def alloc(request:Long):Allocation = {
    var rc = chain.alloc(request)
    if( rc!=null ) {
      rc._free_func = free
    }
    rc
  }

  def free(allocation:Allocation):Unit = {
    // put stuff back in the hot tree.
    active.free(allocation)
  }

}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class FileDirectBufferAllocator(val directory:File) extends DirectBufferAllocator {

  // we use thread local allocators to
  class AllocatorContext(val queue:DispatchQueue) {

    val allocator = new TreeAllocator(Allocation(0, Long.MaxValue))
    var channel:FileChannel = new RandomAccessFile(queue.getLabel, "rw").getChannel

    class AllocationBuffer(val allocation:Allocation) extends BaseRetained with FileDirectBufferTrait {
      def channel: FileChannel = AllocatorContext.this.channel
      def offset: Long = allocation.offset
      def size: Int = allocation.size.toInt

      override def dispose: Unit = {
        super.dispose
        // since we might not get disposed from the same thread
        // that did the allocation..
        queue <<| ^{
          allocation.free()
        }
      }
    }

    def alloc(size: Int): DirectBuffer = with_allocator_context { ctx=>
      val allocation = allocator.alloc(size)
      assert(allocation!=null)
      new AllocationBuffer(allocation)
    }
  }

  val _current_allocator_context = new ThreadLocal[AllocatorContext]()

  protected def start() = {
    directory.mkdirs
  }

  def alloc(size: Int): DirectBuffer = with_allocator_context { ctx=>
    ctx.alloc(size)
  }

  def with_allocator_context[T](func: (AllocatorContext)=>T):T = {
    if( getCurrentThreadQueue == null ) {
      getGlobalQueue().future(func(current_allocator_context))()
    } else {
      func(current_allocator_context)
    }
  }

  def current_allocator_context:AllocatorContext = {
    val thread_queue = getCurrentThreadQueue
    assert(thread_queue != null)
    var rc = _current_allocator_context.get
    if( rc==null ) {
      rc = new AllocatorContext(thread_queue)
      _current_allocator_context.set(rc)
    }
    rc
  }
}