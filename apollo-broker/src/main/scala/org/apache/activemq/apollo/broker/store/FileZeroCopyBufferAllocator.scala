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
import org.fusesource.hawtdispatch.internal.DispatcherConfig
import org.fusesource.hawtdispatch.BaseRetained
import java.nio.channels.{FileChannel, WritableByteChannel, ReadableByteChannel}
import java.io._
import org.apache.activemq.apollo.util._
import java.util.concurrent.TimeUnit
import java.nio.channels.FileChannel.MapMode
import java.security.{AccessController, PrivilegedAction}
import java.lang.reflect.Method
import java.nio.{MappedByteBuffer, ByteBuffer}

/**
 * <p>Tracks allocated space</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class Allocation(offset:Long, size:Long) extends Ordered[Allocation] {

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

/**
 * <p>Manges allocation space using a couple trees to track the free areas.</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
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
    free_by_size.removeEntry(spot_entry)
    free_by_offset.remove(allocation.offset)

    // might be the perfect size
    val rc = if( allocation.size == request ) {
      allocation
    } else {
      // split the allocation..
      var (first, second) = allocation.split(request)

      // put the free part in the free map.
      free_by_offset.put(second.offset, second)
      free_by_size.put(second, null)

      first
    }
    rc._free_func = free
    rc
  }

  def alloc_at(req:Allocation):Boolean = {
    var spot_entry = free_by_offset.floorEntry(req.offset)
    if( spot_entry== null ) {
      return false
    }

    var spot = spot_entry.getValue
    if( spot.offset+spot.size < req.offset+req.size ) {
      return false
    }

    free_by_offset.removeEntry(spot_entry)
    free_by_size.remove(spot)

    // only need to put back if it was not exactly what we need.
    if( spot != req ) {

      // deal with excess at the front
      if( spot.offset != req.offset ) {
        val (prev, next) = spot.split(req.offset - spot.offset)
        free_by_offset.put(prev.offset, prev)
        free_by_size.put(prev, null)
        spot = next
      }

      // deal with excess at the rear
      if( spot.size != req.size ) {
        val (prev, next) = spot.split(req.size)
        free_by_offset.put(next.offset, next)
        free_by_size.put(next, null)
      }
    }

    req._free_func = free
    true
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
 * Helps minimize the active page set by allocating in areas
 * which had previously been allocated.
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
 * <p>The ByteBufferReleaser allows you to more eagerly deallocate byte buffers.</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ByteBufferReleaser {
  val release: (ByteBuffer) => Unit = {

    // Try to drill into the java.nio.DirectBuffer internals...
    AccessController.doPrivileged(new PrivilegedAction[(ByteBuffer) => Unit]() {
      def run = {
        try {

          val cleanerMethod = ByteBuffer.allocateDirect(1).getClass().getMethod("cleaner")
          cleanerMethod.setAccessible(true)
          val cleanMethod = cleanerMethod.getReturnType().getMethod("clean")

          def clean(buffer: ByteBuffer):Unit = {
            try {
              val cleaner = cleanerMethod.invoke(buffer)
              if (cleaner != null) {
                cleanMethod.invoke(cleaner)
              }
            } catch {
              case e: Throwable => e.printStackTrace
            }
          }

          clean _
        } catch {
          case _ =>
            def noop(buffer: ByteBuffer):Unit = { }
            noop _
        }
      }
    })
  }
}

object FileZeroCopyBufferAllocator {
  val OS = System.getProperty("os.name").toLowerCase

  val MMAP_TRANSFER_TO = Option(System.getProperty("apollo.MMAP_TRANSFER_TO")).map(_ == "true").getOrElse{
    // System prop is not set.. lets pick a good default based on OS
    if( OS.startsWith("mac") ) {
      // mmap is faster on the mac than the FileChannel.transferTo call.
      true
    } else {
      false
    }
  }
  val MMAP_TRANSFER_FROM = Option(System.getProperty("apollo.MMAP_TRANSFER_FROM")).map(_ == "true").getOrElse{
    // System prop is not set.. lets pick a good default based on OS
    if( OS.startsWith("mac") ) {
      false
    } else {
      false
    }
  }
}


/**
 * <p>A ZeroCopyBufferAllocator which allocates on files.</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class FileZeroCopyBufferAllocator(val directory:File) extends ZeroCopyBufferAllocator {
  import FileZeroCopyBufferAllocator._

  // we use thread local allocators to
  class AllocatorContext(val id:Int) {

    val allocator = new TreeAllocator(Allocation(0, Long.MaxValue))
    var channel:FileChannel = new RandomAccessFile(new File(directory, ""+id+".data"), "rw").getChannel
    var queue:DispatchQueue = _

    var last_sync_size = channel.size
    @volatile
    var current_size = last_sync_size

    def size_changed = this.synchronized {
      val t = current_size
      if( t != last_sync_size ) {
        last_sync_size = t
        true
      } else {
        false
      }
    }

    var _mmap:MappedByteBuffer = _

    def mmap_slice(offset:Long, size:Int) = {
      if( _mmap == null ) {
        _mmap = channel.map(MapMode.READ_WRITE, 0, current_size)
      }

      // remaps more of the file when needed.
      if( _mmap.capacity < offset+size ) {
        assert(current_size >= offset+size)
        ByteBufferReleaser.release(_mmap)

        val grow = 1024*1024*64
        _mmap = channel.map(MapMode.READ_WRITE, 0, current_size+grow)

        // initialize the grown part...
        _mmap.position(current_size.toInt)
        while(_mmap.hasRemaining) {
          _mmap.put(0.toByte)
        }
        current_size += grow
        _mmap.clear
      }

      _mmap.position(offset.toInt)
      _mmap.limit(offset.toInt+size)
      val slice = _mmap.slice
      _mmap.clear
      slice
    }

    def sync = {
      if( MMAP_TRANSFER_FROM && _mmap!=null ) {
        _mmap.force
      }
      channel.force(size_changed)
    }

    /**
     * <p>A ZeroCopyBuffer which was allocated on a file.</p>
     *
     * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
     */
    trait FileZeroCopyBufferTrait extends BaseRetained with ZeroCopyBuffer {

      def offset:Long

      def remaining(pos: Int): Int = size-pos

      def time[T](name:String)(func: =>T):T = {
        val c = new TimeCounter
        try {
          c.time(func)
        } finally {
          println("%s: %.2f".format(name, c.apply(true).maxTime(TimeUnit.MILLISECONDS)))
        }
      }

      def read(src: Int, target: WritableByteChannel): Int = {
        assert(retained > 0)
        val count: Int = remaining(src)
        assert(count>=0)

        if( MMAP_TRANSFER_TO ) {
          val buffer = mmap_slice(offset+src, count)
          target.write(buffer)
        } else {
          channel.transferTo(offset+src, count, target).toInt
        }
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

        if( MMAP_TRANSFER_FROM ) {
          val buffer = mmap_slice(offset+target, count)
          src.read(buffer)
        } else {
          channel.transferFrom(src, offset+target, count).toInt
        }

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

      def write(target: InputStream): Unit = {
        assert(retained > 0)
        val b = ByteBuffer.allocate(size.min(1024*4))
        var pos = 0
        while( remaining(pos)> 0 ) {
          val max = remaining(pos).min(b.capacity)
          b.clear
          val count = target.read(b.array, 0, max)
          if( count == -1 ) {
            throw new EOFException()
          }
          val x = channel.write(b)
          assert(x == count)
          pos += count
        }
      }
    }

    class AllocationBuffer(val allocation:Allocation) extends FileZeroCopyBufferTrait {

      def file = id
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

    def alloc(size: Int) = current_context { ctx=>
      val allocation = allocator.alloc(size)
      assert(allocation!=null)
      current_size = current_size.max(allocation.offset + allocation.size)
      new AllocationBuffer(allocation)
    }

    def view_buffer(the_offset:Long, the_size:Int):ZeroCopyBuffer = {
      new FileZeroCopyBufferTrait {
        def offset: Long = the_offset
        def size: Int = the_size
      }
    }

  }

  def to_alloc_buffer(buffer:ZeroCopyBuffer) = buffer.asInstanceOf[AllocatorContext#AllocationBuffer]

  val _current_allocator_context = new ThreadLocal[AllocatorContext]()
  var contexts = Map[Int, AllocatorContext]()

  def start() = {
    directory.mkdirs
    var i=0;
    for( queue <- getThreadQueues()) {
      val ctx = new AllocatorContext(i)
      ctx.queue = queue
      contexts += i->ctx
      queue {
        _current_allocator_context.set(ctx)
      }
      i += 1
    }
  }

  def stop() = {
    for( queue <- getThreadQueues() ) {
      queue {
        _current_allocator_context.remove
      }
    }
    contexts = Map()
  }

  def sync(file: Int) = {
    contexts.get(file).get.sync
  }

  def alloc(size: Int): ZeroCopyBuffer = current_context { ctx=>
    ctx.alloc(size)
  }

  def alloc_at(file:Int, offset:Long, size:Int):Unit = context(file) { ctx=>
    ctx.allocator.alloc_at(Allocation(offset, size))
  }

  def free(file:Int, offset:Long, size:Int):Unit = context(file) { ctx=>
    ctx.allocator.free(Allocation(offset, size))
  }

  def view_buffer(file:Int, the_offset:Long, the_size:Int):ZeroCopyBuffer = {
    contexts.get(file).get.view_buffer(the_offset, the_size)
  }

  def context(i:Int)(func: (AllocatorContext)=>Unit):Unit= {
    getThreadQueues()(i) {
      func(current_allocator_context)
    }
  }

  def current_context[T](func: (AllocatorContext)=>T):T = {
    if( getCurrentThreadQueue == null ) {
      getGlobalQueue().future(func(current_allocator_context))()
    } else {
      func(current_allocator_context)
    }
  }

  def current_allocator_context:AllocatorContext = _current_allocator_context.get

}