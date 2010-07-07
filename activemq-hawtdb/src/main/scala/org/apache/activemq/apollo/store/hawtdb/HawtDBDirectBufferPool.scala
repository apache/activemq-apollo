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
package org.apache.activemq.apollo.store.hawtdb

import java.nio.ByteBuffer
import java.io.File

import org.fusesource.hawtdispatch.BaseRetained
import org.fusesource.hawtdb.api.Paged.SliceType
import org.fusesource.hawtdb.api.PageFileFactory
import org.apache.activemq.apollo.util.{DirectBuffer, DirectBufferPool}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class HawtDBDirectBufferPool(val file:File) extends DirectBufferPool {

  private val pageFilefactory = new PageFileFactory()
  private def pageFile = pageFilefactory.getPageFile

  def stop = stop(null)
  def start = start(null)

  def stop(onComplete: Runnable) = {
    pageFilefactory.close
    file.delete
    if( onComplete!=null ) {
      onComplete.run
    }
  }

  def start(onComplete: Runnable) = {
    file.delete
    pageFilefactory.setFile(file);
    pageFilefactory.setHeaderSize(0);
    pageFilefactory.setPageSize(1024)
    pageFilefactory.open
    if( onComplete!=null ) {
      onComplete.run
    }
  }

  class HawtMemoryAllocation(val page:Int, val page_count:Int, val original:ByteBuffer, val buffer:ByteBuffer) extends BaseRetained with DirectBuffer {
    override def dispose = {
      pageFile.unslice(original)
      pageFile.allocator.free(page, page_count)
    }
  }

  def alloc(alloc_size: Int) = {
    val page_count: Int = pageFile.pages(alloc_size)
    val page = pageFile.allocator.alloc(page_count)
    val original = pageFile.slice(SliceType.READ_WRITE, page, page_count)
    original.limit(original.position+alloc_size)
    val slice = original.slice
    new HawtMemoryAllocation(page, page_count, original, slice)
  }
}