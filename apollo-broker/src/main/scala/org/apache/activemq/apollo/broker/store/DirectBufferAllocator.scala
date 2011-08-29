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

import java.nio.channels.{WritableByteChannel, ReadableByteChannel}
import java.nio.ByteBuffer
import java.io._
import org.fusesource.hawtdispatch.Retained

/**
 * <p>Allocates ZeroCopyBuffer objects</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DirectBufferAllocator {
  def alloc(size:Int):DirectBuffer
  def close
}

/**
 * <p>
 * A ZeroCopyBuffer is a reference counted buffer on
 * temp storage.
 *
 * ON the
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DirectBuffer extends Retained {

  def size:Int

  def remaining(from_position: Int): Int

  def read(target: OutputStream):Unit

  def read(src: Int, target: WritableByteChannel): Int

  def copy(src:DirectBuffer): Unit

  def write(src:ReadableByteChannel, target:Int): Int

  def write(src:ByteBuffer, target:Int):Int

  def write(target:InputStream):Unit
}
