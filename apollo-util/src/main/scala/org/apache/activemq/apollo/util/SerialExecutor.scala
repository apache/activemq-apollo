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

package org.apache.activemq.apollo.util

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, Executor}

/**
 * <p>Provides serial execution of runnable tasks on any executor.</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class SerialExecutor(target: Executor) extends Executor {

  private final val triggered: AtomicBoolean = new AtomicBoolean
  private final val queue = new ConcurrentLinkedQueue[Runnable]()

  final def execute(action: Runnable) {
    queue.add(action)
    if (triggered.compareAndSet(false, true)) {
      target.execute(self);
    }
  }

  private final object self extends Runnable {
    def run = drain
  }

  private final def drain: Unit = {
    while (true) {
      try {
        var action = queue.poll
        while (action != null) {
          try {
            action.run
          } catch {
            case e: Throwable =>
              var thread: Thread = Thread.currentThread
              thread.getUncaughtExceptionHandler.uncaughtException(thread, e)
          }
          action = queue.poll
        }
      } finally {
        drained
        triggered.set(false)
        if (queue.isEmpty || !triggered.compareAndSet(false, true)) {
          return
        }
      }
    }
  }

  /**
   * Subclasses can override this method so that it
   * can perform some work once the execution queue
   * is drained.
   */
  protected def drained: Unit = {
  }

}
