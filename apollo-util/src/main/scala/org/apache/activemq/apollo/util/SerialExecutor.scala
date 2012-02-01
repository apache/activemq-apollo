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
