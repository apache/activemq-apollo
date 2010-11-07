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

import _root_.java.util.{LinkedHashMap, HashMap}
import _root_.java.lang.{Throwable, String}
import org.slf4j.{MDC, Logger, LoggerFactory}


import java.util.concurrent.atomic.AtomicLong

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Log {
  val exception_id_generator = new AtomicLong(System.currentTimeMillis)
  def next_exception_id = exception_id_generator.incrementAndGet.toHexString
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Log {
  import Log._
  val log = LoggerFactory.getLogger(getClass.getName.stripSuffix("$"))

  private def with_throwable(e:Throwable)(func: =>Unit) = {
    val stack_ref = if( log.isDebugEnabled ) {
      val id = next_exception_id
      MDC.put("stack reference", id.toString);
      Some(id)
    } else {
      None
    }
    func
    stack_ref.foreach { id=>
      log.debug("stack trace: "+id, e)
      MDC.remove("stack reference")
    }
  }

  private def format(message:String, args:Seq[Any]) = {
    if( args.isEmpty ) {
      message
    } else {
      message.format(args.map(_.asInstanceOf[AnyRef]) : _*)
    }
  }

  def error(m: => String, args:Any*): Unit = {
    if( log.isErrorEnabled ) {
      log.error(format(m, args.toSeq))
    }
  }

  def error(e: Throwable, m: => String, args:Any*): Unit = {
    with_throwable(e) {
      if( log.isErrorEnabled ) {
        log.error(format(m, args.toSeq))
      }
    }
  }

  def error(e: Throwable): Unit = {
    with_throwable(e) {
      if( log.isErrorEnabled ) {
        log.error(e.getMessage)
      }
    }
  }

  def warn(m: => String, args:Any*): Unit = {
    if( log.isWarnEnabled ) {
      log.warn(format(m, args.toSeq))
    }
  }

  def warn(e: Throwable, m: => String, args:Any*): Unit = {
    with_throwable(e) {
      if( log.isWarnEnabled ) {
        log.warn(format(m, args.toSeq))
      }
    }
  }

  def warn(e: Throwable): Unit = {
    with_throwable(e) {
      if( log.isWarnEnabled ) {
        log.warn(e.getMessage)
      }
    }
  }

  def info(m: => String, args:Any*): Unit = {
    if( log.isInfoEnabled ) {
      log.info(format(m, args.toSeq))
    }
  }

  def info(e: Throwable, m: => String, args:Any*): Unit = {
    with_throwable(e) {
      if( log.isInfoEnabled ) {
        log.info(format(m, args.toSeq))
      }
    }
  }

  def info(e: Throwable): Unit = {
    with_throwable(e) {
      if( log.isInfoEnabled ) {
        log.info(e.getMessage)
      }
    }
  }


  def debug(m: => String, args:Any*): Unit = {
    if( log.isDebugEnabled ) {
      log.debug(format(m, args.toSeq))
    }
  }

  def debug(e: Throwable, m: => String, args:Any*): Unit = {
    if( log.isDebugEnabled ) {
      log.debug(format(m, args.toSeq), e)
    }
  }

  def debug(e: Throwable): Unit = {
    if( log.isDebugEnabled ) {
      log.debug(e.getMessage, e)
    }
  }

  def trace(m: => String, args:Any*): Unit = {
    if( log.isTraceEnabled ) {
      log.trace(format(m, args.toSeq))
    }
  }

  def trace(e: Throwable, m: => String, args:Any*): Unit = {
    if( log.isTraceEnabled ) {
      log.trace(format(m, args.toSeq), e)
    }
  }

  def trace(e: Throwable): Unit = {
    if( log.isTraceEnabled ) {
      log.trace(e.getMessage, e)
    }
  }

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class NamedLog(name:String) extends Log {
  def this(clazz:Class[_]) = this(clazz.getName.stripSuffix("$"))
  override val log = LoggerFactory.getLogger(name)
}

/**
 * A Logging trait you can mix into an implementation class without affecting its public API
 */
trait Logging {

  protected def log: Log = new NamedLog(getClass)

  protected def error(message: => String, args:Any*)= log.error(message, args : _*)
  protected def error(e: Throwable, message: => String, args:Any*)= log.error(e, message, args: _*)
  protected def error(e: Throwable)= log.error(e)

  protected def warn(message: => String, args:Any*)= log.warn(message, args: _*)
  protected def warn(e: Throwable, message: => String, args:Any*)= log.warn(e, message, args: _*)
  protected def warn(e: Throwable)= log.warn(e)

  protected def info(message: => String, args:Any*)= log.info(message, args: _*)
  protected def info(e: Throwable, message: => String, args:Any*)= log.info(e, message, args: _*)
  protected def info(e: Throwable)= log.info(e)

  protected def debug(message: => String, args:Any*)= log.debug(message, args: _*)
  protected def debug(e: Throwable, message: => String, args:Any*)= log.debug(e, message, args: _*)
  protected def debug(e: Throwable)= log.debug(e)

  protected def trace(message: => String, args:Any*)= log.trace(message, args: _*)
  protected def trace(e: Throwable, message: => String, args:Any*)= log.trace(e, message, args: _*)
  protected def trace(e: Throwable)= log.trace(e)

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DispatchLogging extends Logging {
}
