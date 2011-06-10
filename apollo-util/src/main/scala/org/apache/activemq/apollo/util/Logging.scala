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

import java.util.concurrent.atomic.AtomicLong
import org.slf4j.{Marker, MDC, Logger, LoggerFactory}
import java.lang.{UnsupportedOperationException, Throwable, String}
import collection.mutable.ListBuffer

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Log {

  def apply(clazz:Class[_]):Log = apply(clazz.getName.stripSuffix("$"))

  def apply(name:String):Log = new Log {
    override val log = LoggerFactory.getLogger(name)
  }

  def apply(value:Logger):Log = new Log {
    override val log = value
  }

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
    if( e!=null ) {
      val stack_ref = if( log.isDebugEnabled ) {
        val id = next_exception_id
        MDC.put("stackref", id.toString);
        Some(id)
      } else {
        None
      }
      func
      stack_ref.foreach { id=>
        log.debug(e.toString, e)
        MDC.remove("stackref")
      }
    } else {
      func
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
 * A Logging trait you can mix into an implementation class without affecting its public API
 */
trait Logging {

  protected def log: Log = Log(getClass)

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

case class LogEntry(level:String, message:String, ts:Long=System.currentTimeMillis())
class MemoryLogger(val next:Logger) extends Logger {

  var messages = ListBuffer[LogEntry]()

  def add(level:String, message:String) = {
    messages.append(LogEntry(message, level))
    while ( messages.size > 1000 ) {
      message.drop(1)
    }
    next
  }
  
  def getName = next.getName

  def isWarnEnabled(marker: Marker) = true
  def isWarnEnabled = true
  def warn(msg: String) = add("warn", msg).warn(msg) 
  def warn(msg: String, t: Throwable) =  add("warn", msg).warn(msg, t)
  def warn(marker: Marker, msg: String, t: Throwable) = throw new UnsupportedOperationException()
  def warn(marker: Marker, msg: String) = throw new UnsupportedOperationException()
  def warn(marker: Marker, format: String, argArray: Array[AnyRef]) = throw new UnsupportedOperationException()
  def warn(marker: Marker, format: String, arg: AnyRef) = throw new UnsupportedOperationException()
  def warn(marker: Marker, format: String, arg1: AnyRef, arg2: AnyRef) = throw new UnsupportedOperationException()
  def warn(format: String, argArray: Array[AnyRef]) = throw new UnsupportedOperationException()
  def warn(format: String, arg: AnyRef) = throw new UnsupportedOperationException()
  def warn(format: String, arg1: AnyRef, arg2: AnyRef) = throw new UnsupportedOperationException()

  def isTraceEnabled(marker: Marker) = true
  def isTraceEnabled = true
  def trace(msg: String) = add("trace", msg).trace(msg) 
  def trace(msg: String, t: Throwable) =  add("trace", msg).trace(msg, t)
  def trace(marker: Marker, msg: String, t: Throwable) = throw new UnsupportedOperationException()
  def trace(marker: Marker, msg: String) = throw new UnsupportedOperationException()
  def trace(marker: Marker, format: String, argArray: Array[AnyRef]) = throw new UnsupportedOperationException()
  def trace(marker: Marker, format: String, arg: AnyRef) = throw new UnsupportedOperationException()
  def trace(marker: Marker, format: String, arg1: AnyRef, arg2: AnyRef) = throw new UnsupportedOperationException()
  def trace(format: String, argArray: Array[AnyRef]) = throw new UnsupportedOperationException()
  def trace(format: String, arg: AnyRef) = throw new UnsupportedOperationException()
  def trace(format: String, arg1: AnyRef, arg2: AnyRef) = throw new UnsupportedOperationException()  
  
  def isInfoEnabled(marker: Marker) = true
  def isInfoEnabled = true
  def info(msg: String) = add("info", msg).info(msg) 
  def info(msg: String, t: Throwable) =  add("info", msg).info(msg, t)
  def info(marker: Marker, msg: String, t: Throwable) = throw new UnsupportedOperationException()
  def info(marker: Marker, msg: String) = throw new UnsupportedOperationException()
  def info(marker: Marker, format: String, argArray: Array[AnyRef]) = throw new UnsupportedOperationException()
  def info(marker: Marker, format: String, arg: AnyRef) = throw new UnsupportedOperationException()
  def info(marker: Marker, format: String, arg1: AnyRef, arg2: AnyRef) = throw new UnsupportedOperationException()
  def info(format: String, argArray: Array[AnyRef]) = throw new UnsupportedOperationException()
  def info(format: String, arg: AnyRef) = throw new UnsupportedOperationException()
  def info(format: String, arg1: AnyRef, arg2: AnyRef) = throw new UnsupportedOperationException()  
  
  def isErrorEnabled(marker: Marker) = true
  def isErrorEnabled = true
  def error(msg: String) = add("error", msg).error(msg) 
  def error(msg: String, t: Throwable) =  add("error", msg).error(msg, t)
  def error(marker: Marker, msg: String, t: Throwable) = throw new UnsupportedOperationException()
  def error(marker: Marker, msg: String) = throw new UnsupportedOperationException()
  def error(marker: Marker, format: String, argArray: Array[AnyRef]) = throw new UnsupportedOperationException()
  def error(marker: Marker, format: String, arg: AnyRef) = throw new UnsupportedOperationException()
  def error(marker: Marker, format: String, arg1: AnyRef, arg2: AnyRef) = throw new UnsupportedOperationException()
  def error(format: String, argArray: Array[AnyRef]) = throw new UnsupportedOperationException()
  def error(format: String, arg: AnyRef) = throw new UnsupportedOperationException()
  def error(format: String, arg1: AnyRef, arg2: AnyRef) = throw new UnsupportedOperationException()  
  
  def isDebugEnabled(marker: Marker) = true
  def isDebugEnabled = true
  def debug(msg: String) = add("debug", msg).debug(msg) 
  def debug(msg: String, t: Throwable) =  add("debug", msg).debug(msg, t)
  def debug(marker: Marker, msg: String, t: Throwable) = throw new UnsupportedOperationException()
  def debug(marker: Marker, msg: String) = throw new UnsupportedOperationException()
  def debug(marker: Marker, format: String, argArray: Array[AnyRef]) = throw new UnsupportedOperationException()
  def debug(marker: Marker, format: String, arg: AnyRef) = throw new UnsupportedOperationException()
  def debug(marker: Marker, format: String, arg1: AnyRef, arg2: AnyRef) = throw new UnsupportedOperationException()
  def debug(format: String, argArray: Array[AnyRef]) = throw new UnsupportedOperationException()
  def debug(format: String, arg: AnyRef) = throw new UnsupportedOperationException()
  def debug(format: String, arg1: AnyRef, arg2: AnyRef) = throw new UnsupportedOperationException()  
}
