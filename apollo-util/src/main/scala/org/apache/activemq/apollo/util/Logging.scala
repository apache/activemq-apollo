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
import _root_.org.apache.commons.logging.LogFactory
import _root_.org.apache.commons.logging.{Log => Logger}
import java.util.concurrent.atomic.AtomicLong

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Log {
  val log = LogFactory.getLog(getClass.getName.stripSuffix("$"))

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class NamedLog(name:String) extends Log {
  def this(clazz:Class[_]) = this(clazz.getName.stripSuffix("$"))
  override val log = LogFactory.getLog(name)
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Logging {
  val exception_id_generator = new AtomicLong(System.currentTimeMillis)
  def next_exception_id = exception_id_generator.incrementAndGet.toHexString
}

/**
 * A Logging trait you can mix into an implementation class without affecting its public API
 */
trait Logging {

  import Logging._
  protected def log: Log = new NamedLog(getClass)
  protected def log_map(message:String) = message

  protected def error(message: => String, args:Any*): Unit = {
    val l = log.log
    if( l.isErrorEnabled ) {
      val m = if( args.isEmpty ) {
        message
      } else {
        format(message, args.map(_.asInstanceOf[AnyRef]) : _*)
      }
      l.error(log_map(m))
    }
  }

  protected def error(e: Throwable, message: => String, args:Any*): Unit = {
    val l = log.log
    if( l.isErrorEnabled ) {
      val m = if( args.isEmpty ) {
        message
      } else {
        format(message, args.map(_.asInstanceOf[AnyRef]) : _*)
      }
      val exception_id = next_exception_id
      log.log.error(log_map(m)+" (ref:"+exception_id+")")
      log.log.debug("(ref:"+exception_id+")", e)
    }
  }

  protected def error(e: Throwable): Unit = {
    val l = log.log
    if( l.isErrorEnabled ) {
      val exception_id = next_exception_id
      log.log.error(log_map(e.getMessage)+" (ref:"+exception_id+")")
      log.log.debug("(ref:"+exception_id+")", e)
    }
  }

  protected def warn(message: => String, args:Any*): Unit = {
    val l = log.log
    if( l.isWarnEnabled ) {
      val m = if( args.isEmpty ) {
        message
      } else {
        format(message, args.map(_.asInstanceOf[AnyRef]) : _*)
      }
      l.warn(log_map(m))
    }
  }

  protected def warn(e: Throwable, message: => String, args:Any*): Unit = {
    val l = log.log
    if( l.isWarnEnabled ) {
      val m = if( args.isEmpty ) {
        message
      } else {
        format(message, args.map(_.asInstanceOf[AnyRef]) : _*)
      }
      val exception_id = next_exception_id
      log.log.warn(log_map(m)+" (ref:"+exception_id+")")
      log.log.debug("(ref:"+exception_id+")", e)
    }
  }

  protected def warn(e: Throwable): Unit = {
    val l = log.log
    if( l.isWarnEnabled ) {
      val exception_id = next_exception_id
      log.log.warn(log_map(e.getMessage)+" (ref:"+exception_id+")")
      log.log.debug("(ref:"+exception_id+")", e)
    }
  }

  protected def info(message: => String, args:Any*): Unit = {
    val l = log.log
    if( l.isInfoEnabled ) {
      val m = if( args.isEmpty ) {
        message
      } else {
        format(message, args.map(_.asInstanceOf[AnyRef]) : _*)
      }
      l.info(log_map(m))
    }
  }

  protected def info(e: Throwable, message: => String, args:Any*): Unit = {
    val l = log.log
    if( l.isInfoEnabled ) {
      val m = if( args.isEmpty ) {
        message
      } else {
        format(message, args.map(_.asInstanceOf[AnyRef]) : _*)
      }
      val exception_id = next_exception_id
      log.log.info(log_map(m)+" (ref:"+exception_id+")")
      log.log.debug("(ref:"+exception_id+")", e)
    }
  }

  protected def info(e: Throwable): Unit = {
    val l = log.log
    if( l.isInfoEnabled ) {
      val exception_id = next_exception_id
      log.log.info(log_map(e.getMessage)+" (ref:"+exception_id+")")
      log.log.debug("(ref:"+exception_id+")", e)
    }
  }


  protected def debug(message: => String, args:Any*): Unit = {
    val l = log.log
    if( l.isDebugEnabled ) {
      val m = if( args.isEmpty ) {
        message
      } else {
        format(message, args.map(_.asInstanceOf[AnyRef]) : _*)
      }
      l.debug(log_map(m))
    }
  }

  protected def debug(e: Throwable, message: => String, args:Any*): Unit = {
    val l = log.log
    if( l.isDebugEnabled ) {
      val m = if( args.isEmpty ) {
        message
      } else {
        format(message, args.map(_.asInstanceOf[AnyRef]) : _*)
      }
      log.log.debug(log_map(m), e)
    }
  }

  protected def debug(e: Throwable): Unit = {
    val l = log.log
    if( l.isDebugEnabled ) {
      log.log.debug(log_map(e.getMessage), e)
    }
  }

  protected def trace(message: => String, args:Any*): Unit = {
    val l = log.log
    if( l.isTraceEnabled ) {
      val m = if( args.isEmpty ) {
        message
      } else {
        format(message, args.map(_.asInstanceOf[AnyRef]) : _*)
      }
      l.trace(log_map(m))
    }
  }

  protected def trace(e: Throwable, message: => String, args:Any*): Unit = {
    val l = log.log
    if( l.isTraceEnabled ) {
      val m = if( args.isEmpty ) {
        message
      } else {
        format(message, args.map(_.asInstanceOf[AnyRef]) : _*)
      }
      log.log.trace(log_map(m), e)
    }
  }

  protected def trace(e: Throwable): Unit = {
    val l = log.log
    if( l.isTraceEnabled ) {
      log.log.trace(log_map(e.getMessage), e)
    }
  }

}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DispatchLogging extends Logging {
  import org.fusesource.hawtdispatch.ScalaDispatch._

  override protected def log_map(message:String) = {
    val d = getCurrentQueue
    if( d!=null && d.getLabe!=null ) {
      d.getLabel+" | "+message
    } else {
      message
    }
  }

}