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

import _root_.java.util.{LinkedHashMap, HashMap}
import _root_.java.lang.{Throwable, String}
import _root_.org.apache.commons.logging.LogFactory
import _root_.org.apache.commons.logging.{Log => Logger}

trait Log {
  val log = LogFactory.getLog(getClass.getName)
}

/**
 * A Logging trait you can mix into an implementation class without affecting its public API
 */
trait Logging {

  protected def log: Log

  protected def error(message: => String): Unit = log.log.error(message)

  protected def error(e: Throwable): Unit = log.log.error(e.getMessage, e)

  protected def error(message: => String, e: Throwable): Unit = log.log.error(message, e)

  protected def warn(message: => String): Unit = log.log.warn(message)

  protected def warn(message: => String, e: Throwable): Unit = log.log.warn(message, e)

  protected def info(message: => String): Unit = log.log.info(message)

  protected def info(message: => String, e: Throwable): Unit = log.log.info(message, e)

  protected def debug(message: => String): Unit = log.log.debug(message)

  protected def debug(message: => String, e: Throwable): Unit = log.log.debug(message, e)

  protected def trace(message: => String): Unit = log.log.trace(message)

  protected def trace(message: => String, e: Throwable): Unit = log.log.trace(message, e)

}

