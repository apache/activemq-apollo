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

import _root_.java.lang.{String}
import java.util.{LinkedHashMap}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ReporterLevel extends Enumeration {
  type ReporterLevel = Value
  val INFO, WARN, ERROR = Value

  class RichReporterLevel(self:ReporterLevel) {
    def | (other:ReporterLevel):ReporterLevel = {
      if( other > self  ) {
        other
      } else {
        self
      }
    }
  }

  implicit def toRichReporterLevel(level:ReporterLevel) = new RichReporterLevel(level)
}
import ReporterLevel._

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Reporter {
  def report(level:ReporterLevel, message:String) = {}
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait LoggingReporter extends Logging with Reporter {
  override def report(level:ReporterLevel, message:String) = {
    level match {
      case INFO=>
        info(message)
      case WARN=>
        warn(message)
      case ERROR=>
        error(message)
    }
  }
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class Reporting(reporter:Reporter) {
  var result = INFO

  protected def warn(msg:String) = {
    reporter.report(WARN, msg)
    result |= WARN
  }

  protected def error(msg:String) = {
    reporter.report(ERROR, msg)
    result |= ERROR
  }

  protected def info(msg:String) = {
    reporter.report(INFO, msg)
    result |= INFO
  }

  protected def empty(value:String) = value==null || value.isEmpty

}