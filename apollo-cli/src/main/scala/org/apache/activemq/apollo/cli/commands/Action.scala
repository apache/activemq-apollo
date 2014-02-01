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
package org.apache.activemq.apollo.cli.commands

import java.io.{InputStream, PrintStream}
import io.airlift.command.OptionType
import io.airlift.command.Option
import java.util.Properties
import org.apache.log4j.PropertyConfigurator

/**
 */
trait Action {
  def execute(in:InputStream, out:PrintStream, err:PrintStream):Int
}

abstract class BaseAction extends Action {

  @Option(`type`=OptionType.GLOBAL, name=Array("--log"), description="The logging level use.")
  var log_level:String = "WARN"

  def init_logging = {
    // Just in case your running a sub command an not the broker
    var log_properties: Properties = new Properties()
    log_properties.put("log4j.appender.console", "org.apache.log4j.ConsoleAppender")
    log_properties.put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout")
    log_properties.put("log4j.appender.console.layout.ConversionPattern", "%-5p | %m%n")

    log_level = log_level.toUpperCase()
    log_level match {
      case "NONE" => log_properties.clear()
      log_properties.put("log4j.rootLogger", "FATAL")
      case "FATAL" =>
        log_properties.put("log4j.rootLogger", log_level+", console")
      case "ERROR" =>
        log_properties.put("log4j.rootLogger", log_level+", console")
      case "WARN" =>
        log_properties.put("log4j.rootLogger", log_level+", console")
      case "INFO" =>
        log_properties.put("log4j.rootLogger", log_level+", console")
      case "DEBUG" =>
        log_properties.put("log4j.rootLogger", log_level+", console")
      case "TRACE" =>
        log_properties.put("log4j.rootLogger", log_level+", console")
    }
    PropertyConfigurator.configure(log_properties)
  }

}