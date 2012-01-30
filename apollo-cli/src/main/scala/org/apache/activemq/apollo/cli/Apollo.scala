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
package org.apache.activemq.apollo.cli

import org.apache.karaf.shell.console.Main
import org.apache.karaf.shell.console.jline.Console
import jline.Terminal
import org.fusesource.jansi.Ansi
import org.apache.activemq.apollo.util.FileSupport._
import org.apache.felix.service.command.CommandSession
import org.apache.felix.gogo.runtime.CommandProcessorImpl
import java.util.logging.LogManager
import java.io.{FileInputStream, File, PrintStream, InputStream}
import org.apache.log4j.PropertyConfigurator
import java.util.Properties
import org.apache.felix.gogo.commands.{Action, Option => option, Argument => argument, Command => command}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Apollo extends Apollo {
  def main(args: Array[String]) = {

    if( System.getProperty("java.util.logging.config.file") == null ) {
      val home = System.getProperty("apollo.home", ".")
      var jul_config = new File(home) / "etc" / "jul.properties"

      val base = System.getProperty("apollo.base", ".")
      if ( base !=null ) {
        val file = new File(base) / "etc" / "jul.properties"
        if  ( file.exists()  ) {
          jul_config = file
        }
      }

      if( jul_config.exists() ) {
        using(new FileInputStream(jul_config)) { is =>
          LogManager.getLogManager.readConfiguration(is)
        }
      }
    }

    Ansi.ansi()
    Apollo.run(("apollo"::args.toList).toArray)
    System.exit(0)
  }

  // Some ANSI helpers...
  def ANSI(value:Any) =  "\u001B["+value+"m"
  val BOLD =  ANSI(1)
  val RESET = ANSI(0)

  var banner_displayed = false

  def print_banner(out: PrintStream) = {
    if( !banner_displayed ) {
      using(getClass().getResourceAsStream("banner.txt")) { source=>
        copy(source, out)
      }
      banner_displayed = true
    }
  }

  override def getDiscoveryResource = "META-INF/services/org.apache.activemq.apollo/apollo-main.index"

  def print_tips(out: PrintStream) = using(getClass().getResourceAsStream("tips.txt")) { source=>
    copy(source, out)
  }

}

@command(scope="apollo", name = "apollo", description = "The Apollo Command line tool")
class Apollo extends Main with Action {
  import Apollo._

  setUser("me")
  setApplication("apollo")

  var debug = false

  val is_apollo_broker = System.getProperty("apollo.base")!=null

  override def getDiscoveryResource = {
    if( is_apollo_broker ) {
      "META-INF/services/org.apache.activemq.apollo/apollo-broker-commands.index"
    } else {
      "META-INF/services/org.apache.activemq.apollo/apollo-commands.index"
    }

  }

  override def isMultiScopeMode() = false

  protected override def createConsole(impl: CommandProcessorImpl, in: InputStream, out: PrintStream, err: PrintStream, terminal: Terminal)  = {
    new Console(impl, in, out, err, terminal, null) {
      protected override def getPrompt = if (is_apollo_broker) {
        BOLD+"apollo-broker> "+RESET
      } else {
        BOLD+"apollo> "+RESET
      }
      protected override def welcome = {
        print_banner(session.getConsole)
        print_tips(session.getConsole)
      }

      protected override def setSessionProperties = {}

      protected override def getHistoryFile: File = {
        val default = (new File(System.getProperty("user.home"))/".apollo"/"apollo.history").getCanonicalPath
        new File(System.getProperty("apollo.history",default))
      }
    }
  }

  @option(name = "--log", description="The logging level use.")
  var log_level:String = "NONE"

  @argument(name = "args", description = "apollo sub command arguments", multiValued=true)
  var args:Array[String] = Array()

  def execute(session: CommandSession): AnyRef = {
    
    // Just in case your running a sub command an not
    // the broker
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
    
    run(session, args)
    null
  }


}