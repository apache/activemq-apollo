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

import org.apache.activemq.apollo.util.FileSupport._
import java.util.logging.LogManager
import java.io._
import java.util.Properties
import io.airlift.command.{ParseArgumentsUnexpectedException, Cli}
import org.apache.activemq.apollo.cli.commands.{HelpAction, Action}
import org.fusesource.jansi.Ansi

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Apollo extends Apollo {

  def main(args: Array[String]) = {

    // Lets configure java.util.logging
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
      } else {
        LogManager.getLogManager.readConfiguration(new ByteArrayInputStream("handlers=\n".getBytes()))
      }
    }

    Ansi.ansi()
    System.exit(Apollo.run(System.in, System.out, System.err, args))
  }

  var banner_displayed = false
  def print_banner(out: PrintStream) = {
    if( !banner_displayed ) {
      using(getClass().getResourceAsStream("banner.txt")) { source=>
        copy(source, out)
      }
      banner_displayed = true
    }
  }
}
class Apollo {

  def properties(is:InputStream):Properties = {
    try {
      val p = new Properties()
      p.load(is);
      return p
    } finally {
      try {
        is.close()
      } catch {
        case _:Throwable =>
      }
    }
  }

  def action_classes(resourceFile:String) = {
    import collection.JavaConversions._
    val loader = this.getClass().getClassLoader
    properties(loader.getResourceAsStream(resourceFile)).keys.map { next =>
      loader.loadClass(next.asInstanceOf[String]).asInstanceOf[Class[Action]]
    }.toArray
  }

  def run(in:InputStream, out:PrintStream, err:PrintStream, args: Array[String]):Int = {

    val is_apollo_broker = System.getProperty("apollo.base")!=null
    val command_name = if( is_apollo_broker ) {
      "apollo-broker"
    } else {
      "apollo"
    }

    var builder = Cli.builder[Action](command_name)
        .withDescription("The Apollo command line tool")
        .withDefaultCommand(classOf[HelpAction])

    for( action <- action_classes("META-INF/services/org.apache.activemq.apollo/" + command_name + "-commands.index") ) {
      builder.withCommand(action)
    }

//        builder.withGroup("remote")
//                .withDescription("Manage set of tracked repositories")
//                .withDefaultCommand(RemoteShow.class)
//                .withCommands(RemoteShow.class, RemoteAdd.class);

    var parser = builder.build()

    val action = try {
      parser.parse(args: _*)
    } catch {
      case e:ParseArgumentsUnexpectedException =>
        err.println(e.getMessage)
        out.println()
        parser.parse("help").execute(in, out, err)
        return 1;
    }
    action.execute(in, out, err)
  }

}