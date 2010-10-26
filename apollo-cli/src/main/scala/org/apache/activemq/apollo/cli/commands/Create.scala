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

import org.apache.felix.gogo.commands.{Action, Option => option, Argument => argument, Command => command}
import org.osgi.service.command.CommandSession
import org.fusesource.jansi.Ansi
import org.fusesource.jansi.Ansi.Color._
import org.fusesource.jansi.Ansi.Attribute._
import Helper._
import java.io.{FileOutputStream, File}

/**
 * The apollo create command
 */
@command(scope="apollo", name = "create", description = "creates a new broker instance")
class Create extends Action {

  @argument(name = "directory", description = "The instance directory to hold the broker's configuration and data", index=0, required=true)
  var directory:File = _

  @option(name = "--force", description = "Overwrite configuration at destination directory")
  var force = false

  def execute(session: CommandSession) = {

    def println(value:Any) = session.getConsole.println(value)
    try {

      val bin = directory / "bin"
      bin.mkdirs

      var target = bin / "apollo-broker"
      write("bin/apollo-broker", target)
      target.setExecutable(true)

      target = bin / "apollo-broker.cmd"
      write("bin/apollo-broker.cmd", target)

      val etc = directory / "etc"
      etc.mkdirs

      target = etc / "log4j.properties"
      write("etc/log4j.properties", target)

      target = etc / "apollo.xml"
      write("etc/apollo.xml", target)

      val data = directory / "data"
      data.mkdirs
      
      val log = directory / "log"
      log.mkdirs

    } catch {
      case x:Helper.Failure=>
        println(ansi.a(INTENSITY_BOLD).fg(RED).a("ERROR: ").reset.a(x.getMessage))
    }


    null
  }

  def write(source:String, target:File) = {
    if( target.exists && !force ) {
      error("The file '%s' already exists.  Use --force to overwrite.".format(target))
    }
    val out = new FileOutputStream(target)
    val in = getClass.getResourceAsStream(source)
    copy(in, out)
    close(out)
  }
}