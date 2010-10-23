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

import org.osgi.service.command.CommandSession
import org.apache.felix.gogo.commands.{Action, Option => option, Argument => argument, Command => command}
import org.apache.felix.gogo.runtime.shell.CommandShellImpl
import org.apache.karaf.shell.console.Main
import org.apache.karaf.shell.console.jline.Console
import jline.Terminal
import org.fusesource.jansi.Ansi
import java.io.{OutputStream, PrintStream, InputStream}

object Apollo {
  def main(args: Array[String]) = {
    Ansi.ansi()
    new Apollo().run(args)
  }

  // Some ANSI helpers...
  def ANSI(value:Any) =  "\u001B["+value+"m"
  val BOLD =  ANSI(1)
  val RESET = ANSI(0)
}

@command(scope="apollo", name = "apollo", description = "The Apollo Command line tool")
class Apollo extends Main with Action {
  import Apollo._

  setUser("me")
  setApplication("apollo")

  var debug = false

  override def getDiscoveryResource = "META-INF/services/org.apache.activemq.apollo/commands.index"

  override def isMultiScopeMode() = false


  protected override def createConsole(commandProcessor: CommandShellImpl, in: InputStream, out: PrintStream, err: PrintStream, terminal: Terminal)  = {
    new Console(commandProcessor, in, out, err, terminal, null) {
      protected override def getPrompt = BOLD+"apollo> "+RESET
      protected override def isPrintStackTraces = debug
      protected override def welcome = {
        val source = getClass().getResourceAsStream("banner.txt")
        copy(source, session.getConsole())
      }

      def copy(in: InputStream, out: OutputStream): Long = {
        var bytesCopied: Long = 0
        val buffer = new Array[Byte](8192)
        var bytes = in.read(buffer)
        while (bytes >= 0) {
          out.write(buffer, 0, bytes)
          bytesCopied += bytes
          bytes = in.read(buffer)
        }

        bytesCopied
      }

      protected override def setSessionProperties = {}
    }
  }

  @argument(name = "args", description = "apollo sub command arguments", multiValued=true)
  var args = Array[String]()

  def execute(session: CommandSession): AnyRef = {
    run(session, args)
    null
  }




}