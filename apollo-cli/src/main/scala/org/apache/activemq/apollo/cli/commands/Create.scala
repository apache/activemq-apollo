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
import java.io._
import org.apache.activemq.apollo.util.FileSupport._
import java.nio.charset.Charset
import java.util.regex.{Pattern, Matcher}

object Create {
  val IS_WINDOWS = System.getProperty("os.name").toLowerCase().trim().startsWith("win");
}

/**
 * The apollo create command
 */
@command(scope="apollo", name = "create", description = "creates a new broker instance")
class Create extends Action {

  import Create._

  @argument(name = "directory", description = "The instance directory to hold the broker's configuration and data", index=0, required=true)
  var directory:File = _

  @option(name = "--host", description = "The host name of the broker")
  var host:String = _

  @option(name = "--force", description = "Overwrite configuration at destination directory")
  var force = false

  var version:String = "${version}"

  def execute(session: CommandSession) = {

    val version = using(getClass().getResourceAsStream("version.txt")) { source=>
      read_text(source)
    }

    def println(value:Any) = session.getConsole.println(value)
    try {
      println("Creating apollo instance at: %s".format(directory))

      if( host == null ) {
        host = directory.getName
      }

      val bin = directory / "bin"
      bin.mkdirs

      val etc = directory / "etc"
      etc.mkdirs

      var target = etc / "log4j.properties"
      write("etc/log4j.properties", target)

      target = etc / "users.properties"
      write("etc/users.properties", target)

      target = etc / "groups.properties"
      write("etc/groups.properties", target)

      target = etc / "login.config"
      write("etc/login.config", target)

      // Generate a keystore with a new key
      println("Generating ssl keystore...")
      val ssl = system(etc, Array(
        "keytool", "-genkey",
        "-storetype", "JKS",
        "-storepass", "password",
        "-keystore", "keystore",
        "-keypass", "password",
        "-alias", host,
        "-keyalg", "RSA",
        "-keysize", "4096",
        "-dname", "cn=%s".format(host),
        "-validity", "3650"))==0

      target = etc / "apollo.xml"
      if( ssl ) {
        write("etc/apollo-ssl.xml", target, true)
      } else {
        println("WARNNIG: Could not generate the keystore, make sure the keytool command is in your PATH")
        write("etc/apollo.xml", target, true)
      }


      if( IS_WINDOWS ) {
        target = bin / "apollo-broker.cmd"
        write("bin/apollo-broker.cmd", target)
      } else {
        target = bin / "apollo-broker"
        write("bin/apollo-broker", target)
        setExecutable(target)
      }

      val data = directory / "data"
      data.mkdirs
      
      val log = directory / "log"
      log.mkdirs

      val tmp = directory / "tmp"
      log.mkdirs

    } catch {
      case x:Helper.Failure=>
        println(ansi.a(INTENSITY_BOLD).fg(RED).a("ERROR: ").reset.a(x.getMessage))
    }


    null
  }

  def write(source:String, target:File, filter:Boolean=false, target_encoding:String=null) = {
    if( target.exists && !force ) {
      error("The file '%s' already exists.  Use --force to overwrite.".format(target))
    }
    if( filter || target_encoding!=null ) {

      val encoding = if( target_encoding!=null ) {
        target_encoding
      } else {
        "UTF-8"
      }

      val out = new ByteArrayOutputStream()
      using(getClass.getResourceAsStream(source)) { in=>
        copy(in, out)
      }

      // Yes this is reading in UTF-8 from the jar file..
      var content = new String(out.toByteArray, "UTF-8")

      if( filter ) {
        content = content.replaceAll(Pattern.quote("${host}"), Matcher.quoteReplacement(host))
        content = content.replaceAll(Pattern.quote("${version}"), Matcher.quoteReplacement(version))
      }

      // and then writing out in the new target encoding.
      val in = new ByteArrayInputStream(content.getBytes(encoding))

      using(new FileOutputStream(target)) { out=>
        copy(in, out)
      }

    } else {
      using(new FileOutputStream(target)) { out=>
        using(getClass.getResourceAsStream(source)) { in=>
          copy(in, out)
        }
      }
    }
  }


  def setExecutable(path:File) = if( !IS_WINDOWS ) {
    try {
        system(path.getParentFile(), Array("chmod", "a+x", path.getName))
    } catch {
      case x =>
    }
  }

  def system(wd:File, command:Array[String]) = {
    val process = Runtime.getRuntime.exec(command, null, wd);
    def drain(is:InputStream, os:OutputStream) = {
      new Thread(command.mkString(" ")) {
        setDaemon(true)
        override def run: Unit = {
          try {
            val buffer = new Array[Byte](1024 * 4)
            var c = is.read(buffer)
            while (c >= 0) {
              os.write(buffer, 0, c);
              c = is.read(buffer)
            }
          } catch {
            case x =>
          }
        }
      }.start
    }
    process.getOutputStream.close;
    drain(process.getInputStream, System.out)
    drain(process.getErrorStream, System.err)
    process.waitFor
    process.exitValue
  }

}