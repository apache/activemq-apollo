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
import org.fusesource.jansi.Ansi.Color._
import org.fusesource.jansi.Ansi.Attribute._
import Helper._
import java.io._
import org.apache.activemq.apollo.util.FileSupport._
import java.util.regex.{Pattern, Matcher}
import org.apache.felix.service.command.CommandSession
import org.apache.activemq.apollo.broker.Broker

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

  @option(name = "--home", description = "Directory where apollo is installed")
  val home: String = System.getProperty("apollo.home")

  @option(name = "--with-ssl", description = "Generate an SSL enabled configuraiton")
  val with_ssl = true
  
  var broker_security_config =
  """
  <authentication domain="apollo"/>
  <!-- Give admins full access -->
  <access_rule allow="admins" action="*"/>
  <access_rule allow="*" action="connect" kind="connector"/>
  """

  var host_security_config =
    """<!-- Uncomment to disable security for the virtual host -->
    <!-- <authentication enabled="false"/> -->
    <access_rule allow="users" action="connect create destroy send receive consume"/>
    """

  var create_login_config = true
  var create_log_config = true

  def execute(session: CommandSession) = {

    def println(value:Any) = session.getConsole.println(value)

    try {
      println("Creating apollo instance at: %s".format(directory))

      if( host == null ) {
        host = directory.getName
      }

      val etc = directory / "etc"
      etc.mkdirs

      if (create_log_config) {
        write("etc/log4j.properties", etc/"log4j.properties")
      }

      if ( create_login_config ) {
        write("etc/users.properties", etc/"users.properties")
        write("etc/groups.properties", etc/"groups.properties")
        write("etc/login.config", etc/"login.config")
        write("etc/black-list.txt", etc/"black-list.txt")
      }

      // Generate a keystore with a new key
      val ssl = with_ssl && {
        println("Generating ssl keystore...")
        val rc = system(etc, Array(
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
        if(!rc) {
          println("WARNNIG: Could not generate the keystore, make sure the keytool command is in your PATH")
        }
        rc
      }

      if( ssl ) {
        write("etc/apollo-ssl.xml", etc/"apollo.xml", true)
      } else {
        write("etc/apollo.xml", etc/"apollo.xml", true)
      }

      val data = directory / "data"
      data.mkdirs

      val tmp = directory / "tmp"
      tmp.mkdirs

      // home is set to null if executing within an OSGi env,
      // it's a hint to not generate startup scripts.
      if ( home!=null ) {
        val log = directory / "log"
        log.mkdirs

        val bin = directory / "bin"
        bin.mkdirs

        if( IS_WINDOWS ) {
          write("bin/apollo-broker.cmd", bin/"apollo-broker.cmd", true)
          write("bin/apollo-broker-service.exe", bin/"apollo-broker-service.exe")
          write("bin/apollo-broker-service.xml", bin/"apollo-broker-service.xml", true)
        } else {
          write("bin/apollo-broker", bin/"apollo-broker", true)
          setExecutable(bin/"apollo-broker")

          write("bin/apollo-broker-service", bin/"apollo-broker-service", true)
          setExecutable(bin/"apollo-broker-service")
        }

        println("")
        println("You can now start the broker by executing:  ")
        println("")
        println("   \"%s\" run".format((bin/"apollo-broker").getCanonicalPath))

        val service = bin / "apollo-broker-service"
        println("")

        if( !IS_WINDOWS ) {

          // Does it look like we are on a System V init system?
          if( new File("/etc/init.d/").isDirectory ) {

            println("Or you can setup the broker as system service and run it in the background:")
            println("")
            println("   sudo ln -s \"%s\" /etc/init.d/".format(service.getCanonicalPath))
            println("   /etc/init.d/apollo-broker-service start")

          } else {

            println("Or you can run the broker in the background using:")
            println("")
            println("   \"%s\" start".format(service.getCanonicalPath))

          }

        } else {

          println("Or you can setup the broker as system service and run it in the background:")
          println("")
          println("   \"%s\" install".format(service.getCanonicalPath))
          println("   \"%s\" start".format(service.getCanonicalPath))

        }
        println("")
      }


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
        def replace(key:String, value:String) = {
          content = content.replaceAll(Pattern.quote(key), Matcher.quoteReplacement(value))
        }
        def cp(value:String) = new File(value).getCanonicalPath

        replace("${user}", System.getProperty("user.name",""))
        replace("${host}", host)
        replace("${version}", Broker.version)
        if( home !=null ) {
          replace("${home}", cp(home))
        }
        replace("${base}", directory.getCanonicalPath)
        replace("${java.home}", cp(System.getProperty("java.home")))
        replace("${store_config}", store_config)

        replace("${broker_security_config}", broker_security_config)
        replace("${host_security_config}", host_security_config)
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

  def can_load(name:String) = {
    try {
      getClass.getClassLoader.loadClass(name)
      true
    } catch {
      case _ => false
    }
  }

  def store_config = {
    if( can_load("org.apache.activemq.apollo.broker.store.leveldb.LevelDBStore")
        && ( can_load("org.fusesource.leveldbjni.JniDBFactory")
        || can_load("org.iq80.leveldb.impl.Iq80DBFactory"))) {
    """<!-- You can delete this element if you want to disable persistence for this virtual host -->
    <leveldb_store directory="${apollo.base}/data"/>
    """
    } else if( can_load("com.sleepycat.je.Environment") ) {
    """<!-- You can delete this element if you want to disable persistence for this virtual host -->
    <bdb_store directory="${apollo.base}/data"/>
    """
    } else if( can_load("jdbm.RecordManagerFactory") ) {
    """<!-- You can delete this element if you want to disable persistence for this virtual host -->
    <jdbm2_store directory="${apollo.base}/data"/>
    """
    } else {
    """<!-- Perisistence disabled because no store implementations were found on the classpath -->
    <!-- <bdb_store directory="${apollo.base}/data"/> -->
    """
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