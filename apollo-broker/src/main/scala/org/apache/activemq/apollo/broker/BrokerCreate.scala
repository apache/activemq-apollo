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

import java.io._
import org.apache.activemq.apollo.util.FileSupport._
import java.util.regex.{Pattern, Matcher}
import org.apache.activemq.apollo.broker.Broker
import scala.Predef._

class BrokerCreate {

  var directory:File = _
  var host:String = _
  var force = false
  var base: File = _
  var home: File = new File(System.getProperty("apollo.home"))
  var with_ssl = true
  var encoding = "UTF-8"

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


  val IS_WINDOWS = System.getProperty("os.name").toLowerCase().trim().startsWith("win");
  val IS_CYGWIN = IS_WINDOWS && System.getenv("OSTYPE") == "cygwin";

  def run(out:PrintStream, err:PrintStream) = {

    try {
      out.println("Creating apollo instance at: %s".format(directory))

      if( host == null ) {
        host = directory.getName
      }

      val etc = directory / "etc"
      etc.mkdirs

      if (create_log_config) {
        write("etc/log4j.properties", etc/"log4j.properties", true, true)
      }

      if ( create_login_config ) {
        write("etc/users.properties", etc/"users.properties", false, true)
        write("etc/groups.properties", etc/"groups.properties", false, true)
        write("etc/login.config", etc/"login.config", false, true)
        write("etc/black-list.txt", etc/"black-list.txt", false, true)
      }

      // Generate a keystore with a new key
      val ssl = with_ssl && {
        out.println("Generating ssl keystore...")
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
          out.println("WARNING: Could not generate the keystore, make sure the keytool command is in your PATH")
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
        }

        if( !IS_WINDOWS || IS_CYGWIN ) {
          write("bin/apollo-broker", bin/"apollo-broker", true, false, true)
          setExecutable(bin/"apollo-broker")

          write("bin/apollo-broker-service", bin/"apollo-broker-service", true, false, true)
          setExecutable(bin/"apollo-broker-service")
        }

        out.println("")
        out.println("You can now start the broker by executing:  ")
        out.println("")
        out.println("   \"%s\" run".format(cp(bin/"apollo-broker", true)))

        val service = bin / "apollo-broker-service"
        out.println("")

        if( !IS_WINDOWS || IS_CYGWIN ) {

          // Does it look like we are on a System V init system?
          if( new File("/etc/init.d/").isDirectory ) {

            out.println("Or you can setup the broker as system service and run it in the background:")
            out.println("")
            out.println("   sudo ln -s \"%s\" /etc/init.d/".format(service.getCanonicalPath))
            out.println("   /etc/init.d/apollo-broker-service start")
            out.println("")

          } else {

            out.println("Or you can run the broker in the background using:")
            out.println("")
            out.println("   \"%s\" start".format(cp(service,true)))
            out.println("")
          }

        }
        if ( IS_WINDOWS ) {

          out.println("Or you can setup the broker as Windows service and run it in the background:")
          out.println("")
          out.println("   \"%s\" install".format(cp(service,true)))
          out.println("   \"%s\" start".format(cp(service,true)))
          out.println("")

        }
      }
      0
    } catch {
      case x:Exception =>
        err.println("ERROR: "+x.getMessage)
        1
    }
  }

  def cp(value:String, unixPaths:Boolean):String = cp(new File(value), unixPaths)
  def cp(value:File, unixPaths:Boolean):String = {
    if( unixPaths && IS_CYGWIN ) {
      import scala.sys.process._
      Seq("cygpath", value.getCanonicalPath).!!.trim
    } else {
      value.getCanonicalPath
    }
  }

  def write(source:String, target:File, filter:Boolean=false, text:Boolean=false, unixTarget:Boolean=false) = {
    if( target.exists && !force ) {
      error("The file '%s' already exists.  Use --force to overwrite.".format(target))
    }
    if( filter || text ) {

      val out = new ByteArrayOutputStream()
      using(this.getClass.getResourceAsStream(source)) { in=>
        copy(in, out)
      }

      // Yes this is reading in UTF-8 from the jar file..
      var content = new String(out.toByteArray, "UTF-8")

      if( filter ) {
        def replace(key:String, value:String) = {
          content = content.replaceAll(Pattern.quote(key), Matcher.quoteReplacement(value))
        }

        replace("${user}", System.getProperty("user.name",""))
        replace("${host}", host)
        replace("${version}", Broker.version)
        if( home !=null ) {
          replace("${home}", cp(home, unixTarget))
        }
        replace("${base}", cp(directory, unixTarget))
        replace("${java.home}", cp(System.getProperty("java.home"), unixTarget))
        replace("${store_config}", store_config)

        if( base !=null ) {
          replace("${apollo.base}", cp(base, unixTarget))
        }

        replace("${broker_security_config}", broker_security_config)
        replace("${host_security_config}", host_security_config)
      }

      // and then writing out in the new target encoding..  Let's also replace \n with the values
      // that is correct for the current platform.
      var separator = if ( unixTarget && IS_CYGWIN ) {
        "\n"
      } else {
        System.getProperty("line.separator")
      }
      val in = new ByteArrayInputStream(content.replaceAll("""\r?\n""",  Matcher.quoteReplacement(separator)).getBytes(encoding))

      using(new FileOutputStream(target)) { out=>
        copy(in, out)
      }

    } else {
      using(new FileOutputStream(target)) { out=>
        using(this.getClass.getResourceAsStream(source)) { in=>
          copy(in, out)
        }
      }
    }
  }

  def can_load(name:String) = {
    try {
      this.getClass.getClassLoader.loadClass(name)
      true
    } catch {
      case _:Throwable => false
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
      case _:Throwable =>
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
            case _:Throwable =>
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
