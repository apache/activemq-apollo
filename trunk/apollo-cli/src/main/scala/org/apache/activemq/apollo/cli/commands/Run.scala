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
import java.io.File
import org.apache.activemq.apollo.broker.{Broker, ConfigStore, FileConfigStore}
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.util.FileSupport._
import org.apache.activemq.apollo.cli.Apollo
import org.apache.activemq.apollo.util.{Log, LoggingReporter}
import org.apache.felix.service.command.CommandSession

/**
 * The apollo run command
 */
@command(scope="apollo", name = "run", description = "runs the broker instance")
class Run extends Action {
  
  @option(name = "--conf", description = "The Apollo configuration file.")
  var conf: File = _

  @option(name = "--tmp", description = "A temp directory.")
  var tmp: File = _

  def system_dir(name:String) = {
    val base_value = System.getProperty(name)
    if( base_value==null ) {
      error("The the %s system property is not set.".format(name))
    }
    val file = new File(base_value)
    if( !file.isDirectory  ) {
      error("The the %s system property is not set to valid directory path %s".format(name, base_value))
    }
    file
  }

  def execute(session: CommandSession):AnyRef = {

    try {

      val base = system_dir("apollo.base")

      if( conf == null ) {
        conf = base / "etc" / "apollo.xml"
      }

      if( !conf.exists ) {
        error("Configuration file'%s' does not exist.\n\nTry creating a broker instance using the 'apollo create' command.".format(conf));
      }

      if( System.getProperty("java.security.auth.login.config")==null ) {
        val login_config = conf.getParentFile / "login.config"
        if( login_config.exists ) {
          System.setProperty("java.security.auth.login.config", login_config.getCanonicalPath)
        }
      }

      if( tmp == null ) {
        tmp = base / "tmp"
        tmp.mkdirs
      }

      Apollo.print_banner(session.getConsole)

      // Load the configs and start the brokers up.
      session.getConsole.println("Loading configuration file '%s'.".format(conf))

      val store = new FileConfigStore
      store.file = conf
      ConfigStore() = store
      store.start

      val broker = new Broker()
      broker.config = store.load(true)
      broker.tmp = tmp
      broker.start()

      // wait forever...  broker will system exit.
      this.synchronized {
        this.wait
      }

    } catch {
      case x:Helper.Failure=>
        error(x.getMessage)
    }
    null
  }

}
