package org.apache.activemq.apollo.cli.commands

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
import org.apache.felix.gogo.commands.{Action, Option => option, Argument => argument, Command => command}
import org.osgi.service.command.CommandSession
import java.io.File
import org.apache.activemq.apollo.broker.FileConfigStore
import org.apache.activemq.apollo.util.FileSupport._
import org.apache.activemq.apollo.util.OptionSupport._
import org.apache.activemq.apollo.util.Logging
import org.apache.activemq.apollo.dto.WebAdminDTO
import org.apache.commons.codec.binary.Base64
import java.net.{HttpURLConnection, URL}

/**
 * The apollo stop command
 */
@command(scope="apollo", name = "stop", description = "stops the broker instance")
class Stop extends Action with Logging {

  @option(name = "--conf", description = "The Apollo configuration file.")
  var conf: File = _

  @option(name = "--user", description = "The user id")
  var user: String = _

  @option(name = "--password", description = "The user password")
  var password: String = _

  def execute(session: CommandSession):AnyRef = {

    try {

      val base = system_dir("apollo.base")

      if( conf == null ) {
        conf = base / "etc" / "apollo.xml"
      }

      if( !conf.exists ) {
        error("Configuration file'%s' does not exist.\n\nTry creating a broker instance using the 'apollo create' command.".format(conf));
      }

      val config = new FileConfigStore(conf).load(true)

      val web_admin = config.web_admin.getOrElse(new WebAdminDTO)
      if( web_admin.enabled.getOrElse(true) ) {

        val prefix = web_admin.prefix.getOrElse("/").stripSuffix("/")
        val port = web_admin.port.getOrElse(8080)
        val host = web_admin.host.getOrElse("127.0.0.1")

        val auth = (user, password) match {
          case (null,null)=> None
          case (null,p)=> Some(":"+p)
          case (u,null)=> Some(u+":")
          case (u,p)=> Some(u+":"+p)
        }

        val connection = new URL(
          "http://%s:%s%s/command/shutdown".format(host, port, prefix)
        ).openConnection.asInstanceOf[HttpURLConnection]
        connection.setRequestMethod("POST");
        auth.foreach{x=> connection.setRequestProperty("Authorization", "Basic "+new String(Base64.encodeBase64(x.getBytes("UTF-8")), "UTF-8")) }
        connection.getContent

      }

    } catch {
      case x:Helper.Failure=>
        error(x.getMessage)
    }
    null
  }


}