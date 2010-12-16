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
import org.apache.activemq.apollo.util.Logging
import org.apache.activemq.apollo.broker.security.EncryptionSupport
import org.jasypt.properties.PropertyValueEncryptionUtils

/**
 * The apollo encrypt command
 */
@command(scope="apollo", name = "decrypt", description = "decrypts a value")
class Decrypt extends Action with Logging {

  @argument(name = "value", description = "The value to decrypt", index=0, required=true)
  var value:String = _

  def execute(session: CommandSession):AnyRef = {
    try {
      val unwrapped = if ( value.startsWith("ENC(") && value.endsWith(")") ) {
        value.stripPrefix("ENC(").stripSuffix(")")
      } else {
        value
      }
      session.getConsole.println(EncryptionSupport.encryptor.decrypt(unwrapped));
    } catch {
      case x:Helper.Failure=> error(x.getMessage)
    }
    null
  }


}