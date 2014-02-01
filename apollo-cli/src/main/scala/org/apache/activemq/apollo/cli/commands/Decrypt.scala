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
import org.apache.activemq.apollo.util.Logging
import org.apache.activemq.apollo.broker.security.EncryptionSupport
import io.airlift.command.{Arguments, Command}
import java.io.{PrintStream, InputStream}

/**
 * The apollo encrypt command
 */
@Command(name = "decrypt", description = "decrypts a value")
class Decrypt extends BaseAction with Logging {

  @Arguments(description = "The value to decrypt", required=true)
  var value:String = _

  def execute(in:InputStream, out:PrintStream, err:PrintStream) = {
    init_logging
    try {
      val unwrapped = if ( value.startsWith("ENC(") && value.endsWith(")") ) {
        value.stripPrefix("ENC(").stripSuffix(")")
      } else {
        value
      }
      out.println(EncryptionSupport.encryptor.decrypt(unwrapped));
      0
    } catch {
      case x:Helper.Failure=>
        error(x.getMessage)
        2
    }
  }
}