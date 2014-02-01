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

import java.io._
import scala.Predef._
import org.apache.activemq.apollo.broker.BrokerCreate
import io.airlift.command.{Arguments, Command, Option}

/**
 * The apollo create command
 */
@Command(name = "create", description = "creates a new broker instance")
class Create extends BaseAction {

  @Arguments(description = "The instance directory to hold the broker's configuration and data", required=true)
  var directory:File = _

  @Option(name = Array("--host"), description = "The host name of the broker")
  var host:String = _

  @Option(name = Array("--force"), description = "Overwrite configuration at destination directory")
  var force = false

  @Option(name = Array("--home"), description = "Directory where apollo is installed")
  var home: File = _

  @Option(name = Array("--with-ssl"), description = "Generate an SSL enabled configuration")
  var with_ssl = true

  @Option(name = Array("--encoding"), description = "The encoding that text files should use")
  var encoding:String = _

  def execute(in:InputStream, out:PrintStream, err:PrintStream) = {
    init_logging
    val create = new BrokerCreate
    if( directory!=null ) create.directory = directory
    if( host!=null ) create.host = host
    create.force = force
    if( home!=null ) create.home = home
    create.with_ssl = with_ssl
    if( encoding!=null ) create.encoding = encoding
    create.run(out, err)
    0
  }
}