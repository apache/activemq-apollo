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

import org.fusesource.jansi.Ansi
import org.fusesource.jansi.Ansi.Attribute._
import java.io.{OutputStream, InputStream, File}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object Helper {

  def ansi= new Ansi()

  class Failure(msg:String, e:Throwable) extends RuntimeException(msg, e) {
    def this(msg:String) = this(msg,null)
  }

  def error(value:Any) = throw new Failure(value.toString)

  def bold(v:String) = ansi.a(INTENSITY_BOLD).a(v).reset

}


