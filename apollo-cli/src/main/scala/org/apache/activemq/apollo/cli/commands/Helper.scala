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

  class Failure(msg:String) extends RuntimeException(msg)

  def error(value:Any) = throw new Failure(value.toString)

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


  class RichFile(file:File) {
    def / (path:String) = new File(file, path)
  }
  implicit def toRichFile(file:File):RichFile = new RichFile(file)

  def bold(v:String) = ansi.a(INTENSITY_BOLD).a(v).reset


  def copy(in: InputStream, out: OutputStream): Long = {
    try {
      var bytesCopied: Long = 0
      val buffer = new Array[Byte](8192)
      var bytes = in.read(buffer)
      while (bytes >= 0) {
        out.write(buffer, 0, bytes)
        bytesCopied += bytes
        bytes = in.read(buffer)
      }
      bytesCopied
    } finally {
      try { in.close  }  catch { case ignore =>  }
    }
  }

  def close(out: OutputStream) = {
    try { out.close  }  catch { case ignore =>  }
  }

}


