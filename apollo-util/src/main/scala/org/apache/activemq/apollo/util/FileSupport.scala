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
package org.apache.activemq.apollo.util

import tools.nsc.io.Path
import java.io.{OutputStream, InputStream, File}

object FileSupport {

  def recursiveCopy(source: Path, target: Path) : Unit = {
    require(source.isDirectory, source.path + " must be a directory.")
    if ( !target.exists ) {
      target.toDirectory.createDirectory()
    }

    def createOrCopy(file: Path) : Unit = {
      val newTarget = target / FileSupport.toDirectory(file.name)
      if (file.isDirectory) {
        recursiveCopy(file.toDirectory, newTarget)
      } else {
        file.toFile.copyTo(newTarget)
      }
    }
    source.toDirectory.list.foreach(createOrCopy)
  }

  def toDirectory(name: String) = {
    new tools.nsc.io.Directory(new File(name))
  }

  def toFile(name: String) = {
    new tools.nsc.io.File(new File(name))
  }


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