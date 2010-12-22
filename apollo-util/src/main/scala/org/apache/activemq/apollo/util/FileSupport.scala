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

import java.io._

object FileSupport {

  implicit def to_rich_file(file:File):RichFile = new RichFile(file)


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

  case class RichFile(self:File) {

    def / (path:String) = new File(self, path)

    def copy_to(target:File) = {
      using(new FileOutputStream(target)){ os=>
        using(new FileInputStream(self)){ is=>
          FileSupport.copy(is, os)
        }
      }
    }

    def recursive_list:List[File] = {
      if( self.isDirectory ) {
        self :: self.listFiles.toList.flatten( _.recursive_list )
      } else {
        self :: Nil
      }
    }

    def recursive_delete: Unit = {
      if( self.exists ) {
        if( self.isDirectory ) {
          self.listFiles.foreach(_.recursive_delete)
        }
        self.delete
      }
    }

    def recursive_copy_to(target: File) : Unit = {
      if (self.isDirectory) {
        target.mkdirs
        self.listFiles.foreach( file=> file.recursive_copy_to( target / file.getName) )
      } else {
        self.copy_to(target)
      }
    }

  }

  /**
   * Returns the number of bytes copied.
   */
  def copy(in: InputStream, out: OutputStream): Long = {
    var bytesCopied: Long = 0
    val buffer = new Array[Byte](8192)
    var bytes = in.read(buffer)
    while (bytes >= 0) {
      out.write(buffer, 0, bytes)
      bytesCopied += bytes
      bytes = in.read(buffer)
    }
    bytesCopied
  }

  def using[R,C <: Closeable](closable: C)(proc: C=>R) = {
    try {
      proc(closable)
    } finally {
      try { closable.close  }  catch { case ignore =>  }
    }
  }

  def read_text(in: InputStream, charset:String="UTF-8"): String = {
    val out = new ByteArrayOutputStream()
    copy(in, out)
    new String(out.toByteArray, charset)
  }

}