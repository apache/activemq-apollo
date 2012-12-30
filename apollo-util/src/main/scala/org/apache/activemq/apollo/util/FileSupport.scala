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
import org.fusesource.hawtdispatch._
import language.implicitConversions

object FileSupport {

  implicit def to_rich_file(file:File):RichFile = new RichFile(file)


  def system_dir(name:String) = {
    val base_value = System.getProperty(name)
    if( base_value==null ) {
      sys.error("The the %s system property is not set.".format(name))
    }
    val file = new File(base_value)
    if( !file.isDirectory  ) {
      sys.error("The the %s system property is not set to valid directory path %s".format(name, base_value))
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

    def list_files:Array[File] = {
      Option(self.listFiles()).getOrElse(Array())
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

    def read_text(charset:String="UTF-8"): String = {
      using(new FileInputStream(self)) { in =>
        FileSupport.read_text(in, charset)
      }
    }

    def read_bytes: Array[Byte] = {
      using(new FileInputStream(self)) { in =>
        FileSupport.read_bytes(in)
      }
    }

    def write_bytes(data:Array[Byte]):Unit = {
      using(new FileOutputStream(self)) { out =>
        FileSupport.write_bytes(out, data)
      }
    }

    def write_text(data:String, charset:String="UTF-8"):Unit = {
      using(new FileOutputStream(self)) { out =>
        FileSupport.write_text(out, data, charset)
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
      try { closable.close  }  catch { case ignore:Throwable =>  }
    }
  }

  def read_text(in: InputStream, charset:String="UTF-8"): String = {
    new String(read_bytes(in), charset)
  }

  def read_bytes(in: InputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    copy(in, out)
    out.toByteArray
  }

  def write_text(out: OutputStream, value: String, charset:String="UTF-8"): Unit = {
    write_bytes(out, value.getBytes(charset))
  }

  def write_bytes(out: OutputStream, data: Array[Byte]): Unit = {
    copy(new ByteArrayInputStream(data), out)
  }

}

object ProcessSupport {
  import FileSupport._

  implicit def to_rich_process_builder(self:ProcessBuilder):RichProcessBuilder = new RichProcessBuilder(self)

  case class RichProcessBuilder(self:ProcessBuilder) {

    def start(out:OutputStream=null, err:OutputStream=null, in:InputStream=null) = {
      self.redirectErrorStream(out == err)
      val process = self.start
      if( in!=null ) {
        ApolloThreadPool.INSTANCE {
          try {
            using(process.getOutputStream) { out =>
              FileSupport.copy(in, out)
            }
          } catch {
            case _ =>
          }
        }
      } else {
        process.getOutputStream.close
      }

      if( out!=null ) {
        ApolloThreadPool.INSTANCE {
          try {
            using(process.getInputStream) { in =>
              FileSupport.copy(in, out)
            }
          } catch {
            case _ =>
          }
        }
      } else {
        process.getInputStream.close
      }

      if( err!=null && err!=out ) {
        ApolloThreadPool.INSTANCE {
          try {
            using(process.getErrorStream) { in =>
              FileSupport.copy(in, err)
            }
          } catch {
            case _ =>
          }
        }
      } else {
        process.getErrorStream.close
      }
      process
    }

  }

  implicit def to_rich_process(self:Process):RichProcess = new RichProcess(self)

  case class RichProcess(self:Process) {
    def on_exit(func: (Int)=>Unit) = ApolloThreadPool.INSTANCE {
      self.waitFor
      func(self.exitValue)
    }
  }

  implicit def to_process_builder(args:Seq[String]):ProcessBuilder = new ProcessBuilder().command(args : _*)

  def launch(command:String*)(func: (Int, Array[Byte], Array[Byte])=>Unit ):Unit = launch(command)(func)
  def launch(p:ProcessBuilder, in:InputStream=null)(func: (Int, Array[Byte], Array[Byte]) => Unit):Unit = {
    val out = new ByteArrayOutputStream
    val err = new ByteArrayOutputStream
    p.start(out, err, in).on_exit { code=>
      func(code, out.toByteArray, err.toByteArray)
    }
  }

  def system(command:String*):(Int, Array[Byte], Array[Byte]) = system(command)
  def system(p:ProcessBuilder, in:InputStream=null):(Int, Array[Byte], Array[Byte]) = {
    val out = new ByteArrayOutputStream
    val err = new ByteArrayOutputStream
    val process = p.start(out, err, in)
    process.waitFor
    (process.exitValue, out.toByteArray, err.toByteArray)
  }

}