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

package org.apache.activemq.apollo.stomp

import java.net.{Socket, InetSocketAddress}
import org.apache.activemq.apollo.broker.ProtocolException
import org.fusesource.hawtbuf.AsciiBuffer
import _root_.org.fusesource.hawtbuf.{ByteArrayOutputStream => BAOS}
import java.io._

/**
 * A simple Stomp client used for testing purposes
 */
  class StompClient {

    var socket:Socket = new Socket
    var out:OutputStream = null
    var in:InputStream = null
    val bufferSize = 64*1204

    def open(host: String, port: Int) = {
      socket = new Socket
      socket.connect(new InetSocketAddress(host, port))
      socket.setSoLinger(true, 0)
      out = new BufferedOutputStream(socket.getOutputStream, bufferSize)
      in = new BufferedInputStream(socket.getInputStream, bufferSize)
    }

    def close() = {
      socket.close
    }

    def send(frame:String) = {
      out.write(frame.getBytes("UTF-8"))
      out.write(0)
      out.write('\n')
      out.flush
    }

    def send(frame:Array[Byte]) = {
      out.write(frame)
      out.write(0)
      out.write('\n')
      out.flush
    }

    def skip():Unit = {
      var c = in.read
      while( c >= 0 ) {
        if( c==0 ) {
          return
        }
        c = in.read()
      }
      throw new EOFException()
    }

    def receive():String = {
      val buffer = new BAOS()
      var c = in.read
      while( c >= 0 ) {
        if( c==0 ) {
          return new String(buffer.toByteArray, "UTF-8")
        }
        buffer.write(c)
        c = in.read()
      }
      throw new EOFException()
    }

    def receiveAscii():AsciiBuffer = {
      val buffer = new BAOS()
      var c = in.read
      while( c >= 0 ) {
        if( c==0 ) {
          return buffer.toBuffer.ascii
        }
        buffer.write(c)
        c = in.read()
      }
      throw new EOFException()
    }

    def receive(expect:String):String = {
      val rc = receive()
      if( !rc.startsWith(expect) ) {
        throw new ProtocolException("Expected "+expect)
      }
      rc
    }

  }