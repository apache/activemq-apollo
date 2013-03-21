/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.apollo.stomp.test

import java.net.{Socket, InetSocketAddress}
import org.fusesource.hawtbuf.AsciiBuffer
import _root_.org.fusesource.hawtbuf.{ByteArrayOutputStream => BAOS}
import java.io._
import org.apache.activemq.apollo.broker.{KeyStorage, ProtocolException}
import javax.net.ssl.{SSLSocket, SSLContext}
import org.scalatest.matchers.ShouldMatchers
import org.apache.activemq.apollo.stomp.Stomp

/**
 * A simple Stomp client used for testing purposes
 */
class StompClient extends ShouldMatchers {

  var socket: Socket = new Socket
  var out: OutputStream = null
  var in: InputStream = null
  val bufferSize = 64 * 1204
  var key_storeage: KeyStorage = null
  var bytes_written = 0L
  var version:String = null

  def open(host: String, port: Int) = {
    bytes_written = 0
    socket = if (key_storeage != null) {
      val context = SSLContext.getInstance("TLS")
      context.init(key_storeage.create_key_managers, key_storeage.create_trust_managers, null)
      context.getSocketFactory().createSocket()
      // socket.asInstanceOf[SSLSocket].setEnabledCipherSuites(Array("SSL_RSA_WITH_RC4_128_MD5"))
      // socket
    } else {
      new Socket
    }
    socket.connect(new InetSocketAddress(host, port))
    socket.setSoLinger(true, 1)
    socket.setSoTimeout(30 * 1000)
    out = new BufferedOutputStream(socket.getOutputStream, bufferSize)
    in = new BufferedInputStream(socket.getInputStream, bufferSize)
  }

  def close() = {
    socket.close
  }

  def write(frame: String):Unit = write(frame.getBytes("UTF-8"))

  def write(frame: Array[Byte]):Unit = {
    out.write(frame)
    bytes_written += frame.length
    out.write(0)
    bytes_written += 1
    out.write('\n')
    bytes_written += 1
    out.flush
  }

  def skip(): Unit = {
    var c = in.read
    while (c >= 0) {
      if (c == 0) {
        return
      }
      c = in.read()
    }
    throw new EOFException()
  }

  def receive(timeout:Int): String = {
    val original = socket.getSoTimeout
    try {
      socket.setSoTimeout(timeout)
      receive()
    } finally {
      try {
        socket.setSoTimeout(original)
      } catch {
        case _:Throwable =>
      }
    }
  }

  def receive(): String = {
    var start = true;
    val buffer = new BAOS()
    var c = in.read
    while (c >= 0) {
      if (c == 0) {
        return new String(buffer.toByteArray, "UTF-8")
      }
      if (!start || c != Stomp.NEWLINE) {
        start = false
        buffer.write(c)
      }
      c = in.read()
    }
    throw new EOFException()
  }

  def wait_for_receipt(id: String): Unit = {
    val frame = receive()
    frame should startWith("RECEIPT\n")
    frame should include("receipt-id:" + id + "\n")
  }


  def receiveAscii(): AsciiBuffer = {
    val buffer = new BAOS()
    var c = in.read
    while (c >= 0) {
      if (c == 0) {
        return buffer.toBuffer.ascii
      }
      buffer.write(c)
      c = in.read()
    }
    throw new EOFException()
  }

  def receive(expect: String): String = {
    val rc = receive()
    if (!rc.startsWith(expect)) {
      throw new ProtocolException("Expected " + expect)
    }
    rc
  }

}