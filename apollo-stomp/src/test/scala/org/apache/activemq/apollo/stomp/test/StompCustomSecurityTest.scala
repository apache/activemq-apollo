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

import java.lang.String
import java.nio.channels.DatagramChannel
import java.net.InetSocketAddress
import org.fusesource.hawtbuf.AsciiBuffer

class StompCustomSecurityTest extends StompTestSupport {

  override def broker_config_uri: String = "xml:classpath:apollo-stomp-custom-security.xml"

  override def is_parallel_test_class: Boolean = false

  override def beforeAll = {
    try {
      val login_file = new java.io.File(getClass.getClassLoader.getResource("login.config").getFile())
      System.setProperty("java.security.auth.login.config", login_file.getCanonicalPath)
    } catch {
      case x: Throwable => x.printStackTrace
    }
    super.beforeAll
  }

  test("User authorized to use own queues") {
    connect("1.1", client,
      "login:guest\n" +
      "passcode:guest\n")

    client.write(
      "SEND\n" +
       "destination:/queue/user.guest\n" +
       "receipt:0\n" +
       "\n" +
       "Hello World\n")

    wait_for_receipt("0")

  }


  test("User not authorized to use other queues") {
    connect("1.1", client,
      "login:guest\n" +
      "passcode:guest\n")

    client.write(
      "SEND\n" +
      "destination:/queue/user.hiram\n" +
      "receipt:0\n" +
      "\n" +
      "Hello World\n")

    val frame = client.receive()
    frame should startWith("ERROR\n")
    frame should include("message:Not authorized to create the queue")

  }

}
