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

import org.openqa.selenium.By
import java.util.concurrent.TimeUnit._
/**
 *
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
class LargeMsgSecureWebSocketTest extends WebSocketSupport with FirefoxWebDriverTrait {

  override def broker_config_uri = "xml:classpath:apollo-stomp-websocket-large.xml"

  // client sends broker large message..
  test("websockets large text messages from client: wss") {
    val protocol = "wss"
    val url = "http://localhost:" + jetty_port + "/websocket-large.html"
    val ws_port: Int = connector_port(protocol).get

    println("url: " + url)
    println("ws url: " + protocol + "://127.0.0.1:" + ws_port)

    driver.get(url + "#" + protocol + "://127.0.0.1:" + ws_port);

    val web_status = driver.findElement(By.id("status"));

    while ("Message not sent" == web_status.getText) {
      Thread.sleep(100)
    }

    if (web_status != "No WebSockets") {
      connect("1.1")
      subscribe("0", "/queue/websocket")

      within(2, SECONDS) {
        assert_received( """x.*""".r, "0")
      }
    }

  }
}
