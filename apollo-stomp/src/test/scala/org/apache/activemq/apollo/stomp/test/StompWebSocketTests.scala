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

import org.openqa.selenium.{By, WebDriver}
import org.apache.activemq.apollo.util.FileSupport._
import java.util.concurrent.TimeUnit._
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.util.thread.ExecutorThreadPool
import org.apache.activemq.apollo.broker.Broker
import org.eclipse.jetty.server.nio.SelectChannelConnector
import org.eclipse.jetty.webapp.WebAppContext
import java.io.File

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object StompWebSocketTests {

  class ChromeStompWebSocketTest extends StompWebSocketTestBase with ChromeWebDriverTrait
  class FirefoxStompWebSocketTest extends StompWebSocketTestBase with FirefoxWebDriverTrait
  class SafariStompWebSocketTest extends StompWebSocketTestBase with SafariWebDriverTrait

  abstract class StompWebSocketTestBase extends StompTestSupport with WebDriverTrait {

    var driver: WebDriver = _

    override def broker_config_uri = "xml:classpath:apollo-stomp-websocket.xml"

    var jetty:Server = _
    var jetty_port = 0

    def start_jetty = {
      jetty = new Server
      val connector = new SelectChannelConnector
      connector.setPort(0)

      test_data_dir.mkdirs()

      var file = new File(getClass.getResource("websocket.html").getPath)
      file.copy_to(test_data_dir/"websocket.html")
      new File(file.getParentFile, "../../../../../../../../../apollo-website/src/scripts/jquery.js").getCanonicalFile.copy_to(test_data_dir/"jquery.js")
      new File(file.getParentFile, "../../../../../../../../../apollo-distro/src/main/release/examples/websocket/js/stomp.js").getCanonicalFile.copy_to(test_data_dir/"stomp.js")

      var context = new WebAppContext
      context.setContextPath("/")
      context.setWar(test_data_dir.getCanonicalPath)
      context.setClassLoader(Broker.class_loader)

      jetty.setHandler(context)
      jetty.setConnectors(Array(connector))
      jetty.setThreadPool(new ExecutorThreadPool(Broker.BLOCKABLE_THREAD_POOL))
      jetty.start

      jetty_port = connector.getLocalPort
    }

    def stop_jetty = {
      if( jetty!=null ) {
        jetty.stop()
        jetty = null
      }
    }

    override def beforeAll() = {
      try {
        driver = create_web_driver(test_data_dir / "profile")
        start_jetty
        super.beforeAll()
      } catch {
        case ignore: Throwable =>
          println("ignoring tests, could not create web driver: " + ignore)
      }
    }

    override def afterAll() = {
      stop_jetty
      if (driver != null) {
        driver.quit()
        driver = null
      }
    }

    override protected def test(testName: String, testTags: org.scalatest.Tag*)(testFun: => scala.Unit): Unit = {
      super.test(testName, testTags: _*) {
        if (driver != null) {
          testFun
        }
      }
    }

    for( protocol <- Array("ws", "wss") ) {
      test("websocket "+protocol) {

        val url = "http://localhost:"+jetty_port+"/websocket.html"
        val ws_port: Int = connector_port(protocol).get

        System.out.println("url: " + url)
        driver.get(url + "#"+protocol+"://127.0.0.1:" + ws_port);
        val web_status = driver.findElement(By.id("status"));
        val web_received = driver.findElement(By.id("received"));

        while ("Loading" == web_status.getText) {
          Thread.sleep(100)
        }

        // Skip test if browser does not support websockets..
        if (web_status.getText != "No WebSockets") {

          // Wait for it to get connected..
          within(2, SECONDS) {
            web_status.getText should be("Connected")
          }

          // Send a message via normal TCP stomp..
          connect("1.1")
          async_send("/queue/websocket", "Hello")
          within(2, SECONDS) {
            // it should get received by the websocket client.
            web_received.getText should be("Hello")
          }

          // Send a bunch of messages..
          val send_count = 100000
          for (i <- 1 to send_count) {
            async_send("/queue/websocket", "messages #" + i)
          }

          within(10, SECONDS) {
            // it should get received by the websocket client.
            web_received.getText should be("messages #" + send_count)
          }

        }

      }
    }
    }

}



