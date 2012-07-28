package org.apache.activemq.apollo.stomp

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

import java.io.File
import org.openqa.selenium.{By, WebDriver}
import org.openqa.selenium.chrome.{ChromeOptions, ChromeDriver}
import org.openqa.selenium.remote.DesiredCapabilities
import java.util.Arrays
import org.apache.activemq.apollo.util.FileSupport._
import org.openqa.selenium.firefox.{FirefoxDriver, FirefoxProfile}
import java.util.concurrent.TimeUnit._
import org.apache.activemq.apollo.stomp.test._

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

trait WebDriverTrait {
  def create_web_driver(profileDir: File): WebDriver
}

trait ChromeWebDriverTrait extends WebDriverTrait {
  def create_web_driver(profileDir: File) = {
    //    val f = new File("/Applications/Google Chrome.app/Contents/MacOS/Google Chrome")
    //    if( f.exists() ) {
    //      System.setProperty("webdriver.chrome.driver", f.getCanonicalPath)
    //    }

    val file = profileDir / "chrome"
    file.mkdirs
    val options = new ChromeOptions()
    options.addArguments("--enable-udd-profiles", "--user-data-dir=" + file, "--allow-file-access-from-files")
    //    val capabilities = new DesiredCapabilities
    //    capabilities.setCapability("chrome.switches", Arrays.asList("--enable-udd-profiles", "--user-data-dir="+file, "--allow-file-access-from-files"));
    new ChromeDriver(options)
  }
}

trait FirefoxWebDriverTrait extends WebDriverTrait {
  def create_web_driver(profileDir: File) = {
    var file = profileDir / "firefox"
    file.mkdirs
    val profile = new FirefoxProfile(file);
    new FirefoxDriver(profile)
  }
}


abstract class StompWebSocketTestSupport extends StompTestSupport with WebDriverTrait {

  var driver: WebDriver = _

  override def broker_config_uri = "xml:classpath:apollo-stomp-websocket.xml"

  def ws_port: Int = connector_port("ws").get

  override def beforeAll() = {
    try {
      driver = create_web_driver(test_data_dir / "profile")
      super.beforeAll()
    } catch {
      case ignore: Exception =>
        println("ignoring tests, could not create web driver: " + ignore)
    }
  }

  override def afterAll() = {
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

  test("websocket") {
    val url = getClass.getResource("websocket.html")

    System.out.println("url: " + url)
    driver.get(url + "#ws://127.0.0.1:" + ws_port);
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

class ChromeStompWebSocketTest extends StompWebSocketTestSupport with ChromeWebDriverTrait

class FirefoxStompWebSocketTest extends StompWebSocketTestSupport with FirefoxWebDriverTrait