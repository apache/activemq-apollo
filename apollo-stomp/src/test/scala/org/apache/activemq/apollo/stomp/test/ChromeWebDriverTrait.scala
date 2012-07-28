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

import java.io.File
import org.openqa.selenium.chrome.{ChromeOptions, ChromeDriver}
import org.apache.activemq.apollo.util.FileSupport._

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