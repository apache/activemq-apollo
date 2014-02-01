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
package org.apache.activemq.apollo.cli

import java.io.File
import org.apache.activemq.apollo.util.FileSupport._

/**
 * <p>
 * Launches the Apollo broker assuming it's being run from
 * an IDE environment.
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object ApolloIDERunner  {

  // We use this to figure out where the source code is in the files system.
  def project_base = new File(getClass.getResource("banner.txt").toURI.resolve("../../../../../../../..").toURL.getFile)

  def main(args:Array[String]):Unit = {

    // Let the user know where he configure logging at.
    println("Logging was configured using '%s'.".format(getClass.getClassLoader.getResource("log4j.properties")));


    // Setups where the broker base directory is...
    if( System.getProperty("apollo.base") == null ) {
      val apollo_base = project_base / "apollo-cli" / "target" / "test-classes" / "example-broker"
      System.setProperty("apollo.base", apollo_base.getCanonicalPath)
    }
    println("apollo.base=%s".format(System.getProperty("apollo.base")));
    System.setProperty("basedir", System.getProperty("apollo.base"))


    // Setup where the web app resources are...
    if( System.getProperty("apollo.webapp") == null ) {
      val apollo_webapp = project_base / "apollo-web"/ "src" / "main"/ "webapp"
      System.setProperty("apollo.webapp", apollo_webapp.getCanonicalPath)
    }

    System.setProperty("scalate.mode", "development")


    // Configure jul logging..
    val apollo_base = new File(System.getProperty("apollo.base"))
    val jul_properties = apollo_base / "etc" / "jul.properties"
    System.setProperty("java.util.logging.config.file", jul_properties.getCanonicalPath)

    println("=======================")
    println("Press Enter To Shutdown")
    println("=======================")

    new Apollo().run(System.in, System.out, System.err, Array("run"))
    System.in.read
    println("=============")
    println("Shutting down")
    println("=============")
    System.exit(0)
  }

}