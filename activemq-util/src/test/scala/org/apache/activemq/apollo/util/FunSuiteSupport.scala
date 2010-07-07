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

import _root_.org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import java.io.File
import java.lang.String
import collection.immutable.Map
import org.scalatest._

/**
 * @version $Revision : 1.1 $
 */
@RunWith(classOf[JUnitRunner])
abstract class FunSuiteSupport extends FunSuite with Logging with BeforeAndAfterAll {
  protected var _basedir = "."


  /**
   * Returns the base directory of the current project
   */
  def baseDir = {
    new File(_basedir)
  }

  /**
   * Returns ${basedir}/target/test-data
   */
  def testDataDir = {
    new File(new File(_basedir, "target"), "test-data")
  }

  override protected def beforeAll(map: Map[String, Any]): Unit = {
    _basedir = map.get("basedir") match {
      case Some(basedir) => basedir.toString
      case _ => System.getProperty("basedir", ".")
    }
    debug("using basedir: " + _basedir)
    super.beforeAll(map)
  }

  //
  // Allows us to get the current test name.
  //

  val _testName = new ThreadLocal[String]();

  def testName = _testName.get

  protected override def runTest(testName: String, reporter: org.scalatest.Reporter, stopper: Stopper, configMap: Map[String, Any], tracker: Tracker) = {
    _testName.set(testName)
    try {
      super.runTest(testName, reporter, stopper, configMap, tracker)
    } finally {
      _testName.remove
    }
  }

}