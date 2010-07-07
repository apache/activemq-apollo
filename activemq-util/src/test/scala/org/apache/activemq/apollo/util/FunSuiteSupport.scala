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