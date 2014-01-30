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

import org.junit.runner.RunWith
import java.io.File
import java.lang.String
import collection.immutable.Map
import org.scalatest._
import java.util.concurrent.TimeUnit
import FileSupport._
import scala.Some
import org.apache.activemq.apollo.util.FunSuiteSupport._
import java.util.concurrent.locks.{ReentrantReadWriteLock, Lock, ReadWriteLock}
import java.util.concurrent.atomic.AtomicLong
import java.lang.management.ManagementFactory
import javax.management.ObjectName
import javax.management.openmbean.CompositeData

object FunSuiteSupport {
  class SkipTestException extends RuntimeException
  val parallel_test_class_lock = new ReentrantReadWriteLock()
  val id_counter = new AtomicLong()
}

/**
 * @version $Revision : 1.1 $
 */
@RunWith(classOf[org.scalatest.junit.ParallelJUnitRunner])
abstract class FunSuiteSupport extends FunSuite with Logging with ParallelBeforeAndAfterAll {

  def next_id(prefix:String="", suffix:String="") = prefix+id_counter.incrementAndGet()+suffix

  protected var _basedir = try {
    var file = new File(getClass.getProtectionDomain.getCodeSource.getLocation.getFile)
    file = (file / ".." / "..").getCanonicalFile
    if( file.isDirectory ) {
      file.getPath
    } else {
      "."
    }
  } catch {
    case x:Throwable =>
      "."
  }

  def skip(check:Boolean=true):Unit = if(check) throw new SkipTestException()

  def getJVMHeapUsage = {
    val mbean_server = ManagementFactory.getPlatformMBeanServer()
    val data = mbean_server.getAttribute(new ObjectName("java.lang:type=Memory"), "HeapMemoryUsage").asInstanceOf[CompositeData]
    data.get("used").asInstanceOf[java.lang.Long].longValue()
  }

  var _log:Log = null
  override protected def log: Log = {
    if( _log == null ) {
      super.log
    } else {
      _log
    }
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Unit) {
    super.test(testName, testTags:_*) {
      try {
        _log = Log(getClass.getName.stripSuffix("$")+":"+testName)
        testFun
      } catch {
        case e:SkipTestException =>
        case e:Throwable =>
          onTestFailure(e)
          throw e
      } finally {
        _log = null
      }
    }
  }

  def onTestFailure(e:Throwable) = {}

  /**
   * Returns the base directory of the current project
   */
  def basedir = new File(_basedir).getCanonicalFile

  /**
   * Returns ${basedir}/target/test-data
   */
  def test_data_dir = basedir / "target"/ "test-data" / (getClass.getName)

  /**
   * Can this test class run in parallel with other
   * test classes.
   * @return
   */
  def is_parallel_test_class = true

  override protected def beforeAll(map: Map[String, Any]): Unit = {
    val original_name = Thread.currentThread().getName
    try {
      Thread.currentThread().setName(getClass.getName)
      if ( is_parallel_test_class ) {
        parallel_test_class_lock.readLock().lock()
      } else {
        parallel_test_class_lock.writeLock().lock()
      }

      _basedir = map.get("basedir") match {
        case Some(basedir) =>
          basedir.toString
        case _ =>
          System.getProperty("basedir", _basedir)
      }
      System.setProperty("basedir", _basedir)
      test_data_dir.recursive_delete
      super.beforeAll(map)
    } finally {
      Thread.currentThread().setName(original_name)
    }
  }

  override protected def afterAll(configMap: Map[String, Any]) {
    try {
      super.afterAll(configMap)
    } finally {
      if ( is_parallel_test_class ) {
        parallel_test_class_lock.readLock().unlock()
      } else {
        parallel_test_class_lock.writeLock().unlock()
      }
    }
  }


  //
  // Allows us to get the current test name.
  //

  /**
   * Defines a method (that takes a <code>configMap</code>) to be run after
   * all of this suite's tests and nested suites have been run.
   *
   * <p>
   * This trait's implementation
   * of <code>run</code> invokes this method after executing all tests
   * and nested suites (passing in the <code>configMap</code> passed to it), thus this
   * method can be used to tear down a test fixture
   * needed by the entire suite. This trait's implementation of this method invokes the
   * overloaded form of <code>afterAll</code> that takes no <code>configMap</code>.
   * </p>
   */

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

  private class ShortCircuitFailure(msg:String) extends RuntimeException(msg)

  def exit_within_with_failure[T](msg:String):T = throw new ShortCircuitFailure(msg)

  def within[T](timeout:Long, unit:TimeUnit)(func: => Unit ):Unit = {
    val start = System.currentTimeMillis
    var amount = unit.toMillis(timeout)
    var sleep_amount = amount / 100
    var last:Throwable = null

    if( sleep_amount < 1 ) {
      sleep_amount = 1
    }
    try {
      func
      return
    } catch {
      case e:ShortCircuitFailure => throw e
      case e:Throwable => last = e
    }

    while( (System.currentTimeMillis-start) < amount ) {
      Thread.sleep(sleep_amount)
      try {
        func
        return
      } catch {
        case e:ShortCircuitFailure => throw e
        case e:Throwable => last = e
      }
    }

    throw last
  }
}