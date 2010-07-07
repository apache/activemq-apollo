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

package org.apache.activemq.apollo.store.cassandra

import org.apache.cassandra.thrift.CassandraDaemon
import org.apache.cassandra.config.DatabaseDescriptor
import java.io.File
import java.net.ConnectException
import org.apache.thrift.transport.{TTransportException, TSocket}
import com.shorrockin.cascal.session._
import com.shorrockin.cascal.utils.{Utils, Logging}
import org.scalatest.{Suite, BeforeAndAfterAll}

/**
 * trait which mixes in the functionality necessary to embed
 * cassandra into a unit test
 */
trait CassandraServerMixin extends BeforeAndAfterAll {
this: Suite =>

  private var basedir = "target"

  override protected def beforeAll(configMap: Map[String, Any]): Unit = {
    configMap.get("basedir") match {
      case Some(x) => basedir = x.toString
      case _ =>
    }
    startCassandra
    super.beforeAll(configMap)
  }

  override protected def afterAll(configMap: Map[String, Any]) = {
    super.afterAll(configMap)
    stopCassandra
  }


  import Utils._

  protected def cassandraHomeDirectory = new File(new File(basedir),"cassandra.home.unit-tests")
  protected def resource(str:String) = getClass.getResourceAsStream(str)

  private var daemon:CassandraDaemon = null
  var pool:SessionPool = null
  var daemonThread:Thread = null

  private def startCassandra = synchronized {
    val hosts  = Host("localhost", 9160, 250) :: Nil
    val params = new PoolParams(10, ExhaustionPolicy.Fail, 500L, 6, 2)
    pool = new SessionPool(hosts, params, Consistency.One)

    val home = cassandraHomeDirectory
    delete(home)
    home.mkdirs

    val fileSep     = System.getProperty("file.separator")
    val storageFile = new File(cassandraHomeDirectory, "storage-conf.xml")
    val logFile     = new File(cassandraHomeDirectory, "log4j.properties")

    replace(copy(resource("storage-conf.xml"), storageFile), ("%temp-dir%" -> (cassandraHomeDirectory.getCanonicalPath + fileSep)))
    copy(resource("log4j.properties"), logFile)

    System.setProperty("storage-config", cassandraHomeDirectory.getCanonicalPath)

    try {
      DatabaseDescriptor.getAllDataFileLocations.foreach {
        (file) => new File(file).mkdirs
      }
      new File(DatabaseDescriptor.getLogFileLocation).mkdirs

      daemon = new CassandraDaemon
      daemon.init(new Array[String](0))

      daemonThread = new Thread("Cassandra Daemon") {
        override def run = {
          daemon.start
        }
      }
      daemonThread.start
      
    } catch {
      case e:Throwable =>
        e.printStackTrace
        throw e
    }

    // try to make sockets until the server opens up - there has to be a better
    // way - just not sure what it is.
    val socket = new TSocket("localhost", 9160);
    var opened = false
    while (!opened) {
      try {
        socket.open()
        opened = true
        socket.close()
      } catch {
        case e:TTransportException => /* ignore */
        case e:ConnectException => /* ignore */
      }
    }
  }

  private def stopCassandra() = {
    pool.close
    daemon.stop
    daemonThread.join
    daemonThread = null

    daemon.destroy
    daemon = null
  }

}

