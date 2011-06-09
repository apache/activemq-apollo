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

package org.apache.activemq.apollo.broker.store.cassandra

import org.apache.cassandra.thrift.CassandraDaemon
import org.apache.cassandra.config.DatabaseDescriptor
import java.io.File
import java.net.ConnectException
import org.apache.thrift.transport.{TTransportException, TSocket}
import com.shorrockin.cascal.session._
import com.shorrockin.cascal.utils.{Utils, Logging}
import org.scalatest.{Suite, BeforeAndAfterAll}
import org.apache.activemq.apollo.util.{FileSupport, FunSuiteSupport}
import FileSupport._

/**
 * trait which mixes in the functionality necessary to embed
 * cassandra into a unit test
 */
trait CassandraServerMixin extends FunSuiteSupport with BeforeAndAfterAll {
this: Suite =>

  override protected def beforeAll(configMap: Map[String, Any]): Unit = {
    start_cassandra
    super.beforeAll(configMap)
  }

  override protected def afterAll(configMap: Map[String, Any]) = {
    super.afterAll(configMap)
    stop_cassandra
  }


  import Utils._

  protected def cassandra_home = test_data_dir / "cassandra-data"
  protected def resource(str:String) = getClass.getResourceAsStream(str)

  private var daemon:CassandraDaemon = null
  var pool:SessionPool = null
  var daemon_thread:Thread = null

  private def start_cassandra = synchronized {
    val hosts  = Host("localhost", 9160, 250) :: Nil
    val params = new PoolParams(10, ExhaustionPolicy.Fail, 500L, 6, 2)
    pool = new SessionPool(hosts, params, Consistency.One)

    val home = cassandra_home
    delete(home)
    home.mkdirs

    val file_sep     = System.getProperty("file.separator")
    val storage_file = new File(cassandra_home, "storage-conf.xml")
    val log_file     = new File(cassandra_home, "log4j.properties")

    replace(copy(resource("storage-conf.xml"), storage_file), ("%temp-dir%" -> (cassandra_home.getCanonicalPath + file_sep)))
    copy(resource("log4j.properties"), log_file)

    System.setProperty("storage-config", cassandra_home.getCanonicalPath)

    try {
      DatabaseDescriptor.getAllDataFileLocations.foreach {
        (file) => new File(file).mkdirs
      }
      new File(DatabaseDescriptor.getLogFileLocation).mkdirs

      daemon = new CassandraDaemon
      daemon.init(new Array[String](0))

      daemon_thread = new Thread("Cassandra Daemon") {
        override def run = {
          daemon.start
        }
      }
      daemon_thread.start
      
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

  private def stop_cassandra() = {
    pool.close
    daemon.stop
    daemon_thread.join
    daemon_thread = null

    daemon.destroy
    daemon = null
  }

}

