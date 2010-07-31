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
package org.apache.activemq.apollo.stomp

import org.apache.activemq.apollo.broker.Broker
import java.io.File
import org.apache.activemq.apollo.dto.{CassandraStoreDTO, HawtDBStoreDTO}
import org.apache.activemq.apollo.util._
import org.apache.activemq.apollo.store.bdb.dto.BDBStoreDTO

/**
 */
object StompBroker {

  var address = "0.0.0.0"
  var port = 61613
  var storeType = "hawtdb"
  var purge = true

  def main(args:Array[String]) = run

  def run = {
    println("=======================")
    println("Press ENTER to shutdown");
    println("=======================")
    println("")
    val uri = "tcp://"+address+":"+port
    println("Starting stomp broker: "+uri)
    println("log4j at: "+getClass.getClassLoader.getResource("log4j.properties"))

    val broker = new Broker()
//    val connector = broker.config.connectors.get(0)
//    connector.bind = uri
//    connector.protocol = "stomp"
//    connector.advertise = uri

    val store = storeType match {
      case "none" =>
        null

      case "hawtdb" =>
        val rc = new HawtDBStoreDTO
        rc.directory = new File("activemq-data")
        rc

      case "bdb" =>
        val rc = new BDBStoreDTO
        rc.directory = new File("activemq-data")
        rc

      case "cassandra" =>
        val rc = new CassandraStoreDTO
        rc.hosts.add("localhost:9160")
        rc
    }
    broker.config.virtual_hosts.get(0).store = store
    broker.config.virtual_hosts.get(0).purge_on_startup = purge



    var tracker = new LoggingTracker("broker startup")
    tracker.start(broker)
    tracker.await
    println("Startup complete.")

    System.in.read

    println("Shutting down...")
    tracker = new LoggingTracker("broker shutdown")
    tracker.stop(broker)
    tracker.await
    
    println("=======================")
    println("Shutdown");
    println("=======================")

  }

  override def toString() = {
    "--------------------------------------\n"+
    "StompBroker Properties\n"+
    "--------------------------------------\n"+
    "address          = "+address+"\n"+
    "port             = "+port+"\n"+
    "storeType        = "+storeType+"\n" +
    "purge            = "+purge+"\n"+
    ""
  }  
}