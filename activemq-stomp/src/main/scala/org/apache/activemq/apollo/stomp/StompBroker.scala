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

import org.apache.activemq.transport.TransportFactory
import org.apache.activemq.apollo.broker.{LoggingTracker, Broker}
import java.io.File
import org.apache.activemq.apollo.dto.{CassandraStoreDTO, HawtDBStoreDTO}

/**
 */
object StompBroker {

  var address = "0.0.0.0"
  var port = 61613

  def main(args:Array[String]) = {
    val uri = "tcp://"+address+":"+port

    println("Starting stomp broker: "+uri)

    val broker = new Broker()
    val connector = broker.config.connectors.get(0)
    connector.bind = uri
    connector.protocol = "stomp"
    connector.advertise = uri

    val store = new CassandraStoreDTO
    store.hosts.add("localhost:9160")

//    val store = new HawtDBStoreDTO
//    store.directory = new File("activemq-data")
    
    broker.config.virtualHosts.get(0).store = store

    val tracker = new LoggingTracker("broker startup")
    tracker.start(broker)
    tracker.await
    println("Startup complete.")

    System.in.read
    println("Shutting down...")
    broker.stop
    println("Shutdown complete.")
  }

  
}