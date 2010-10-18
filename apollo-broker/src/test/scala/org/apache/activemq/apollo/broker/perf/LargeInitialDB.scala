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
 
package org.apache.activemq.apollo.broker.perf

import org.apache.activemq.apollo.broker.{Destination, Broker}
import tools.nsc.io.Directory
import org.apache.activemq.apollo.util.metric.MetricAggregator
import org.apache.activemq.apollo.util.{FileSupport, LoggingTracker}


trait LargeInitialDB extends PersistentScenario {

  PURGE_STORE = false

  var original: Directory = null
  var backup: Directory = null;

  // delete existing data file and copy new data file over
  override protected def beforeEach() = {
    println("Restoring DB")
    restoreDB
    super.beforeEach
  }

  // start a broker connect a producer and dump a bunch of messages
  // into a destination
  override protected def beforeAll(configMap: Map[String, Any]) = {
    super.beforeAll(configMap)

    sendBroker = new Broker()
    sendBroker.config = createBrokerConfig("Broker", sendBrokerBindURI, sendBrokerConnectURI)
    val store = sendBroker.config.virtual_hosts.get(0).store

    original = new Directory(storeDirectory)
    if ( original.exists ) {
      original.deleteRecursively
      original.createDirectory(true)      
    }
    val backupLocation = FileSupport.toDirectory(storeDirectory.getParent)./(FileSupport.toDirectory("backup"))
    backup = backupLocation
    cleanBackup

    println("Using store at " + original + " and backup at " + backup)

    var tracker = new LoggingTracker("initial db broker startup")
    tracker.start(sendBroker)
    tracker.await

    PTP = true
    val dests: Array[Destination] = createDestinations(1)
    totalProducerRate = new MetricAggregator().name("Aggregate Producer Rate").unit("items")    
    val producer: RemoteProducer = _createProducer(0, 1024, dests(0))
    producer.persistent = true

    tracker = new LoggingTracker("initial db producer startup")
    tracker.start(producer)
    tracker.await

    val messages = 1000000L

    println("Filling broker with " + messages + " 1k messages")
    while (producer.rate.counter() < messages) {
      println("Waiting for producer " + producer.rate.counter() + "/" + messages)
      Thread.sleep(5000)
    }

    tracker = new LoggingTracker("producer shutdown")
    tracker.stop(producer)
    tracker.await
    tracker = new LoggingTracker("broker shutdown")
    tracker.stop(sendBroker)
    tracker.await

    Thread.sleep(10000)

    saveDB
  }

  def saveDB {
    println("Copying contents of " + original + " to " + backup)
    cleanBackup
    FileSupport.recursiveCopy(original, backup)
    printStores
  }

  def printStores {
    println("\nOriginal store")
    original.deepList().foreach(println)
    println("\n\nBackup store")
    backup.deepList().foreach(println)
  }

  def restoreDB {
    original.deleteRecursively
    println("Copying contents of " + backup + " to " + original)
    FileSupport.recursiveCopy(backup, original)
    printStores
  }

  def cleanBackup {
    if (backup.exists) {
      backup.deleteRecursively
    }
    backup.createDirectory(true)
    printStores
  }

}