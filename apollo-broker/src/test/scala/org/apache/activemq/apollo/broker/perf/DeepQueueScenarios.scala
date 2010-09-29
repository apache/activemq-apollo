/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
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

import java.net.URL

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait DeepQueueScenarios extends PersistentScenario {

  PERSISTENT = true

  override def reportResourceTemplate():URL = { classOf[DeepQueueScenarios].getResource("persistent-report.html") }

  //override def partitionedLoad = List(1, 2, 4, 8, 10)
  override def highContention = 100
  //override def messageSizes = List(20, 1024, 1024*256)

  for ( load <- partitionedLoad ; messageSize <- List(20,1024)  ) {

    val totalMessages = 100000
    val numMessages = totalMessages / load

    def benchmark(name: String)(func: => Unit) {
      test(name) {
        MAX_MESSAGES = numMessages
        PTP = true
        MESSAGE_SIZE = messageSize
        destCount = 1;
        func
      }
    }

    val info = "queue " + numMessages + " " + (if((messageSize%1024)==0) (messageSize/1024)+"k" else messageSize+"b" ) + " with " + load + " "

    benchmark("En" + info + "producer(s)") {
      PURGE_STORE = true
      producerCount = load;
      createConnections();

      // Start 'em up.
      startClients();
      try {
        reportRates();
      } finally {
        stopServices();
      }
      this.assert(messagesSent == totalMessages, "Unexpected number of messages sent!")
    }

    benchmark("De" + info + "consumer(s)") {
      PURGE_STORE = false
      consumerCount = load;
      createConnections();

      // Start 'em up.
      startClients();
      try {
        reportRates();
      } finally {
        stopServices();
      }
      this.assert(messagesReceived == totalMessages, "Unexpected number of messages received!")
    }
  }

}