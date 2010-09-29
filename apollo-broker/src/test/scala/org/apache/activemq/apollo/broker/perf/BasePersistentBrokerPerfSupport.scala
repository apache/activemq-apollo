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
abstract class BasePersistentBrokerPerfSupport extends BaseBrokerPerfSupport {

  PERSISTENT = true

  override def reportResourceTemplate():URL = { classOf[BasePersistentBrokerPerfSupport].getResource("persistent-report.html") }

  override def highContention = 100

  for ( load <- partitionedLoad ; messageSize <- messageSizes ) {
    val numMessages = 1000000 / load

    val info = "queue " + numMessages + " " + (if((messageSize%1024)==0) (messageSize/1024)+"k" else messageSize+"b" ) + " with " + load + " "    

    test("En" + info + "producer(s)") {
      MAX_MESSAGES = numMessages
      PTP = true
      PURGE_STORE = true      
      MESSAGE_SIZE = messageSize
      producerCount = load;
      destCount = 1;
      createConnections();

      // Start 'em up.
      startClients();
      try {
        reportRates();
      } finally {
        stopServices();
      }
    }

    test("De" + info + "consumer(s)") {
      MAX_MESSAGES = numMessages
      PTP = true
      PURGE_STORE = false
      MESSAGE_SIZE = messageSize
      consumerCount = load;
      destCount = 1;
      createConnections();

      // Start 'em up.
      startClients();
      try {
        reportRates();
      } finally {
        stopServices();
      }
    }
  }

}