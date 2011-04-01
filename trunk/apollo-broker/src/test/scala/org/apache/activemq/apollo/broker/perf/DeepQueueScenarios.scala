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
  override def reportResourceTemplate(): URL = {classOf[PersistentScenario].getResource("persistent-report.html")}

  override def highContention = 100

  for (count <- partitionedLoad; messageSize <- messageSizes) {

    def benchmark(name: String)(func: => Unit) {
      test(name) {
        PTP = true
        MESSAGE_SIZE = messageSize
        destCount = 1;
        func
      }
    }

    val prefix = "queue " + (if ((messageSize % 1024) == 0) (messageSize / 1024) + "k" else messageSize + "b") + " "
    val suffix = "" //(if( durable ) " durable" else "")

    benchmark(format("%s%d%s", prefix, count, suffix)) {
      producerCount = count;
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