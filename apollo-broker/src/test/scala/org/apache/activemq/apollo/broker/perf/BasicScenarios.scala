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

import _root_.org.apache.activemq.apollo.util.metric.{Period}
import java.net.URL

/**
 * 
 */
trait BasicScenarios extends BrokerPerfSupport {

  def reportResourceTemplate():URL = { classOf[BasicScenarios].getResource("report.html") }

  // benchmark all the combinations
  for( ptp <- List(true,false) ; durable <- List(false) ; messageSize <- messageSizes ) {

    def benchmark(name:String)(func: =>Unit) {
      test(name) {
        this.PTP = ptp
        this.DURABLE = durable
        this.MESSAGE_SIZE = messageSize
        func
      }
    }

    val prefix = (if( ptp ) "queue " else "topic ") + (if((messageSize%1024)==0) (messageSize/1024)+"k" else messageSize+"b" ) + " "
    val suffix = (if( durable ) " durable" else "")

    if( ptp && durable ) {
      // skip this combination since queues and durable subs don't mix.
    } else {

      /**
       * Used to benchmark what is the raw speed of sending messages one way.
       * Divide by 2 and compare against 1-1-1 to figure out what the broker dispatching
       * overhead is.
       */
      if (!ptp) {
        benchmark(prefix+"1->1->0"+suffix) {
          producerCount = 1;
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

      /**
       * benchmark( increasing partitioned load.
       */
      for( count <- partitionedLoad ) {
        benchmark(format("%s%d->%d->%d%s", prefix, count, count, count, suffix)) {
          println(testName)
          producerCount = count;
          destCount = count;
          consumerCount = count;

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

      /**
       * benchmark( the effects of high producer and consumer contention on a single
       * destination.
       */
      for( (producers, consumers) <- List((highContention, 1), (1, highContention), (highContention, highContention)) ) {
        benchmark(format("%s%d->1->%d%s", prefix, producers, consumers, suffix)) {
          producerCount = producers;
          consumerCount = consumers;
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

//    /**
//     *  benchmark(s 1 producers sending to 1 destination with 1 slow and 1 fast consumer.
      //     *
//     * queue case: the producer should not slow down since it can dispatch to the
//     *             fast consumer
//     *
//     * topic case: the producer should slow down since it HAS to dispatch to the
//     *             slow consumer.
//     *
//     */
//    benchmark("1->1->[1 slow,1 fast]") {
//      producerCount = 2;
//      destCount = 1;
//      consumerCount = 2;
//
//      createConnections();
//      consumers.get(0).thinkTime = 50;
//
//      // Start 'em up.
//      startClients();
//      try {
//        reportRates();
//      } finally {
//        stopServices();
//      }
//    }
//
//    benchmark("2->2->[1,1 selecting]") {
//      producerCount = 2;
//      destCount = 2;
//      consumerCount = 2;
//
//      createConnections();
//
//      // Add properties to match producers to their consumers
//      for (i <- 0 until consumerCount) {
//        var property = "match" + i;
//        consumers.get(i).selector = property;
//        producers.get(i).property = property;
//      }
//
//      // Start 'em up.
//      startClients();
//      try {
//        reportRates();
//      } finally {
//        stopServices();
//      }
//    }

//    /**
//     * benchmark( sending with 1 high priority sender. The high priority sender should
//     * have higher throughput than the other low priority senders.
//     *
//     * @throws Exception
//     */
//    benchmark("[1 high, 1 normal]->1->1") {
//      producerCount = 2;
//      destCount = 1;
//      consumerCount = 1;
//
//      createConnections();
//      var producer = producers.get(0);
//      producer.priority = 1
//      producer.rate.setName("High Priority Producer Rate");
//
//      consumers.get(0).thinkTime = 1;
//
//      // Start 'em up.
//      startClients();
//      try {
//        println("Checking rates...");
//        for (i <- 0 until PERFORMANCE_SAMPLES) {
//          var p = new Period();
//          Thread.sleep(SAMPLE_PERIOD);
//          println(producer.rate.getRateSummary(p));
//          println(totalProducerRate.getRateSummary(p));
//          println(totalConsumerRate.getRateSummary(p));
//          totalProducerRate.reset();
//          totalConsumerRate.reset();
//        }
//
//      } finally {
//        stopServices();
//      }
//    }

//    /**
//     * benchmark( sending with 1 high priority sender. The high priority sender should
//     * have higher throughput than the other low priority senders.
//     *
//     * @throws Exception
//     */
//    benchmark("[1 high, 1 mixed, 1 normal]->1->1") {
//      producerCount = 2;
//      destCount = 1;
//      consumerCount = 1;
//
//      createConnections();
//      var producer = producers.get(0);
//      producer.priority = 1;
//      producer.priorityMod = 3;
//      producer.rate.setName("High Priority Producer Rate");
//
//      consumers.get(0).thinkTime = 1
//
//      // Start 'em up.
//      startClients();
//      try {
//
//        println("Checking rates...");
//        for (i <- 0 until PERFORMANCE_SAMPLES) {
//          var p = new Period();
//          Thread.sleep(SAMPLE_PERIOD);
//          println(producer.rate.getRateSummary(p));
//          println(totalProducerRate.getRateSummary(p));
//          println(totalConsumerRate.getRateSummary(p));
//          totalProducerRate.reset();
//          totalConsumerRate.reset();
//        }
//
//      } finally {
//        stopServices();
//      }
//    }

    }

  }

}
