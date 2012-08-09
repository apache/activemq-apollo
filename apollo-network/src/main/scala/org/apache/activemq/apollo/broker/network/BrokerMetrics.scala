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
package org.apache.activemq.apollo.broker.network

import dto.LoadStatusDTO
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics
import collection.mutable.HashMap

class CounterDrivenRate {
  var last_value = Long.MaxValue
  val stats = new DescriptiveStatistics()
  stats.setWindowSize(10)
  var mean = 0.0d

  def update(value:Long, duration:Double) = {
    // is it a counter reset??
    if( value < last_value ) {
      last_value = value
    } else {
      val increment = value - last_value
      stats.addValue(increment / duration)
      mean = stats.getMean
    }
  }
}

class DestinationMetrics {
  var message_size = 0L
  val enqueue_size_rate = new CounterDrivenRate()
  var consumer_count = 0L
  var dequeue_size_rate = 0d
}

class BrokerMetrics() {

  import collection.JavaConversions._
  var queue_load = HashMap[String, DestinationMetrics]()
//  var topic_load = HashMap[String, DestinationMetrics]()
  var timestamp = System.currentTimeMillis()

  def update(current:LoadStatusDTO, network_user:String) = {
    val now = System.currentTimeMillis()
    val duration = (now - timestamp)/1000.0d
    timestamp = now

    var next_queue_load = HashMap[String, DestinationMetrics]()
    for( dest <- current.queues ) {
      val dest_load = queue_load.get(dest.id).getOrElse(new DestinationMetrics())
      dest_load.message_size = dest.message_size

      // Lets not include the network consumers in the the consumer rates..
      val consumers = dest.consumers.filter(_.user != network_user).toArray

      dest_load.consumer_count = consumers.size
      dest_load.dequeue_size_rate = 0
      for( c <- consumers ) {
        if( c.ack_size_rate !=null ) {
          dest_load.dequeue_size_rate +=  c.ack_size_rate
        }
      }
      dest_load.enqueue_size_rate.update(dest.message_size_enqueue_counter, duration)
      next_queue_load += dest.id -> dest_load
    }
    queue_load = next_queue_load

//    var next_topic_load = HashMap[String, DestinationMetrics]()
//    for( dest <- current.topics ) {
//      val dest_load = topic_load.get(dest.id).getOrElse(new DestinationMetrics())
//      dest_load.message_size = dest.message_size
//      dest_load.enqueue_size_rate.update(dest.message_size_enqueue_counter, duration)
//      dest_load.dequeue_size_rate.update(dest.message_size_dequeue_counter, duration)
//      next_topic_load += dest.id -> dest_load
//    }
//    topic_load = next_topic_load
  }
}
