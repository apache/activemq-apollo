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
package org.apache.activemq.apollo.util

import java.util.concurrent.TimeUnit

/**
 * <p>Produces a LongMetric which track Long events</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class LongMetricCounter extends MetricProducer[LongMetric] {

  private var max = Long.MinValue
  private var min = Long.MaxValue
  private var total = 0L
  private var count = 0

  def apply(reset: Boolean):LongMetric = {
    val rc = LongMetric(count, total, min, max)
    if (reset) {
      clear()
    }
    rc
  }

  def clear() = {
    max = Long.MinValue
    min = Long.MaxValue
    total = 0L
    count = 0
  }

  /**
   * Adds a duration to our current Timing.
   */
  def +=(value: Long): Unit = {
    if (value > -1) {
      max = value max max
      min = value min min
      total += value
      count += 1
    }
  }

}

case class LongMetric(count:Long, total:Long, min:Long, max:Long) {
  def avg = if( count==0 ) 0f else total.toFloat / count
  def frequency = 1.toFloat / avg
}
