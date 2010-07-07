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
 * <p>A Timer collects time durations and produces a TimeMetric.</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class TimeCounter extends MetricProducer[TimeMetric] {

  private var maximum = Long.MinValue
  private var minimum = Long.MaxValue
  private var total = 0L
  private var count = 0

  def apply(reset: Boolean):TimeMetric = {
    val rc = TimeMetric(count, total, minimum, maximum)
    if (reset) {
      clear()
    }
    rc
  }

  def clear() = {
    maximum = Long.MinValue
    minimum = Long.MaxValue
    total = 0L
    count = 0
  }

  /**
   * Adds a duration to our current Timing.
   */
  def +=(value: Long): Unit = {
    if (value > -1) {
      maximum = value max maximum
      minimum = value min minimum
      total += value
      count += 1
    }
  }

  /**
   *
   */
  def time[T](func: => T): T = {
    val startTime = System.nanoTime
    try {
      func
    } finally {
      this += System.nanoTime - startTime
    }
  }

  /**
   *
   */
  def start[T](func: ( ()=>Unit )=> T): T = {
    val startTime = System.nanoTime
    def endFunc():Unit = {
      val end = System.nanoTime
      this += System.nanoTime - startTime
    }
    func(endFunc)
  }
}

case class TimeMetric(count:Int, total:Long, minimum:Long, maximum:Long) {
  def maxTime(unit:TimeUnit) = (maximum).toFloat / unit.toNanos(1)
  def minTime(unit:TimeUnit) = (minimum).toFloat / unit.toNanos(1)
  def totalTime(unit:TimeUnit) = (total).toFloat / unit.toNanos(1)
  def avgTime(unit:TimeUnit) = if( count==0 ) 0f else totalTime(unit) / count
  def avgFrequency(unit:TimeUnit) = 1.toFloat / avgTime(unit)
}