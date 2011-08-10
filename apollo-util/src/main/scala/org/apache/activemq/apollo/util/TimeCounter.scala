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

  var max = Long.MinValue
  var min = Long.MaxValue
  var total = 0L
  var count = 0

  def apply(reset: Boolean):TimeMetric = {
    val rc = if(count==0) {
      TimeMetric(0, 0, 0, 0)
    } else {
      TimeMetric(count, total, min, max)
    }
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

  def total(unit:TimeUnit):Long = {
    unit.convert(total, TimeUnit.NANOSECONDS)
  }
}

case class TimeMetric(count:Int, total:Long, min:Long, max:Long) {
  def maxTime(unit:TimeUnit) = (max).toFloat / unit.toNanos(1)
  def minTime(unit:TimeUnit) = (min).toFloat / unit.toNanos(1)
  def totalTime(unit:TimeUnit) = (total).toFloat / unit.toNanos(1)
  def avgTime(unit:TimeUnit) = if( count==0 ) 0f else totalTime(unit) / count
  def frequencyTime(unit:TimeUnit) = 1.toFloat / avgTime(unit)
}