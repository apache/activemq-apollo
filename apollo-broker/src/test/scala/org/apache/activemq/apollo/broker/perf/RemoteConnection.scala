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

import org.apache.activemq.apollo.util.metric._
import org.apache.activemq.apollo.broker.{Destination, Delivery, Connection}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.TimeUnit
import org.fusesource.hawtdispatch.ScalaDispatch._
import java.io.IOException
import org.apache.activemq.apollo.transport.TransportFactory

abstract class RemoteConnection extends Connection {
  var uri: String = null
  var name: String = null

  val rate = new MetricCounter()
  var rateAggregator: MetricAggregator = null

  var stopping: AtomicBoolean = null
  var destination: Destination = null

  def init = {
    if (rate.getName == null) {
      rate.name(name + " Rate")
    }
    rateAggregator.add(rate)
  }

  var callbackWhenConnected: Runnable = null

  override protected def _start(onComplete: Runnable) = {
    callbackWhenConnected = onComplete
    transport = TransportFactory.connect(uri)
    super._start(^ {})
  }

  override def onTransportConnected() = {
    onConnected()
    transport.resumeRead
    callbackWhenConnected.run
    callbackWhenConnected = null
  }

  protected def onConnected()

  override def onTransportFailure(error: IOException) = {
    if (!stopped) {
      if (stopping.get()) {
        transport.stop
      } else {
        onFailure(error)
        if (callbackWhenConnected != null) {
          warn("connect attempt failed. will retry connection..")
          dispatchQueue.dispatchAfter(50, TimeUnit.MILLISECONDS, ^ {
            if (stopping.get()) {
              callbackWhenConnected.run
            } else {
              // try to connect again...
              transport = TransportFactory.connect(uri)
              super._start(^ {})
            }
          })
        }
      }
    }
  }
}

abstract class RemoteConsumer extends RemoteConnection {
  var thinkTime: Long = 0
  var selector: String = null
  var durable = false
  var persistent = false

  protected def messageReceived()
}


abstract class RemoteProducer extends RemoteConnection {
  var messageIdGenerator: AtomicLong = null
  var priority = 0
  var persistent = false
  var priorityMod = 0
  var counter = 0
  var producerId = 0
  var property: String = null
  var next: Delivery = null
  var thinkTime: Long = 0

  var filler: String = null
  var payloadSize = 20

  override def init = {
    super.init

    if (payloadSize > 0) {
      var sb = new StringBuilder(payloadSize)
      for (i <- 0 until payloadSize) {
        sb.append(('a' + (i % 26)).toChar)
      }
      filler = sb.toString()
    }
  }

  def createPayload(): String = {
    if (payloadSize >= 0) {
      var sb = new StringBuilder(payloadSize)
      sb.append(name)
      sb.append(':')
      counter += 1
      sb.append(counter)
      sb.append(':')
      var length = sb.length
      if (length <= payloadSize) {
        sb.append(filler.subSequence(0, payloadSize - length))
        return sb.toString()
      } else {
        return sb.substring(0, payloadSize)
      }
    } else {
      counter += 1
      return name + ":" + (counter)
    }
  }

}