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

package org.apache.activemq.apollo.amqp.test

import org.apache.qpid.proton.amqp.messaging.{AmqpValue, Source, Target}
import org.fusesource.hawtdispatch._
import org.apache.qpid.proton.hawtdispatch.api._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */

  class AmqpConnectionTest extends AmqpTestSupport {

  def print_result[T](action: String)(then_action: => Unit): Callback[T] = new Callback[T] {
    def onSuccess(value: T) {
      println(action + " completed");
      then_action
    }

    def onFailure(value: Throwable) {
      println(action + " failed: " + value);
      value.printStackTrace()
    }
  }

  def then_do[T](func: (T) => Unit): Callback[T] = new Callback[T] {
    def onSuccess(value: T) {
      func(value)
    }

    def onFailure(value: Throwable) {
      value.printStackTrace()
    }
  }

  test("Sender Open") {
    val amqp = new AmqpConnectOptions();
    amqp.setHost("localhost", port)
    amqp.setUser("admin");
    amqp.setPassword("password");

    val connection = AmqpConnection.connect(amqp)
    connection.queue() {
      var session = connection.createSession()
      val target = new Target
      target.setAddress("queue://FOO")
      val sender = session.createSender(target);
      val md = sender.send(session.createTextMessage("Hello World"))
      md.onSettle(print_result("message sent") {
        println("========================================================")
        println("========================================================")
        val source = new Source
        source.setAddress("queue://FOO")
        val receiver = session.createReceiver(source);
        receiver.resume()
        receiver.setDeliveryListener(new AmqpDeliveryListener {
          def onMessageDelivery(delivery: MessageDelivery) = {
            println("Received: " + delivery.getMessage().getBody().asInstanceOf[AmqpValue].getValue);
            delivery.settle()
            connection.close()
          }
        })
      })
    }
    connection.waitForDisconnected()
  }
}