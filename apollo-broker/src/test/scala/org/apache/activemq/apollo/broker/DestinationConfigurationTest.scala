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
package org.apache.activemq.apollo.broker

import org.fusesource.hawtbuf.Buffer._
import scala.util.continuations._
import org.apache.activemq.apollo.util.{ServiceControl, FunSuiteSupport}
import org.apache.activemq.apollo.dto.{BindingDTO, DurableSubscriptionBindingDTO, PointToPointBindingDTO}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DestinationConfigurationTest extends FunSuiteSupport {

  test("Simple Config") {
    val uri = "xml:classpath:org/apache/activemq/apollo/broker/destination-config.xml"
    info("Loading broker configuration from the classpath with URI: " + uri)
    val broker = BrokerFactory.createBroker(uri)
    ServiceControl.start(broker, "broker")

    val host = broker.config.virtual_hosts.get(0)

    expect("test") {
      host.host_names.get(0)
    }

    // Let make sure we are reading in the expected config..
    expect(2) {
      host.destinations.size
    }
    expect(3) {
      host.queues.size
    }

    val router = broker.default_virtual_host.router

    def check_tune_queue_buffer(expected:Int)(dto:BindingDTO) = {
      var actual=0
      reset {
        var q = router.create_queue(dto).get
        actual = q.tune_queue_buffer
      }
      expect(expected) {actual}
    }

    check_tune_queue_buffer(333) {
      var p = new PointToPointBindingDTO()
      p.destination = "unified.a"
      p
    }
    check_tune_queue_buffer(444) {
      val p = new DurableSubscriptionBindingDTO()
      p.destination = "unified.b"
      p.client_id = "a"
      p.subscription_id = "b"
      p
    }

    check_tune_queue_buffer(111) {
      var p = new PointToPointBindingDTO()
      p.destination = "notunified.other"
      p
    }

    def dest(v:String) = Binding.destination_parser.parsePath(ascii(v))
    expect(true) {
      router.destinations.chooseValue(dest("unified.a")).unified
    }
    expect(false) {
      router.destinations.chooseValue(dest("notunified.other")).unified
    }
    ServiceControl.stop(broker, "broker")
  }

}
