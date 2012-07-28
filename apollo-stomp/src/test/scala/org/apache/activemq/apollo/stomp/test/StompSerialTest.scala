/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.stomp.test

import org.apache.activemq.apollo.broker._

/**
 * These tests seem to have trouble being run in Parallel
 */
class StompSerialTest extends StompTestSupport with BrokerParallelTestExecution {

  // This is the test case for https://issues.apache.org/jira/browse/APLO-88
  test("ACK then socket close with/without DISCONNECT, should still ACK") {
    for (i <- 1 until 3) {
      connect("1.1")

      def send(id: Int) = {
        client.write(
          "SEND\n" +
                  "destination:/queue/from-seq-end\n" +
                  "message-id:id-" + i + "-" + id + "\n" +
                  "receipt:0\n" +
                  "\n")
        wait_for_receipt("0")
      }

      def get(seq: Long) = {
        val frame = client.receive()
        frame should startWith("MESSAGE\n")
        frame should include("message-id:id-" + i + "-" + seq + "\n")
        client.write(
          "ACK\n" +
                  "subscription:0\n" +
                  "message-id:id-" + i + "-" + seq + "\n" +
                  "\n")
      }

      send(1)
      send(2)

      client.write(
        "SUBSCRIBE\n" +
                "destination:/queue/from-seq-end\n" +
                "id:0\n" +
                "ack:client\n" +
                "\n")
      get(1)
      client.write(
        "DISCONNECT\n" +
                "\n")
      client.close

      connect("1.1")
      client.write(
        "SUBSCRIBE\n" +
                "destination:/queue/from-seq-end\n" +
                "id:0\n" +
                "ack:client\n" +
                "\n")
      get(2)
      client.close
    }
  }

}
