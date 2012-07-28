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

import java.lang.String
import org.apache.activemq.apollo.broker._

class StompWildcardParallelTest extends StompTestSupport with BrokerParallelTestExecution {

  def path_separator = "."

  test("Wildcard subscription") {
    connect("1.1")

    client.write(
      "SUBSCRIBE\n" +
              "destination:/queue/foo" + path_separator + "*\n" +
              "id:1\n" +
              "receipt:0\n" +
              "\n")

    wait_for_receipt("0")

    def put(dest: String) = {
      client.write(
        "SEND\n" +
                "destination:/queue/" + dest + "\n" +
                "\n" +
                "message:" + dest + "\n")
    }

    def get(dest: String) = {
      val frame = client.receive()
      frame should startWith("MESSAGE\n")
      frame should endWith("\n\nmessage:%s\n".format(dest))
    }

    // We should not get this one..
    put("bar" + path_separator + "a")

    put("foo" + path_separator + "a")
    get("foo" + path_separator + "a")

    put("foo" + path_separator + "b")
    get("foo" + path_separator + "b")
  }
}
