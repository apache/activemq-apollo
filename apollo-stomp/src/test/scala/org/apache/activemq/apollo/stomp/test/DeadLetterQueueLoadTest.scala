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

import org.apache.activemq.apollo.broker.BrokerParallelTestExecution
import java.lang.String

/**
 */
class DeadLetterQueueLoadTest extends StompTestSupport with BrokerParallelTestExecution {

  override def broker_config_uri: String = "xml:classpath:apollo-stomp-bdb.xml"

  for (i <- 1 to 16 )
  test("naker.load."+i) {
    connect("1.1")
    val dlq_client = connect("1.1", new StompClient)
    subscribe("0", "/queue/nacker.load."+i, "client", false, "", false)
    subscribe("dlq", "/queue/dlq.nacker.load."+i, "client", false, "", false, c=dlq_client)

    for( j <- 1 to 1000 ) {
      async_send("/queue/nacker.load."+i, j)
      assert_received(j, "0")(false)
      assert_received(j, "0")(false)
      // It should be sent to the DLQ after the 2nd nak
      assert_received(j, "dlq", c=dlq_client)
    }
  }

}
