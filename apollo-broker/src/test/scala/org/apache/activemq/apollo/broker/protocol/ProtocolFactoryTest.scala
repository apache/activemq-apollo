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
package org.apache.activemq.apollo.broker.protocol

import org.apache.activemq.apollo.util.FunSuiteSupport
import org.scalatest.matchers.ShouldMatchers

/**
 * Verify the protocols in the apollo-broker module can be found.
 *
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ProtocolFactoryTest extends FunSuiteSupport with ShouldMatchers{
  test("ProtocolFactory tests"){
    var proto = ProtocolFactory.get("udp")
    proto should not be (None)

    proto = ProtocolFactory.get("ssl")
    proto should not be (None)

    proto = ProtocolFactory.get("any")
    proto should not be (None)

    // The stomp and openwire protocols will not be available yet
    // since their impl are not on the classpath yet.
  }
}
