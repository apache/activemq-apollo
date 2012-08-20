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

package org.apache.activemq.apollo.broker.store



import org.fusesource.hawtbuf.Buffer
import java.util.concurrent.atomic.AtomicReference
import collection.mutable.ListBuffer

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class QueueEntryRecord {

  var queue_key = 0L
  var entry_seq = 0L
  var message_key = 0L
  var message_locator:AtomicReference[Object] = _
  var attachment:Buffer = _
  var size = 0
  var expiration = 0L
  var redeliveries:Short = 0
  var sender:List[Buffer] = _

}
