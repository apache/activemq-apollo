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
package org.apache.activemq.apollo.store.cassandra

import org.fusesource.hawtbuf.AsciiBuffer._
import org.fusesource.hawtbuf.Buffer
import com.shorrockin.cascal.model.{StandardKey, Keyspace}
import com.shorrockin.cascal.session.ColumnPredicate

case class Schema(name:String) {

  val keyspace = Keyspace(name)

  /**
   */
  val message = keyspace \ "messages"
  val message_data = message \ "data"

  /**
   */
  val queue = keyspace \ "queues"
  val queue_name = queue \ "name"

  /**
   */
  val entries = keyspace \ "entries"

//  protected val subscriptionColumns = columns(subscriptionKey, "destination" :: "created" :: "prefetchCount" :: "inactivityTimeout" :: Nil)
//
//  protected def columns(key: StandardKey, names: Seq[String]) = {
//    val columns = names.map{n => (key \ (n, "")).name}
//    ColumnPredicate(columns)
//  }
}