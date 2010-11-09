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
package org.apache.activemq.apollo.broker.protocol

import org.apache.activemq.apollo.transport.Transport
import java.util.concurrent.TimeUnit

/**
 * <p>A HeartBeatMonitor can be used to periodically check the activity
 * of a transport to see if it is still alive or if a keep alive
 * packet needs to be transmitted to keep it alive.</p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class HeartBeatMonitor() {

  var transport:Transport = _
  var write_interval = 0L
  var read_interval = 0L

  var on_keep_alive = ()=>{}
  var on_dead = ()=>{}

  var session = 0

  def schedual_check_writes(session:Int):Unit = {
    val last_write_counter = transport.getProtocolCodec.getWriteCounter()
    transport.getDispatchQueue.after(write_interval, TimeUnit.MILLISECONDS) {
      if( this.session == session ) {
        if( last_write_counter==transport.getProtocolCodec.getWriteCounter ) {
          on_keep_alive()
        }
        schedual_check_writes(session)
      }
    }
  }

  def schedual_check_reads(session:Int):Unit = {
    val last_read_counter = transport.getProtocolCodec.getReadCounter()
    transport.getDispatchQueue.after(read_interval, TimeUnit.MILLISECONDS) {
      if( this.session == session ) {
        if( last_read_counter==transport.getProtocolCodec.getReadCounter ) {
          on_dead()
        }
        schedual_check_reads(session)
      }
    }
  }

  def start = {
    session += 1
    if( write_interval!=0 ) {
      schedual_check_writes(session)
    }
    if( read_interval!=0 ) {
      schedual_check_reads(session)
    }
  }

  def stop = {
    session += 1
  }
}