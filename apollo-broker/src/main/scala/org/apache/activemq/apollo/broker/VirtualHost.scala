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
package org.apache.activemq.apollo.broker;

import _root_.java.util.{ArrayList, HashMap}
import _root_.java.lang.{String}
import _root_.scala.collection.JavaConversions._
import org.fusesource.hawtdispatch._

import java.util.concurrent.TimeUnit
import org.apache.activemq.apollo.util._
import path.PathFilter
import org.fusesource.hawtbuf.{Buffer, AsciiBuffer}
import collection.JavaConversions
import java.util.concurrent.atomic.AtomicLong
import org.apache.activemq.apollo.util.OptionSupport._
import org.apache.activemq.apollo.util.path.{Path, PathParser}
import security.{AclAuthorizer, JaasAuthenticator, Authenticator, Authorizer}
import org.apache.activemq.apollo.broker.store.{ZeroCopyBufferAllocator, Store, StoreFactory}
import org.apache.activemq.apollo.dto._

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object VirtualHost extends Log {
}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class VirtualHost(val broker: Broker, val id:String) extends BaseService {
  import VirtualHost._
  
  override val dispatch_queue:DispatchQueue = createQueue("virtual-host") // getGlobalQueue(DispatchPriority.HIGH).createQueue("virtual-host")

  var config:VirtualHostDTO = _
  var router:Router = _

  var names:List[String] = Nil;

  var store:Store = null
  val queue_id_counter = new LongCounter()

  val session_counter = new AtomicLong(0)

  var authenticator:Authenticator = _
  var authorizer:Authorizer = _

  var audit_log:Log = _
  var security_log:Log  = _
  var connection_log:Log = _
  var console_log:Log = _

  override def toString = if (config==null) "virtual-host" else "virtual-host: "+config.id

  /**
   * Validates and then applies the configuration.
   */
  def update(config: VirtualHostDTO, on_completed:Runnable) = dispatch_queue{
    this.config = config

    if( service_state.is_started ) {
    }
  }

  override protected def _start(on_completed:Runnable):Unit = {

    // Configure the logging categories...
    val log_category = config.log_category.getOrElse(new LogCategoryDTO)
    security_log = Option(log_category.security).map(Log(_)).getOrElse(broker.security_log)
    audit_log = Option(log_category.audit).map(Log(_)).getOrElse(broker.audit_log)
    connection_log = Option(log_category.connection).map(Log(_)).getOrElse(broker.connection_log)
    console_log = Option(log_category.console).map(Log(_)).getOrElse(broker.console_log)

    val tracker = new LoggingTracker("virtual host startup", console_log, dispatch_queue)

    if( config.authentication != null ) {
      if( config.authentication.enabled.getOrElse(true) ) {
        // Virtual host has it's own settings.
        authenticator = new JaasAuthenticator(config.authentication, security_log)
        authorizer = new AclAuthorizer(config.authentication.acl_principal_kinds().toList, security_log)
      } else {
        // Don't use security on this host.
        authenticator = null
        authorizer = null
      }
    } else {
      // use the broker's settings..
      authenticator = broker.authenticator
      authorizer = broker.authorizer
    }

    router = RouterFactory.create(this)
    store = StoreFactory.create(config.store)

    if( store!=null ) {
      val task = tracker.task("store startup")
      console_log.info("Starting store: "+store)
      store.start {
        {
          val task = tracker.task("store get last queue key")
          store.get_last_queue_key{ key=>
            key match {
              case Some(x)=>
                queue_id_counter.set(key.get)
              case None =>
                warn("Could not get last queue key")
            }
            task.run
          }

          if( config.purge_on_startup.getOrElse(false) ) {
            val task = tracker.task("store purge")
            store.purge {
              task.run
            }
          }
        }
        task.run
      }
    }

    tracker.callback {
      val tracker = new LoggingTracker("virtual host startup", console_log, dispatch_queue)
      tracker.start(router)
      tracker.callback(on_completed)
    }

  }


  override protected def _stop(on_completed:Runnable):Unit = {

    val tracker = new LoggingTracker("virtual host shutdown", console_log, dispatch_queue)
    tracker.stop(router);
    if( store!=null ) {
      tracker.stop(store);
    }
    tracker.callback(on_completed)
  }


}
