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
import org.apache.activemq.apollo.dto._
import store.{PersistentLongCounter, ZeroCopyBufferAllocator, Store, StoreFactory}

trait VirtualHostFactory {
  def create(broker:Broker, dto:VirtualHostDTO):VirtualHost
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object VirtualHostFactory {

  val finder = new ClassFinder[VirtualHostFactory]("META-INF/services/org.apache.activemq.apollo/virtual-host-factory.index",classOf[VirtualHostFactory])

  def create(broker:Broker, dto:VirtualHostDTO):VirtualHost = {
    if( dto == null ) {
      return null
    }
    finder.singletons.foreach { provider=>
      val connector = provider.create(broker, dto)
      if( connector!=null ) {
        return connector;
      }
    }
    return null
  }
}

object DefaultVirtualHostFactory extends VirtualHostFactory with Log {

  def create(broker: Broker, dto: VirtualHostDTO): VirtualHost = dto match {
    case dto:VirtualHostDTO =>
      if( dto.getClass != classOf[VirtualHostDTO] ) {
        // ignore sub classes of AcceptingVirtualHostDTO
        null;
      } else {
        val rc = new VirtualHost(broker, dto.id)
        rc.config = dto
        rc
      }
    case _ =>
      null
  }
}

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
  val router:Router = new LocalRouter(this)

  var names:List[String] = Nil;

  var store:Store = null
  val queue_id_counter = new LongCounter()

  val session_counter = new PersistentLongCounter("session_counter")

  var authenticator:Authenticator = _
  var authorizer:Authorizer = _

  var audit_log:Log = _
  var security_log:Log  = _
  var connection_log:Log = _
  var console_log:Log = _

  // This gets set if client should get redirected to another address.
  @volatile
  var client_redirect:Option[String] = None

  override def toString = if (config==null) "virtual-host" else "virtual-host: "+config.id

  /**
   * Validates and then applies the configuration.
   */
  def update(config: VirtualHostDTO, on_completed:Runnable) = dispatch_queue {
    if ( !service_state.is_started ) {
      this.config = config
      on_completed.run
    } else {

      // in some cases we have to restart the virtual host..
      if( config.store != this.config.store ) {
        stop(^{
          this.config = config
          start(on_completed)
        })
      } else {
        this.config = config
        apply_update
        this.router.apply_update(on_completed)
      }
    }
  }

  def apply_update:Unit = {
    // Configure the logging categories...
    val log_category = config.log_category.getOrElse(new LogCategoryDTO)
    security_log = Option(log_category.security).map(Log(_)).getOrElse(broker.security_log)
    audit_log = Option(log_category.audit).map(Log(_)).getOrElse(broker.audit_log)
    connection_log = Option(log_category.connection).map(Log(_)).getOrElse(broker.connection_log)
    console_log = Option(log_category.console).map(Log(_)).getOrElse(broker.console_log)

    if (config.authentication != null) {
      if (config.authentication.enabled.getOrElse(true)) {
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

  }

  override protected def _start(on_completed:Runnable):Unit = {
    apply_update

    store = StoreFactory.create(config.store)

    val tracker = new LoggingTracker("virtual host startup", console_log)
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

      val tracker = new LoggingTracker("virtual host startup", console_log)

      // The default host handles persisting the connection id counter.
      if(store!=null) {
        if(session_counter.get == 0) {
          val task = tracker.task("load session counter")
          session_counter.init(store) {
            task.run()
          }
        } else {
          session_counter.connect(store)
        }
      }

      tracker.start(router)
      tracker.callback(on_completed)
    }

  }


  override protected def _stop(on_completed:Runnable):Unit = {

    val tracker = new LoggingTracker("virtual host shutdown", console_log)
    tracker.stop(router);
    if( store!=null ) {
      val task = tracker.task("store session counter")
      session_counter.disconnect{
        tracker.stop(store);
        task.run()
      }
    }
    tracker.callback(on_completed)
  }


}
