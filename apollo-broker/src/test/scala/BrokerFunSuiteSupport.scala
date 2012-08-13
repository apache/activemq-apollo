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

import org.apache.activemq.apollo.util.{ServiceControl, Logging, FunSuiteSupport}
import java.net.InetSocketAddress
import org.apache.activemq.apollo.util._
import FileSupport._
import org.fusesource.hawtdispatch._
import org.apache.activemq.apollo.dto.{DestMetricsDTO, AggregateDestMetricsDTO, QueueStatusDTO, TopicStatusDTO}
import collection.immutable.HashMap
import java.io.File
import org.scalatest.{ParallelTestExecution, OneInstancePerTest}
import java.util

object BrokerTestSupport {
  import FutureResult._

  def connector_port(broker:Broker, connector: String): Option[Int] = Option(connector).map {
    id => broker.connectors.get(id).map(_.socket_address.asInstanceOf[InetSocketAddress].getPort).getOrElse(0)
  }

  def queue_exists(broker:Broker, name: String): Boolean = {
    val host = broker.default_virtual_host
    host.dispatch_queue.future {
      val router = host.router.asInstanceOf[LocalRouter]
      router.local_queue_domain.destination_by_id.get(name).isDefined
    }.await()
  }

  def delete_queue(broker:Broker, name: String) = {
    val host = broker.default_virtual_host
    host.dispatch_queue.future {
      val router = host.router.asInstanceOf[LocalRouter]
      for( node<- router.local_queue_domain.destination_by_id.get(name) ) {
        router._destroy_queue(node)
      }
    }.await()
  }

  def topic_exists(broker:Broker, name: String): Boolean = {
    val host = broker.default_virtual_host
    host.dispatch_queue.future {
      val router = host.router.asInstanceOf[LocalRouter]
      router.local_topic_domain.destination_by_id.get(name).isDefined
    }.await()
  }

  def topic_status(broker:Broker, name: String): TopicStatusDTO = {
    val host = broker.default_virtual_host
    sync(host) {
      val router = host.router.asInstanceOf[LocalRouter]
      router.local_topic_domain.destination_by_id.get(name).get.status
    }
  }

  def get_queue_metrics(broker:Broker): AggregateDestMetricsDTO = {
    unwrap_future_result(broker.default_virtual_host.get_queue_metrics)
  }

  def get_topic_metrics(broker:Broker): AggregateDestMetricsDTO = {
    unwrap_future_result(broker.default_virtual_host.get_topic_metrics)
  }

  def get_dsub_metrics(broker:Broker): AggregateDestMetricsDTO = {
    unwrap_future_result(broker.default_virtual_host.get_dsub_metrics)
  }

  def get_dest_metrics(broker:Broker):AggregateDestMetricsDTO = {
    unwrap_future_result(broker.default_virtual_host.get_dest_metrics)
  }

  def queue_status(broker:Broker, name: String): QueueStatusDTO = {
    val host = broker.default_virtual_host
    sync(host) {
      val router = host.router.asInstanceOf[LocalRouter]
      val queue = router.local_queue_domain.destination_by_id.get(name).get
      sync(queue) {
        queue.status(false)
      }
    }
  }

  def dsub_status(broker:Broker, name: String): QueueStatusDTO = {
    val host = broker.default_virtual_host
    sync(host) {
      val router = host.router.asInstanceOf[LocalRouter]
      router.local_dsub_domain.destination_by_id.get(name).get.status(false)
    }
  }

  def webadmin_uri(broker:Broker, scheme:String) = {
    Option(broker.web_server).flatMap(_.uris().find(_.getScheme == scheme)).get
  }

}

trait BrokerParallelTestExecution extends ParallelTestExecution {
  self: BrokerFunSuiteSupport =>

  override def newInstance = {
    val rc = super.newInstance.asInstanceOf[BrokerFunSuiteSupport]
    rc.before_and_after_all_object = self
    rc.broker = self.broker
    rc.port = self.port
    rc
  }

}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class BrokerFunSuiteSupport extends FunSuiteSupport with Logging { // with ShouldMatchers with BeforeAndAfterEach with Logging {
  var before_and_after_all_object:BrokerFunSuiteSupport = _
  var broker: Broker = null
  var port = 0

  def broker_config_uri = "xml:classpath:apollo.xml"

  def createBroker = {
    val props = new java.util.Properties(System.getProperties)
    props.setProperty("testdatadir", test_data_dir.getCanonicalPath)
    BrokerFactory.createBroker(broker_config_uri, props)
  }

  override def beforeAll() = {
    super.beforeAll()
    if( before_and_after_all_object==null ) {
      try {
        broker = createBroker
        broker.setTmp(test_data_dir / "tmp")
        broker.getTmp().mkdirs()
        ServiceControl.start(broker)
        port = broker.get_socket_address.asInstanceOf[InetSocketAddress].getPort
      } catch {
        case e: Throwable => e.printStackTrace
      }
    } else {
      broker = before_and_after_all_object.broker
      port = before_and_after_all_object.port
    }
  }

  override def afterAll() = {
    if( before_and_after_all_object==null ) {
      ServiceControl.stop(broker)
    }
    super.afterAll()
  }

  def connector_port(connector: String) = BrokerTestSupport.connector_port(broker, connector)
  def queue_exists(name: String) = BrokerTestSupport.queue_exists(broker, name)
  def delete_queue(name: String) = BrokerTestSupport.delete_queue(broker, name)
  def topic_exists(name: String) = BrokerTestSupport.topic_exists(broker, name)
  def topic_status(name: String) = BrokerTestSupport.topic_status(broker, name)
  def get_queue_metrics = BrokerTestSupport.get_queue_metrics(broker)
  def get_topic_metrics = BrokerTestSupport.get_topic_metrics(broker)
  def get_dsub_metrics = BrokerTestSupport.get_dsub_metrics(broker)
  def get_dest_metrics = BrokerTestSupport.get_dest_metrics(broker)
  def queue_status(name: String) = BrokerTestSupport.queue_status(broker, name)
  def dsub_status(name: String) = BrokerTestSupport.dsub_status(broker, name)
  def webadmin_uri(scheme:String = "http") = BrokerTestSupport.webadmin_uri(broker, scheme)

  def json(value:Any) = org.apache.activemq.apollo.dto.JsonCodec.encode(value).ascii().toString;

}

class MultiBrokerTestSupport extends FunSuiteSupport {

  case class BrokerAdmin(override val broker_config_uri:String) extends BrokerFunSuiteSupport

  def broker_config_uris = Array("xml:classpath:apollo.xml")
  var admins = Array[BrokerAdmin]()

  override protected def beforeAll() = {
    super.beforeAll()
    try {
      admins = broker_config_uris.map(BrokerAdmin(_))
      admins.foreach(_.beforeAll)
    } catch {
      case e: Throwable => e.printStackTrace
    }
  }

  override protected def afterAll() = {
    for( admin <- admins ) {
      try {
        admin.afterAll
      } catch {
        case e => debug(e)
      }
    }
    admins = Array()
    super.afterAll()
  }

}