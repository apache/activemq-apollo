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
package org.apache.activemq.apollo.openwire

import command._
import org.apache.activemq.apollo.broker.security.SecurityContext
import collection.mutable.HashMap
import DestinationConverter._
import support.advisory.AdvisorySupport
import org.apache.activemq.apollo.util._
import java.util.Map.Entry
import org.apache.activemq.apollo.broker._
import org.fusesource.hawtdispatch._
import org.fusesource.hawtbuf.UTF8Buffer

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object DestinationAdvisoryRouterListenerFactory extends RouterListenerFactory {
  def create(router: Router) = new DestinationAdvisoryRouterListener(router)
}

object DestinationAdvisoryRouterListener extends Log {
  final val ID_GENERATOR = new IdGenerator
}

/**
 * <p>
 *   A listener to Route events which implements Destination advisories
 *   which are needed
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DestinationAdvisoryRouterListener(router: Router) extends RouterListener {

  import DestinationAdvisoryRouterListener._

  final val destination_advisories = HashMap[ActiveMQDestination, Delivery]()
  final val advisoryProducerId = new ProducerId
  final val messageIdGenerator = new LongSequenceGenerator

  advisoryProducerId.setConnectionId(new UTF8Buffer(ID_GENERATOR.generateId))
  var enabled = false


  class ProducerRoute extends DeliveryProducerRoute(router) {
    override def dispatch_queue = router.virtual_host.dispatch_queue
  }

  var producerRoutes = new LRUCache[List[ConnectAddress], ProducerRoute](10) {
    override def onCacheEviction(eldest: Entry[List[ConnectAddress], ProducerRoute]) = {
      router.disconnect(eldest.getKey.toArray, eldest.getValue)
    }
  }

  def on_create(dest: DomainDestination, security: SecurityContext) = {
    val ow_destination = to_activemq_destination(Array(dest.address))
    if (ow_destination!=null && !AdvisorySupport.isAdvisoryTopic(ow_destination)) {
      destination_advisories.getOrElseUpdate(ow_destination, {
        var info = new DestinationInfo(null, DestinationInfo.ADD_OPERATION_TYPE, ow_destination)
        val topic = AdvisorySupport.getDestinationAdvisoryTopic(ow_destination);
        val advisory = create_advisory_delivery(topic, info)
        send(advisory)
        advisory
      })
    }
  }

  def on_destroy(dest: DomainDestination, security: SecurityContext) = {
    val destination = to_activemq_destination(Array(dest.address))
    if (destination!=null && !AdvisorySupport.isAdvisoryTopic(destination)) {
      for (info <- destination_advisories.remove(destination)) {
        var info = new DestinationInfo(null, DestinationInfo.REMOVE_OPERATION_TYPE, destination)
        val topic = AdvisorySupport.getDestinationAdvisoryTopic(destination);
        send(create_advisory_delivery(topic, info));
      }
    }
  }

  def on_bind(dest: DomainDestination, consumer: DeliveryConsumer, security: SecurityContext) = {
    val destination = to_activemq_destination(Array(dest.address))
    if (destination!=null && AdvisorySupport.isDestinationAdvisoryTopic(destination)) {
      // replay the destination advisories..
      enabled = true
      if( !destination_advisories.isEmpty ) {
        val producer = new ProducerRoute
        producer.bind(consumer::Nil, ()=>{})
        producer.connected()
        for( info <- destination_advisories.values ) {
          producer.offer(info)
        }
      }
    }
  }

  def on_unbind(dest: DomainDestination, consumer: DeliveryConsumer, persistent: Boolean) = {
  }
  def on_connect(dest: DomainDestination, producer: BindableDeliveryProducer, security: SecurityContext) = {
  }
  def on_disconnect(dest: DomainDestination, producer: BindableDeliveryProducer) = {
  }


  def close = {
    import collection.JavaConversions._
    for (entry <- producerRoutes.entrySet()) {
      router.disconnect(entry.getKey.toArray, entry.getValue)
    }
    producerRoutes.clear
  }

  def create_advisory_delivery(topic: ActiveMQTopic, command: Command) = {

    // advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_NAME, getBrokerName());
    // val id = getBrokerId() != null ? getBrokerId().getValue(): "NOT_SET";
    // advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_ID, id);
    val message = new ActiveMQMessage()
    message.setStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_ID, "NOT_SET");

    //    val url = getBrokerService().getVmConnectorURI().toString();
    //    if (getBrokerService().getDefaultSocketURIString() != null) {
    //      url = getBrokerService().getDefaultSocketURIString();
    //    }
    //    advisoryMessage.setStringProperty(AdvisorySupport.MSG_PROPERTY_ORIGIN_BROKER_URL, url);

    //set the data structure
    message.setDataStructure(command);
    message.setPersistent(false);
    message.setType(AdvisorySupport.ADIVSORY_MESSAGE_TYPE_BUFFER);
    message.setMessageId(new MessageId(advisoryProducerId, messageIdGenerator.getNextSequenceId()));
//    message.setTargetConsumerId(targetConsumerId);
    message.setDestination(topic);
    message.setResponseRequired(false);
    message.setProducerId(advisoryProducerId);

    val delivery = new Delivery
    delivery.message = new OpenwireMessage(message)
    delivery.size = message.getSize
    delivery
  }

  def send(delivery:Delivery): Unit = {
    val message = delivery.message.asInstanceOf[OpenwireMessage].message
    val dest = to_destination_dto(message.getDestination,null)
    val key = dest.toList
    if( enabled && router.virtual_host.service_state.is_started ) {
      val route = producerRoutes.get(key) match {
        case null =>
          // create the producer route...
          val route = new ProducerRoute
          producerRoutes.put(key, route)
          val rc = router.connect(dest, route, null)
          rc match {
            case Some(failure) => warn("Could not connect to advisory topic '%s' due to: %s", message.getDestination, failure)
            case None =>
          }
          route

        case route => route
      }
      route.offer(delivery)
    }
  }

}

