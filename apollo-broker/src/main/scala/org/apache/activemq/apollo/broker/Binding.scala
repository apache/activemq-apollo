/**
 *  Licensed to the Apache Software Foundation (ASF) under one or more
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

import org.apache.activemq.apollo.selector.SelectorParser
import org.apache.activemq.apollo.filter.{ConstantExpression, BooleanExpression}
import org.apache.activemq.apollo.util.ClassFinder
import org.apache.activemq.apollo.util.path.Path
import org.fusesource.hawtbuf.{Buffer, AsciiBuffer}
import collection.JavaConversions._
import org.apache.activemq.apollo.dto._

trait BindingFactory {
  def apply(binding_kind:AsciiBuffer, binding_data:Buffer):Binding
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object BindingFactory {

  val finder = new ClassFinder[BindingFactory]("META-INF/services/org.apache.activemq.apollo/binding-factory.index",classOf[BindingFactory])

  def create(binding_kind:AsciiBuffer, binding_data:Buffer):Binding = {
    finder.singletons.foreach { provider=>
      val rc = provider(binding_kind, binding_data)
      if( rc!=null ) {
        return rc
      }
    }
    throw new IllegalArgumentException("Invalid binding type: "+binding_kind);
  }

}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
trait Binding {

  /**
   * Wires a queue into the a virtual host based on the binding information contained
   * in the buffer.
   */
  def bind(node:LocalRouter, queue:Queue)
  
  def unbind(node:LocalRouter, queue:Queue)

  def dto_class:Class[_ <:DestinationDTO]
  def dto:DestinationDTO = JsonCodec.decode(binding_data, dto_class)

  def binding_kind:AsciiBuffer
  def binding_data:Buffer

  def address:DestinationAddress

  def message_filter:BooleanExpression = ConstantExpression.TRUE

  def config(host:VirtualHost):QueueSettingsDTO

  override def toString = address.toString
}

object QueueDomainQueueBinding extends BindingFactory {

  val POINT_TO_POINT_KIND = new AsciiBuffer("ptp")
  val DESTINATION_PATH = new AsciiBuffer("default");

  def apply(binding_kind:AsciiBuffer, binding_data:Buffer):QueueDomainQueueBinding = {
    if( binding_kind == POINT_TO_POINT_KIND ) {
      val dto = JsonCodec.decode(binding_data, classOf[QueueDestinationDTO])

      // TODO: remove after next release.
      // schema upgrade, we can get rid of this after the next release.
      if( !dto.path.isEmpty ) {
        dto.name = LocalRouter.destination_parser.encode_path_iter(dto.path.toIterable)
      }

      var path: Path = DestinationAddress.decode_path(dto.name)
      new QueueDomainQueueBinding(binding_data, SimpleAddress("queue", path))
    } else {
      null
    }
  }

  def apply(address:DestinationAddress):QueueDomainQueueBinding = {
    val dto = new QueueDestinationDTO(address.id)
    new QueueDomainQueueBinding(JsonCodec.encode(dto), address)
  }

  def queue_config(virtual_host:VirtualHost, path:Path):QueueDTO = {
    import LocalRouter.destination_parser._

    def matches(x:QueueDTO):Boolean = {
      x.id==null || decode_filter(x.id).matches(path)
    }
    virtual_host.config.queues.find(matches _).getOrElse(new QueueDTO)
  }

}


/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class QueueDomainQueueBinding(val binding_data:Buffer, val address:DestinationAddress) extends Binding {

  import QueueDomainQueueBinding._
  def dto_class = classOf[QueueDestinationDTO]
  def binding_kind = POINT_TO_POINT_KIND

  def unbind(node: LocalRouter, queue: Queue) = {
    node.local_queue_domain.unbind(queue)
  }

  def bind(node: LocalRouter, queue: Queue) = {
    node.local_queue_domain.bind(queue)
  }

  override def hashCode = binding_kind.hashCode ^ binding_data.hashCode

  override def equals(o:Any):Boolean = o match {
    case x: QueueDomainQueueBinding => x.binding_data == binding_data
    case _ => false
  }


  def config(host:VirtualHost):QueueDTO = queue_config(host, address.path)
}


object DurableSubscriptionQueueBinding extends BindingFactory {

  val DURABLE_SUB_KIND = new AsciiBuffer("ds")

  def apply(binding_kind:AsciiBuffer, binding_data:Buffer):DurableSubscriptionQueueBinding = {
    if( binding_kind == DURABLE_SUB_KIND ) {
      var dto = JsonCodec.decode(binding_data, classOf[DurableSubscriptionDestinationDTO])
      // TODO: remove after next release.
      // schema upgrade, we can get rid of this after the next release.
      if( !dto.path.isEmpty ) {
        dto.name = dto.path.get(0);
      }
      import collection.JavaConversions._
      val topics = dto.topics.toSeq.toArray.map { t=>
        new SimpleAddress("topic", DestinationAddress.decode_path(t.name))
      }

      var path: Path = DestinationAddress.decode_path(dto.name)
      DurableSubscriptionQueueBinding(binding_data, SubscriptionAddress(path, dto.selector, topics))
    } else {
      null
    }
  }
  
  def apply(destination:SubscriptionAddress):DurableSubscriptionQueueBinding = {
    val dto = new DurableSubscriptionDestinationDTO(destination.id)
    dto.selector = destination.selector
    destination.topics.foreach { t =>
      dto.topics.add(new TopicDestinationDTO(t.id))
    }
    DurableSubscriptionQueueBinding(JsonCodec.encode(dto), destination)
  }


  def dsub_config(host:VirtualHost, address:SubscriptionAddress) = {
    import LocalRouter.destination_parser._
    import collection.JavaConversions._
    def matches(x:DurableSubscriptionDTO):Boolean = {
      if( x.id != null ) {
        return decode_filter(x.id).matches(address.path)
      }
      if( x.id_regex != null ) {
        // May need to cache the regex...
        val regex = x.id_regex.r
        return regex.findFirstIn(address.id).isDefined
      }
      true
    }
    host.config.dsubs.find(matches _).getOrElse(new DurableSubscriptionDTO)
  }
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class DurableSubscriptionQueueBinding(binding_data:Buffer, address:SubscriptionAddress) extends Binding {
  import DurableSubscriptionQueueBinding._

  def dto_class = classOf[DurableSubscriptionDestinationDTO]
  def binding_kind = DURABLE_SUB_KIND


  def unbind(router: LocalRouter, queue: Queue) = {
    router.local_dsub_domain.unbind(queue)
  }

  def bind(router: LocalRouter, queue: Queue) = {
    router.local_dsub_domain.bind(queue)
  }

  override def hashCode = binding_kind.hashCode ^ binding_data.hashCode

  override def equals(o:Any):Boolean = o match {
    case x: DurableSubscriptionQueueBinding => x.binding_data == binding_data
    case _ => false
  }

  override def message_filter = {
    if ( address.selector==null ) {
      ConstantExpression.TRUE
    } else {
      SelectorParser.parse(address.selector)
    }
  }

  def config(host:VirtualHost):DurableSubscriptionDTO = dsub_config(host, address)
}


object TempQueueBinding {
  val TEMP_DATA = new AsciiBuffer("")
  val TEMP_KIND = new AsciiBuffer("tmp")
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
case class TempQueueBinding(topic:String, address:DestinationAddress, settings:QueueSettingsDTO) extends Binding {
  import TempQueueBinding._

  def binding_kind = TEMP_KIND
  def binding_data = TEMP_DATA


  override def dto: DestinationDTO = new TopicDestinationDTO(topic)
  def dto_class = null

  def unbind(router: LocalRouter, queue: Queue) = {}
  def bind(router: LocalRouter, queue: Queue) = {}

  override def hashCode = if(topic==null) 0 else topic.hashCode

  def config(host: VirtualHost) = settings

  override def equals(o:Any):Boolean = o match {
    case x: TempQueueBinding => x.topic == topic
    case _ => false
  }

  override def toString = super.toString+":"+topic
}
