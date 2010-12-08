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

import org.fusesource.hawtbuf.{Buffer, AsciiBuffer}
import org.apache.activemq.apollo.selector.SelectorParser
import org.apache.activemq.apollo.filter.{ConstantExpression, BooleanExpression}
import Buffer._
import org.apache.activemq.apollo.dto._
import org.apache.activemq.apollo.util.{OptionSupport, ClassFinder}
import org.apache.activemq.apollo.util.path.{Path, Part}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object BindingFactory {

  trait Provider {
    def create(binding_kind:AsciiBuffer, binding_data:Buffer):Binding
    def create(binding_dto:BindingDTO):Binding
  }

  def discover = {
    val finder = new ClassFinder[Provider]("META-INF/services/org.apache.activemq.apollo/binding-factory.index")
    finder.new_instances
  }

  var providers = discover

  def create(binding_kind:AsciiBuffer, binding_data:Buffer):Binding = {
    providers.foreach { provider=>
      val rc = provider.create(binding_kind, binding_data)
      if( rc!=null ) {
        return rc
      }
    }
    throw new IllegalArgumentException("Invalid binding type: "+binding_kind);
  }
  def create(binding_dto:BindingDTO):Binding = {
    providers.foreach { provider=>
      val rc = provider.create(binding_dto)
      if( rc!=null ) {
        return rc
      }
    }
    throw new IllegalArgumentException("Invalid binding type: "+binding_dto);
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
   * A user friendly description of the binding.
   */
  def label:String

  /**
   * Wires a queue into the a virtual host based on the binding information contained
   * in the buffer.
   */
  def bind(node:RoutingNode, queue:Queue)
  
  def unbind(node:RoutingNode, queue:Queue)

  def binding_kind:AsciiBuffer

  def binding_data:Buffer

  def binding_dto:BindingDTO

  def message_filter:BooleanExpression = ConstantExpression.TRUE

  def matches(config:QueueDTO):Boolean = {
    import DestinationParser.default._
    import OptionSupport._
    var rc = (o(config.destination).map{ x=> parseFilter(ascii(x)).matches(destination) }.getOrElse(true))
    rc = rc && (o(config.kind).map{ x=> x == binding_kind.toString }.getOrElse(true))
    rc
  }

  def destination:Path
}

object QueueBinding {
  val POINT_TO_POINT_KIND = new AsciiBuffer("ptp")
  val DESTINATION_PATH = new AsciiBuffer("default");
}

import QueueBinding._

class QueueBindingFactory extends BindingFactory.Provider {

  def create(binding_kind:AsciiBuffer, binding_data:Buffer) = {
    if( binding_kind == POINT_TO_POINT_KIND ) {
      val dto = new QueueBindingDTO
      dto.destination = binding_data.ascii.toString
      new QueueBinding(binding_data, dto)
    } else {
      null
    }
  }

  def create(binding_dto:BindingDTO) = {
    if( binding_dto.isInstanceOf[QueueBindingDTO] ) {
      val ptp_dto = binding_dto.asInstanceOf[QueueBindingDTO]
      val data = new AsciiBuffer(ptp_dto.destination).buffer
      new QueueBinding(data, ptp_dto)
    } else {
      null
    }
  }
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class QueueBinding(val binding_data:Buffer, val binding_dto:QueueBindingDTO) extends Binding {

  val destination = DestinationParser.decode_path(binding_dto.destination)
  def binding_kind = POINT_TO_POINT_KIND

  def unbind(node: RoutingNode, queue: Queue) = {
    if( node.unified ) {
      node.remove_broadcast_consumer(queue)
    }
  }

  def bind(node: RoutingNode, queue: Queue) = {
    if( node.unified ) {
      node.add_broadcast_consumer(queue)
    }
  }

  def label = binding_dto.destination

  override def hashCode = binding_kind.hashCode ^ binding_data.hashCode

  override def equals(o:Any):Boolean = o match {
    case x: QueueBinding => x.binding_data == binding_data
    case _ => false
  }

}


object SubscriptionBinding {
  val DURABLE_SUB_KIND = new AsciiBuffer("ds")
}

import SubscriptionBinding._

class SubscriptionBindingFactory extends BindingFactory.Provider {
  def create(binding_kind:AsciiBuffer, binding_data:Buffer) = {
    if( binding_kind == DURABLE_SUB_KIND ) {
      new SubscriptionBinding(binding_data, JsonCodec.decode(binding_data, classOf[SubscriptionBindingDTO]))
    } else {
      null
    }
  }
  def create(binding_dto:BindingDTO) = {
    if( binding_dto.isInstanceOf[SubscriptionBindingDTO] ) {
      new SubscriptionBinding(JsonCodec.encode(binding_dto), binding_dto.asInstanceOf[SubscriptionBindingDTO])
    } else {
      null
    }
  }

}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class SubscriptionBinding(val binding_data:Buffer, val binding_dto:SubscriptionBindingDTO) extends Binding {

  val destination = DestinationParser.decode_path(binding_dto.destination)

  def binding_kind = DURABLE_SUB_KIND


  def unbind(node: RoutingNode, queue: Queue) = {
    node.remove_broadcast_consumer(queue)
  }

  def bind(node: RoutingNode, queue: Queue) = {
    node.add_broadcast_consumer(queue)
  }

  def label = {
    var rc = "sub: '"+binding_dto.subscription_id+"'"
    if( binding_dto.filter!=null ) {
      rc += " filtering '"+binding_dto.filter+"'"
    }
    if( binding_dto.client_id!=null ) {
      rc += " for client '"+binding_dto.client_id+"'"
    }
    rc
  }

  override def hashCode = binding_kind.hashCode ^ binding_data.hashCode

  override def equals(o:Any):Boolean = o match {
    case x: SubscriptionBinding => x.binding_data == binding_data
    case _ => false
  }

  override def message_filter = {
    if ( binding_dto.filter==null ) {
      ConstantExpression.TRUE
    } else {
      SelectorParser.parse(binding_dto.filter)
    }
  }

  override def matches(config: QueueDTO): Boolean = {
    import OptionSupport._
    var rc = super.matches(config)
    rc = rc && (o(config.client_id).map{ x=> x == binding_dto.client_id }.getOrElse(true))
    rc = rc && (o(config.subscription_id).map{ x=> x == binding_dto.subscription_id }.getOrElse(true))
    rc
  }
}


object TempBinding {
  val TEMP_DATA = new AsciiBuffer("")
  val TEMP_KIND = new AsciiBuffer("tmp")
  val TEMP_DTO = new TempBindingDTO
}

import TempBinding._

class TempBindingFactory extends BindingFactory.Provider {

  def create(binding_kind:AsciiBuffer, binding_data:Buffer) = {
    if( binding_kind == TEMP_KIND ) {
      new TempBinding("", "")
    } else {
      null
    }
  }

  def create(binding_dto:BindingDTO) = {
    if( binding_dto.isInstanceOf[TempBindingDTO] ) {
      new TempBinding("", "")
    } else {
      null
    }
  }
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class TempBinding(val key:AnyRef, val label:String) extends Binding {
  def this(c:DeliveryConsumer) = this(c, c.connection.map(_.transport.getRemoteAddress).getOrElse("known") )

  val destination = null
  def binding_kind = TEMP_KIND
  def binding_dto = TEMP_DTO
  def binding_data = TEMP_DATA

  def unbind(node: RoutingNode, queue: Queue) = {
    if( node.unified ) {
      node.remove_broadcast_consumer(queue)
    }
  }

  def bind(node: RoutingNode, queue: Queue) = {
    if( node.unified ) {
      node.add_broadcast_consumer(queue)
    }
  }

  override def hashCode = if(key==null) 0 else key.hashCode

  override def equals(o:Any):Boolean = o match {
    case x: TempBinding => x.key == key
    case _ => false
  }

}
