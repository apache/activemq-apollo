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

import org.apache.activemq.apollo.util.ClassFinder
import org.fusesource.hawtbuf.{Buffer, AsciiBuffer}
import org.apache.activemq.apollo.dto.{JsonCodec, DurableSubscriptionBindingDTO, PointToPointBindingDTO, BindingDTO}
import org.apache.activemq.apollo.selector.SelectorParser
import org.apache.activemq.apollo.filter.{ConstantExpression, BooleanExpression}

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

  def destination:AsciiBuffer
}

object PointToPointBinding {
  val POINT_TO_POINT_KIND = new AsciiBuffer("p2p")
  val DESTINATION_PATH = new AsciiBuffer("default");
}

import PointToPointBinding._

class PointToPointBindingFactory extends BindingFactory.Provider {

  def create(binding_kind:AsciiBuffer, binding_data:Buffer) = {
    if( binding_kind == POINT_TO_POINT_KIND ) {
      val dto = new PointToPointBindingDTO
      dto.destination = binding_data.ascii.toString
      new PointToPointBinding(binding_data, dto)
    } else {
      null
    }
  }

  def create(binding_dto:BindingDTO) = {
    if( binding_dto.isInstanceOf[PointToPointBindingDTO] ) {
      val p2p_dto = binding_dto.asInstanceOf[PointToPointBindingDTO]
      val data = new AsciiBuffer(p2p_dto.destination).buffer
      new PointToPointBinding(data, p2p_dto)
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
class PointToPointBinding(val binding_data:Buffer, val binding_dto:PointToPointBindingDTO) extends Binding {

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
    case x: PointToPointBinding => x.binding_data == binding_data
    case _ => false
  }

  def destination = new AsciiBuffer(binding_dto.destination)
}


object DurableSubBinding {
  val DURABLE_SUB_KIND = new AsciiBuffer("ds")
}

import DurableSubBinding._

class DurableSubBindingFactory extends BindingFactory.Provider {
  def create(binding_kind:AsciiBuffer, binding_data:Buffer) = {
    if( binding_kind == DURABLE_SUB_KIND ) {
      new DurableSubBinding(binding_data, JsonCodec.decode(binding_data, classOf[DurableSubscriptionBindingDTO]))
    } else {
      null
    }
  }
  def create(binding_dto:BindingDTO) = {
    if( binding_dto.isInstanceOf[DurableSubscriptionBindingDTO] ) {
      new DurableSubBinding(JsonCodec.encode(binding_dto), binding_dto.asInstanceOf[DurableSubscriptionBindingDTO])
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
class DurableSubBinding(val binding_data:Buffer, val binding_dto:DurableSubscriptionBindingDTO) extends Binding {

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
    case x: DurableSubBinding => x.binding_data == binding_data
    case _ => false
  }

  override def message_filter = {
    if ( binding_dto.filter==null ) {
      ConstantExpression.TRUE
    } else {
      SelectorParser.parse(binding_dto.filter)
    }
  }

  def destination = new AsciiBuffer(binding_dto.destination)

}