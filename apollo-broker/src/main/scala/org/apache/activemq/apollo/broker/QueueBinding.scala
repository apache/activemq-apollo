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
import org.apache.activemq.apollo.util.path.Path

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object QueueBinding {

  trait Provider {
    def create(binding_kind:AsciiBuffer, binding_data:Buffer):QueueBinding
    def create(binding_dto:DestinationDTO):QueueBinding
  }

  def discover = {
    val finder = new ClassFinder[Provider]("META-INF/services/org.apache.activemq.apollo/binding-factory.index")
    finder.new_instances
  }

  var providers = discover

  def create(binding_kind:AsciiBuffer, binding_data:Buffer):QueueBinding = {
    providers.foreach { provider=>
      val rc = provider.create(binding_kind, binding_data)
      if( rc!=null ) {
        return rc
      }
    }
    throw new IllegalArgumentException("Invalid binding type: "+binding_kind);
  }
  def create(binding_dto:DestinationDTO):QueueBinding = {
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
trait QueueBinding {

  /**
   * A user friendly description of the binding.
   */
  def label:String

  /**
   * Wires a queue into the a virtual host based on the binding information contained
   * in the buffer.
   */
  def bind(node:LocalRouter, queue:Queue)
  
  def unbind(node:LocalRouter, queue:Queue)

  def binding_kind:AsciiBuffer

  def binding_data:Buffer

  def binding_dto:DestinationDTO

  def message_filter:BooleanExpression = ConstantExpression.TRUE

  def destination:Path
}

object QueueDomainQueueBinding extends QueueBinding.Provider {

  val POINT_TO_POINT_KIND = new AsciiBuffer("ptp")
  val DESTINATION_PATH = new AsciiBuffer("default");

  def create(binding_kind:AsciiBuffer, binding_data:Buffer) = {
    if( binding_kind == POINT_TO_POINT_KIND ) {
      val dto = new QueueDestinationDTO
      dto.name = binding_data.ascii.toString
      new QueueDomainQueueBinding(binding_data, dto)
    } else {
      null
    }
  }

  def create(binding_dto:DestinationDTO) = {
    if( binding_dto.isInstanceOf[QueueDestinationDTO] ) {
      val ptp_dto = binding_dto.asInstanceOf[QueueDestinationDTO]
      val data = new AsciiBuffer(ptp_dto.name).buffer
      new QueueDomainQueueBinding(data, ptp_dto)
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
class QueueDomainQueueBinding(val binding_data:Buffer, val binding_dto:QueueDestinationDTO) extends QueueBinding {

  import QueueDomainQueueBinding._

  val destination = DestinationParser.decode_path(binding_dto.name)
  def binding_kind = POINT_TO_POINT_KIND

  def unbind(node: LocalRouter, queue: Queue) = {
    node.queue_domain.unbind(queue)
  }

  def bind(node: LocalRouter, queue: Queue) = {
    node.queue_domain.bind(queue)
  }

  def label = binding_dto.name

  override def hashCode = binding_kind.hashCode ^ binding_data.hashCode

  override def equals(o:Any):Boolean = o match {
    case x: QueueDomainQueueBinding => x.binding_data == binding_data
    case _ => false
  }

}


object DurableSubscriptionQueueBinding extends QueueBinding.Provider {

  val DURABLE_SUB_KIND = new AsciiBuffer("ds")

  def create(binding_kind:AsciiBuffer, binding_data:Buffer) = {
    if( binding_kind == DURABLE_SUB_KIND ) {
      new DurableSubscriptionQueueBinding(binding_data, JsonCodec.decode(binding_data, classOf[DurableSubscriptionDestinationDTO]))
    } else {
      null
    }
  }
  def create(binding_dto:DestinationDTO) = {
    if( binding_dto.isInstanceOf[DurableSubscriptionDestinationDTO] ) {
      new DurableSubscriptionQueueBinding(JsonCodec.encode(binding_dto), binding_dto.asInstanceOf[DurableSubscriptionDestinationDTO])
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
class DurableSubscriptionQueueBinding(val binding_data:Buffer, val binding_dto:DurableSubscriptionDestinationDTO) extends QueueBinding {
  import DurableSubscriptionQueueBinding._

  val destination = DestinationParser.decode_path(binding_dto.name)

  def binding_kind = DURABLE_SUB_KIND


  def unbind(router: LocalRouter, queue: Queue) = {
    router.topic_domain.unbind(queue)
  }

  def bind(router: LocalRouter, queue: Queue) = {
    router.topic_domain.bind(queue)
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
    case x: DurableSubscriptionQueueBinding => x.binding_data == binding_data
    case _ => false
  }

  override def message_filter = {
    if ( binding_dto.filter==null ) {
      ConstantExpression.TRUE
    } else {
      SelectorParser.parse(binding_dto.filter)
    }
  }
}


object TempQueueBinding extends QueueBinding.Provider {
  val TEMP_DATA = new AsciiBuffer("")
  val TEMP_KIND = new AsciiBuffer("tmp")
  val TEMP_DTO = null

  def create(binding_kind:AsciiBuffer, binding_data:Buffer) = {
    if( binding_kind == TEMP_KIND ) {
      new TempQueueBinding("", "")
    } else {
      null
    }
  }

  def create(binding_dto:DestinationDTO) = throw new UnsupportedOperationException
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class TempQueueBinding(val key:AnyRef, val label:String) extends QueueBinding {
  import TempQueueBinding._

  def this(c:DeliveryConsumer) = this(c, c.connection.map(_.transport.getRemoteAddress).getOrElse("known") )

  val destination = null
  def binding_kind = TEMP_KIND
  def binding_dto = TEMP_DTO
  def binding_data = TEMP_DATA

  def unbind(router: LocalRouter, queue: Queue) = {
  }

  def bind(router: LocalRouter, queue: Queue) = {
  }

  override def hashCode = if(key==null) 0 else key.hashCode

  override def equals(o:Any):Boolean = o match {
    case x: TempQueueBinding => x.key == key
    case _ => false
  }

}
