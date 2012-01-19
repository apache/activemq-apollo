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
import org.apache.activemq.apollo.dto._
import org.apache.activemq.apollo.util.ClassFinder
import org.apache.activemq.apollo.util.path.Path
import java.lang.String
import org.fusesource.hawtbuf.{Buffer, AsciiBuffer}
import Buffer._

trait BindingFactory {
  def create(binding_kind:AsciiBuffer, binding_data:Buffer):Binding
  def create(binding_dto:DestinationDTO):Binding
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
      val rc = provider.create(binding_kind, binding_data)
      if( rc!=null ) {
        return rc
      }
    }
    throw new IllegalArgumentException("Invalid binding type: "+binding_kind);
  }
  def create(binding_dto:DestinationDTO):Binding = {
    finder.singletons.foreach { provider=>
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
   * The name of the queue (could be the queue name or a subscription id etc)
   */
  def id:String

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

  def config(host:VirtualHost):QueueDTO

  override def toString: String = id
}

object QueueDomainQueueBinding extends BindingFactory {

  val POINT_TO_POINT_KIND = new AsciiBuffer("ptp")
  val DESTINATION_PATH = new AsciiBuffer("default");

  def create(binding_kind:AsciiBuffer, binding_data:Buffer) = {
    if( binding_kind == POINT_TO_POINT_KIND ) {
      val dto = JsonCodec.decode(binding_data, classOf[QueueDestinationDTO])
      new QueueDomainQueueBinding(binding_data, dto)
    } else {
      null
    }
  }

  def create(binding_dto:DestinationDTO) = binding_dto match {
    case ptp_dto:QueueDestinationDTO =>
      new QueueDomainQueueBinding(JsonCodec.encode(ptp_dto), ptp_dto)
    case _ => null
  }

  def queue_config(virtual_host:VirtualHost, path:Path):QueueDTO = {
    import collection.JavaConversions._
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
class QueueDomainQueueBinding(val binding_data:Buffer, val binding_dto:QueueDestinationDTO) extends Binding {

  import QueueDomainQueueBinding._

  val destination = LocalRouter.destination_parser.decode_path(binding_dto.path)
  def binding_kind = POINT_TO_POINT_KIND

  def unbind(node: LocalRouter, queue: Queue) = {
    node.local_queue_domain.unbind(queue)
  }

  def bind(node: LocalRouter, queue: Queue) = {
    node.local_queue_domain.bind(queue)
  }

  val id = binding_dto.name(LocalRouter.destination_parser.path_separator)

  override def hashCode = binding_kind.hashCode ^ binding_data.hashCode

  override def equals(o:Any):Boolean = o match {
    case x: QueueDomainQueueBinding => x.binding_data == binding_data
    case _ => false
  }


  def config(host:VirtualHost):QueueDTO = queue_config(host, destination)

}


object DurableSubscriptionQueueBinding extends BindingFactory {

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


  def dsub_config(host:VirtualHost, id:String) = {
    import collection.JavaConversions._
    def matches(x:DurableSubscriptionDTO):Boolean = {
      if( x.id != null && x.id!=id ) {
        return false
      }

      if( x.id_regex != null ) {
        // May need to cache the regex...
        val regex = x.id_regex.r
        if( !regex.findFirstIn(id).isDefined ) {
          return false
        }
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
class DurableSubscriptionQueueBinding(val binding_data:Buffer, val binding_dto:DurableSubscriptionDestinationDTO) extends Binding {
  import DurableSubscriptionQueueBinding._

  val destination = Path(binding_dto.subscription_id)

  def binding_kind = DURABLE_SUB_KIND


  def unbind(router: LocalRouter, queue: Queue) = {
    router.local_dsub_domain.unbind(queue)
  }

  def bind(router: LocalRouter, queue: Queue) = {
    router.local_dsub_domain.bind(queue)
  }

  def id = binding_dto.subscription_id

  override def hashCode = binding_kind.hashCode ^ binding_data.hashCode

  override def equals(o:Any):Boolean = o match {
    case x: DurableSubscriptionQueueBinding => x.binding_data == binding_data
    case _ => false
  }

  override def message_filter = {
    if ( binding_dto.selector==null ) {
      ConstantExpression.TRUE
    } else {
      SelectorParser.parse(binding_dto.selector)
    }
  }

  def config(host:VirtualHost):DurableSubscriptionDTO = dsub_config(host, binding_dto.subscription_id)

}


object TempQueueBinding {
  val TEMP_DATA = new AsciiBuffer("")
  val TEMP_KIND = new AsciiBuffer("tmp")

//  def create(binding_kind:AsciiBuffer, binding_data:Buffer) = {
//    if( binding_kind == TEMP_KIND ) {
//      new TempQueueBinding("", "")
//    } else {
//      null
//    }
//  }
//
//  def create(binding_dto:DestinationDTO) = throw new UnsupportedOperationException
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class TempQueueBinding(val key:AnyRef, val destination:Path, val binding_dto:DestinationDTO) extends Binding {
  import TempQueueBinding._

  def binding_kind = TEMP_KIND
  def binding_data = TEMP_DATA

  def unbind(router: LocalRouter, queue: Queue) = {}
  def bind(router: LocalRouter, queue: Queue) = {}

  override def hashCode = if(key==null) 0 else key.hashCode

  def id = key.toString

  def config(host: VirtualHost) = new QueueDTO

  override def equals(o:Any):Boolean = o match {
    case x: TempQueueBinding => x.key == key
    case _ => false
  }

}
